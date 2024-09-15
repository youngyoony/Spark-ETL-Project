from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

def load_data(spark, file_path):
    return spark.read.json(file_path)

def process_market_trends(data):
    processed_data = data.select(
        col("symbol"),
        col("name"),
        col("price"),
        col("change"),
        col("change_percent")
    )
    return processed_data

def process_stock_time_series(data):
    # 가격 변화율 및 거래량 분석을 위한 추가 컬럼 생성
    spark = data.sparkSession
    data = data.withColumn("price_change_percent", 
                            (col("price") - col("previous_close")) / col("previous_close") * 100)
    array_df = data.selectExpr("array(time_series.*) as arr", "*") \
        .drop("time_series")
    
    data.show()
    array_df.show()

    flatten_df = array_df.select(
        explode("arr").alias("ts"),
        col("*")
        ) \
        .drop("arr")
    flatten_df.show()

    schema1 = StructType([
        StructField("col", StringType(), True)
    ])
    cols = [ (c,) for c in data.select('time_series.*').columns]
    column_df = spark.createDataFrame(cols, schema1)

    ws = Window.orderBy("index")
    df1_with_index = column_df.withColumn("index", monotonically_increasing_id()) \
        .withColumn("idx", row_number().over(ws)) \
        .drop("index")
    df2_with_index = flatten_df.withColumn("index", monotonically_increasing_id()) \
        .withColumn("idx", row_number().over(ws)) \
        .drop("index")
    combined_df = df1_with_index.join(df2_with_index, "idx").drop("idx")
    combined_df.show()

    processed_data = combined_df.select(
        col("symbol"),
        col("ts.price").alias("price"),
        col("previous_close"),
        col("ts.change").alias("change"),
        col("ts.change_percent").alias("change_percent"),
        col("price_change_percent"),
        col("ts.volume").alias("volume"),
        col("col").alias("time_series"),
    ) 
    return processed_data

def save_to_elasticsearch(df, index_name, mapping_id="symbol"):
    es_write_conf = {
        "es.nodes": "es",
        "es.port": "9200",
        "es.resource": index_name,
        "es.mapping.id": mapping_id
    }
    df.write.format("org.elasticsearch.spark.sql").options(**es_write_conf).mode("overwrite").save()

        #"es.input.json": "true",


if __name__ == "__main__":
    spark = SparkSession.builder \
                        .appName("FinanceDataProcessing") \
                        .getOrCreate()

    print("=========== Processing (Market trends) ===================")
    # Process
    spark = SparkSession.builder.appName("FinanceDataProcessing").getOrCreate()

    print("step 1: loading…")
    market_trends = load_data(spark, "/opt/bitnami/spark/data/market_trends.json")
    print("step 2: processing…")
    processed_market_trends = process_market_trends(market_trends)
    print("step 3: writing…")
    save_to_elasticsearch(processed_market_trends, "market_trends")

    print("=========== Processing (stock time series) ===================")
    print("step 1: loading…")
    stock_time_series = load_data(spark, "/opt/bitnami/spark/data/stock_time_series.json")
    print("step 2: processing…")
    processed_stock_time_series = process_stock_time_series(stock_time_series)
    print("step 3: writing…")
    save_to_elasticsearch(processed_stock_time_series, "stock_time_series", mapping_id="time_series")



    #market_trends = load_data(spark, "/opt/bitnami/spark/data/market_trends.json")
    #processed_market_trends = process_market_trends(market_trends)
    #save_to_elasticsearch(processed_market_trends, "market_trends")

    #stock_time_series = load_data(spark, "/opt/bitnami/spark/data/stock_time_series.json")
    #processed_stock_time_series = process_stock_time_series(stock_time_series)
    #save_to_elasticsearch(processed_stock_time_series, "stock_time_series")
