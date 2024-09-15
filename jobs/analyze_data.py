import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, max, min, when

def load_data_from_elasticsearch(spark, index_name, mapping_id="symbol"):
    es_read_conf = {
        "es.nodes": "es",
        "es.port": "9200",
        "es.resource": index_name,
        "es.read.metadata": "true",
        "es.mapping.id": mapping_id
    }
    return spark.read.format("org.elasticsearch.spark.sql").options(**es_read_conf).load()

def analyze_market_trends(data):
    # 상위 상승 종목
    top_gainers = data.orderBy(col("change_percent").desc()).limit(5)
    # 상위 하락 종목
    top_losers = data.orderBy(col("change_percent").asc()).limit(5)
    return top_gainers, top_losers


def analyze_stock_time_series(data):
    # 가격 변화율 분석
    data = data.withColumn("price_change_percent", 
                            (col("price") - col("previous_close")) / col("previous_close") * 100)
    price_analysis = data.groupBy("symbol").agg(
        avg("price_change_percent").alias("average_price_change_percent"),
        stddev("price_change_percent").alias("stddev_price_change_percent")
    )

    # 거래량 분석
    volume_analysis = data.groupBy("symbol").agg(
        avg("volume").alias("average_volume"),
        stddev("volume").alias("stddev_volume")
    )
    
    return price_analysis, volume_analysis

def save_analysis_result(df, file_path):
    df.coalesce(1).write.mode("overwrite").csv(file_path, header=True)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("FinanceDataAnalysis").getOrCreate()
    
    market_trends = load_data_from_elasticsearch(spark, "market_trends")
    top_gainers, top_losers = analyze_market_trends(market_trends)
    save_analysis_result(top_gainers, "data/top_gainers.csv")
    save_analysis_result(top_losers, "data/top_losers.csv")

    stock_time_series = load_data_from_elasticsearch(spark, "stock_time_series")
    price_analysis, volume_analysis = analyze_stock_time_series(stock_time_series)
    save_analysis_result(price_analysis, "data/price_analysis.csv")
    save_analysis_result(volume_analysis, "data/volume_analysis.csv")