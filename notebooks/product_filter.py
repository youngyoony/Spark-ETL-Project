from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F

spark = (SparkSession
         .builder
         .master("local")
         .appName("product-data")
         .getOrCreate())
sc = spark.sparkContext

def load_data(sc):
    # 고객 데이터 스키마 정의
    customers_schema = StructType([
        StructField('Id', IntegerType(), True),
        StructField('NickName', StringType(), True)
    ])

    # 고객 데이터 로드
    customers_df = spark.read.schema(customers_schema).json('./data/customers.json')

    # 제품 데이터 스키마 정의
    products_schema = StructType([
        StructField('Id', IntegerType(), True),
        StructField('Name', StringType(), True),
        StructField('UnitPrice', StringType(), True)
    ])

    # 제품 데이터 로드
    products_df = spark.read.schema(products_schema).json('./data/products.json')

    # 평가 데이터 스키마 정의
    ratings_schema = StructType([
        StructField('CustomerID', IntegerType(), True),
        StructField('ProductID', IntegerType(), True),
        StructField('Rate', IntegerType(), True),
        StructField('CreateDate', StringType(), True)
    ])

    # 평가 데이터 로드
    ratings_df = spark.read.schema(ratings_schema).json('./data/new_ratings.json')  # 올바른 파일 경로로 수정 필요

    # 고객 데이터와 평가 데이터 병합
    merged_df = ratings_df.join(customers_df, ratings_df.CustomerID == customers_df.Id, 'inner') \
                          .select(ratings_df['*'], customers_df['NickName'])

    # 병합된 데이터와 제품 데이터 병합
    final_df = merged_df.join(products_df, merged_df.ProductID == products_df.Id, 'inner') \
                        .select(merged_df['*'], products_df['Name'], products_df['UnitPrice'])

    return final_df

def analyze_data(final_df):
    # 각 고객이 평가한 제품의 평균 평점 계산
    average_ratings = final_df.groupBy('NickName').agg(F.avg('Rate').alias('AverageRating'))
    print('===========각 고객별 평가한 제품의 평균 평점===========')
    average_ratings.show(10, False)

    print('===========각 제품별 평균 평점===========')
    final_df.groupBy("ProductID").agg(F.count("Rate").alias("review_count")).show()

    print('===========고객별 총 구매 금액===========')
    final_df.groupBy("CustomerID").agg(F.sum("UnitPrice").alias("total_purchase")).show()

    print('===========고객별 최고 평점===========')
    customer_avg_rating_df = final_df.groupBy("CustomerID").agg(F.avg("Rate").alias("average_rating"))
    customer_avg_rating_df.show()

    print('===========고객별 구매 횟수===========')
    nickname_purchase_count_df = final_df.groupBy("NickName").agg(F.count("ProductID").alias("purchase_count"))
    nickname_purchase_count_df.show()

    print('===========제품별 총 수익===========')
    total_revenue_df = final_df.groupBy("Name").agg(F.sum("UnitPrice").alias("total_revenue"))
    total_revenue_df.show()

    print('===========평점 4점 이상 제품===========')
    high_rated_products = final_df.filter(final_df["Rate"] >= 4)
    high_rated_products.show()

    print('===========특정 고객(103416) 구매 제품===========')
    specific_customer_data = final_df.filter(final_df["CustomerID"] == 103416)
    specific_customer_data.show()

