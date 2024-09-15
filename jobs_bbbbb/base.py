import glob
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType  # IntegerType 추가
import pyspark.sql.functions as F
import pyspark.sql.functions as F

def load_data(spark):  # 파라미터를 sc에서 spark로 변경
    # 고객 데이터 스키마 정의
    customers_schema = StructType([
        StructField('Id', IntegerType(), True),  # IntegerType import 확인
        StructField('NickName', StringType(), True)
    ])

    # 고객 데이터 로드
    customers_df = spark.read.schema(customers_schema).json('/opt/bitnami/spark/data/customers.json')

    # 제품 데이터 스키마 정의
    products_schema = StructType([
        StructField('Id', IntegerType(), True),
        StructField('Name', StringType(), True),
        StructField('UnitPrice', StringType(), True)
    ])

    # 제품 데이터 로드
    products_df = spark.read.schema(products_schema).json('/opt/bitnami/spark/data/products.json')

    # 평가 데이터 스키마 정의
    ratings_schema = StructType([
        StructField('CustomerID', IntegerType(), True),
        StructField('ProductID', IntegerType(), True),
        StructField('Rate', IntegerType(), True),
        StructField('CreateDate', StringType(), True)
    ])

    # 평가 데이터 로드
    ratings_df = spark.read.schema(ratings_schema).json('/opt/bitnami/spark/data/new_ratings.json')

    # 고객 데이터와 평가 데이터 병합
    merged_df = ratings_df.join(customers_df, ratings_df.CustomerID == customers_df.Id, 'inner') \
                          .select(ratings_df['*'], customers_df['NickName'])

    # 병합된 데이터와 제품 데이터 병합
    final_df = merged_df.join(products_df, merged_df.ProductID == products_df.Id, 'inner') \
                        .select(merged_df['*'], products_df['Name'], products_df['UnitPrice'])

    return final_df


class BaseFilter(ABC):
    def __init__(self, args):
        self.args = args
        self.spark = args.spark

    def filter(self, df):
        None
