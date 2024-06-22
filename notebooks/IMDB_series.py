from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, BooleanType, FloatType

# Spark 세션
spark = (
    SparkSession.builder
        .master("local")
        .appName("spark-session-test")
        .config("spark.ui.port", "4055")  # Spark UI 포트를 4055로 설정
        .getOrCreate()
)

# CSV 파일 읽기
df = spark.read.option("header", "true").option("inferSchema", "true").csv("./data/IMDb Top TV Series.csv")
df.printSchema()
df.count()
df.show(5)

# 스키마 정의
schema = StructType([
    StructField("Title", StringType(), True),
    StructField("Year", LongType(), True),
    StructField("Parental Rating", StringType(), False),
    StructField("Rating", FloatType(), True),
    StructField("Number of Votes", StringType(), True),
    StructField("Description", StringType(), True)
])

# RDD 생성
rdd = spark.sparkContext.textFile('./data/IMDb Top TV Series.csv')
print(rdd.take(1))

# Spark 세션 종료
spark.stop()