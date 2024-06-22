from abc import ABC, abstractmethod
from pyspark.sql.types import StructType, StructField, StringType, LongType

import pyspark.sql.functions as F

actor_schema = StructType([
    StructField('login', StringType(), True),
    StructField('url', StringType(), True)
])

payload_schema = StructType([
    StructField('repository_id', LongType(), True),
    StructField('size', LongType(), True),
    StructField('distinct_size', LongType(), True),
    StructField('comment', StructType([StructField('body', StringType(), True)]), True),
])

repo_schema = StructType([
    StructField('name', StringType(), True),
    StructField('url', StringType(), True)
])

class BaseFilter(ABC):
    def __init__(self, args):
        self.args = args
        self.spark = args.spark

    def read_input(self):
        def _input_exists(input_path):
            return True

        path = self.args.input_path
        if _input_exists(path):
            spark = self.spark
            df = spark.read.json(path)
            df.printSchema()
            # max_executor_num = 3
            # if df.rdd.getNumPartitions() < max_executor_num:
            #     df = df.repartition(max_executor_num)
            return df
        else:
            return None
        
    def df_with_meta(self, df, datetime):
        df = df.withColumn("@timestamp", F.lit(datetime.strftime("%Y-%m-%d"))) 
        return df

    def init_df(self, df):
        df = df.withColumn('actor_json', F.from_json('actor', actor_schema)) \
                .select('created_at', 'id', 'payload', 'type', 'actor_json.*', 'repo')
        df = df.withColumn('payload_json', F.from_json('payload', payload_schema)) \
                .select('login', 'url', 'created_at', 'id', 'payload_json.*', 'type', 'repo')
        df = df.withColumn('repo_json', F.from_json('repo', repo_schema)) \
                .select('login', 'url', 'created_at', 'id', 'repository_id', 'size', 'distinct_size', 'comment', 'type', 'repo_json.*')
        
        n_cols = ['user_name', 'url', 'created_at', 'id', 'repository_id', 'size', 'distinct_size', 'comment', \
                'type', 'name', 'repo_url']
        df = df.toDF(*n_cols)

        df = df.filter(F.col("login") != "github-actions[bot]")
        df = df.withColumn('created_at', F.trim(F.regexp_replace(df.created_at, "[TZ]", " ")))
        df = df.withColumn('created_at', F.to_timestamp(df.created_at, 'yyyy-MM-dd HH:mm:ss'))

        udf_check_repo_name = F.udf(lambda name: name.split("/")[-1], StringType())
        df = df.withColumn('repo_name', udf_check_repo_name(F.col('name')))
        df = df.drop('name')
        return df

    def filter(self, df):
        None
