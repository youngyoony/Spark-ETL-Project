import argparse
from pyspark.sql import SparkSession

from filter import DailyStatFilter


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # parser.add_argument("--input_path", required=True)
    # parser.add_argument("--target_date", default=None, help="optional:target date(yyyymmdd)")
    args = parser.parse_args()

    spark = (SparkSession
        .builder
        .master("local")
        .appName("spark-sql")
        .getOrCreate())
    args.spark = spark
    args.input_path = "/opt/bitnami/spark/data/*.json"

    # filter
    filter = DailyStatFilter(args)
    df = filter.read_input()
    df = filter.init_df(df)
    df = filter.filter(df)

    # store data to ES
