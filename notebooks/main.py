from pyspark.sql import SparkSession
import sys
import filter

def main():
    spark = SparkSession.builder \
        .appName("Product Analysis") \
        .getOrCreate()

    final_df = filter.load_data(spark)

    filter.analyze_data(final_df)

if __name__ == "__main__":
    main()
