import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

from base import load_data  # Ensure these functions are defined as needed
## from base import load_data, init_df, df_with_meta  # Ensure these functions are defined as needed
from filter import ProductFilter, UserFilter
from es import Es


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target_date", default=None, help="optional: target date (yyyy-mm-dd)")
    args = parser.parse_args()

    spark = (SparkSession
        .builder
        .master("local")
        .appName("spark-sql")
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
        .config("spark.jars", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
        .getOrCreate())

    df = load_data(spark)  # 데이터 업데이트
    ## df = init_df(df)

    # 필터 초기화
    repo_filter = ProductFilter(df)
    user_filter = UserFilter(df)

    # TopRepoFilter에서 메소드 사용
    average_ratings_by_customer = repo_filter.average_ratings_by_customer()
    total_purchase_by_customer = repo_filter.total_purchase_by_customer()
    average_rating_by_customer = repo_filter.average_rating_by_customer()
    purchase_count_by_customer = repo_filter.purchase_count_by_customer()
    total_revenue_by_product = repo_filter.total_revenue_by_product()

    # TopUserFilter에서 메소드 사용
    average_ratings_by_product = user_filter.average_ratings_by_product()
    high_rated_products = user_filter.high_rated_products()

    # Elasticsearch에 데이터 저장
    es = Es("http://es:9200")
    es.write_df(average_ratings_by_customer, "average-ratings-by-customer-2024")
    es.write_df(total_purchase_by_customer, "total-purchase-by-customer-2024")
    es.write_df(average_rating_by_customer, "average-rating-by-customer-2024")
    es.write_df(purchase_count_by_customer, "purchase-count-by-customer-2024")
    es.write_df(total_revenue_by_product, "total-revenue-by-product-2024")
    es.write_df(average_ratings_by_product, "average-ratings-by-product-2024")
    es.write_df(high_rated_products, "high-rated-products-2024")

    # 데이터 컨펌 차원에서 show
    print("Average Ratings by Customer:")
    average_ratings_by_customer.show()

    print("Total Purchase by Customer:")
    total_purchase_by_customer.show()

    print("Average Rating by Customer:")
    average_rating_by_customer.show()

    print("Purchase Count by Customer:")
    purchase_count_by_customer.show()

    print("Total Revenue by Product:")
    total_revenue_by_product.show()

    print("Average Ratings by Product:")
    average_ratings_by_product.show()

    print("High Rated Products:")
    high_rated_products.show()

    spark.stop()
