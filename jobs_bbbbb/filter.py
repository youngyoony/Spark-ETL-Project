
from base import BaseFilter
import pyspark.sql.functions as F

class ProductFilter(BaseFilter):
    def __init__(self, df):
        self.df = df

    def average_ratings_by_customer(self):
        average_ratings = self.df.groupBy('NickName').agg(F.avg('Rate').alias('AverageRating'))
        return average_ratings

    def total_purchase_by_customer(self):
        total_purchase = self.df.groupBy("CustomerID").agg(F.sum("UnitPrice").alias("total_purchase"))
        return total_purchase

    def average_rating_by_customer(self):
        average_rating = self.df.groupBy("CustomerID").agg(F.avg("Rate").alias("average_rating"))
        return average_rating

    def purchase_count_by_customer(self):
        purchase_count = self.df.groupBy("NickName").agg(F.count("ProductID").alias("purchase_count"))
        return purchase_count

    def total_revenue_by_product(self):
        total_revenue = self.df.groupBy("Name").agg(F.sum("UnitPrice").alias("total_revenue"))
        return total_revenue

    def purchases_by_specific_customer(self, customer_id):
        specific_customer_data = self.df.filter(self.df["CustomerID"] == customer_id)
        return specific_customer_data


class UserFilter(BaseFilter):
    def __init__(self, df):
        self.df = df

    def average_ratings_by_product(self):
        review_count = self.df.groupBy("ProductID").agg(F.count("Rate").alias("review_count"))
        return review_count

    def high_rated_products(self):
        high_rated = self.df.filter(self.df["Rate"] >= 4)
        return high_rated
