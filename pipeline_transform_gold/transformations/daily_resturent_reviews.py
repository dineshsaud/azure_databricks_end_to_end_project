from pyspark.sql.functions import *

from pyspark import pipelines as dp 


@dp.materialized_view(
    name="03_gold.restaurant_reviews",
    table_properties = {"quality":"gold"}
)

def restaurant_reviews():
    df_review_stat = (
    dp.read("02_silver.fact_reviews")
    .groupBy("restaurant_id")
    .agg(
        countDistinct("review_id").alias("total_reviews"),
        round(avg("rating"),2).alias("avg_rating"),
        sum(when(col("rating") == 5, 1).otherwise(0)).alias("total_5_stars"),
        sum(when(col("rating") == 4, 1).otherwise(0)).alias("total_4_stars"),
        sum(when(col("rating") == 3, 1).otherwise(0)).alias("total_3_stars"),
        sum(when(col("rating") == 2, 1).otherwise(0)).alias("total_2_stars"),
        sum(when(col("rating") == 1, 1).otherwise(0)).alias("total_1_stars"),
        sum(when(col("sentiment") == "positive", 1).otherwise(0)).alias("sentiment_positive_count"),
        sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("sentiment_negative_count"),
        sum(when(col("sentiment") == "neutral", 1).otherwise(0)).alias("sentiment_neutral_count")

)
    
)
    


    df_restaurant = spark.read.table("02_silver.dim_restaurants")
    df_restaurant_reviews = (df_review_stat.join(df_restaurant, "restaurant_id",how="left")
    .select(
        "restaurant_id",
        col("name").alias("restaurant_name"),
        "city",
        coalesce(col("total_reviews"),lit(0)).alias("total_reviews"),
        coalesce(col("avg_rating"),lit(0)).alias("average_rating"),
    coalesce(col("total_5_stars"),lit(0)).alias("total_5_stars"),
    coalesce(col("total_4_stars"),lit(0)).alias("total_4_stars"),
    coalesce(col("total_3_stars"),lit(0)).alias("total_3_stars"),
    coalesce(col("total_2_stars"),lit(0)).alias("total_2_stars"),
    coalesce(col("total_1_stars"),lit(0)).alias("total_1_stars"),
    coalesce(col("sentiment_positive_count"),lit(0)).alias("sentiment_positive_count"),
    coalesce(col("sentiment_negative_count"),lit(0)).alias("sentiment_negative_count"),
    coalesce(col("sentiment_neutral_count"),lit(0)).alias("sentiment_neutral_count"),
    )
    )
    return df_restaurant_reviews