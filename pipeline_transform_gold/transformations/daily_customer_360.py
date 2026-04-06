from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark import pipelines as dp 


@dp.materialized_view(
    name = "03_gold.customer_360",
    table_properties = {"quality":"gold"}
)

def customer_360():
    df_orders = dp.read("02_silver.fact_orders")

    df_order_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("total_amount").alias("lifetime_spend"),
        round(avg("total_amount"),2).alias("avg_order_value"),
        max("order_date").alias("last_order_date"))
    .withColumn(
        "loyality_tier",
        when(col("lifetime_spend") >= 5000, "Platinum")
        .when(col("lifetime_spend") >= 2000, "Gold")
        .when(col("lifetime_spend") >= 1000, "Silver")
        .otherwise("Bronze" )
    
    ))
    df_reviews = dp.read("02_silver.fact_reviews")

    df_review_stats = (
        df_reviews
        .groupBy("customer_id")
        .agg(
            countDistinct("review_id").alias("total_reviews"),
            round(avg("rating"),2).alias("avg_rating")       
            )   
    )

    df_restureant = dp.read("02_silver.dim_restaurants")
    df_fav_resturant = (
        df_orders.
        join(df_restureant, on = "restaurant_id", how = "inner")
        .groupBy("customer_id","name")
        .agg(
            count("order_id").alias("order_count"),
        )
        .withColumn("rn",row_number().over(Window.partitionBy("customer_id").orderBy(desc("order_count"))))
        .filter(col("rn") == 1)
        .drop("rn")
        .select("customer_id",col("name").alias("favourite_restaurant")
    ))


    df_fact_order_items = dp.read("02_silver.fact_order_items")

    df_fav_item = (
        df_orders
        .join(df_fact_order_items, on = "order_id")
        .groupBy("customer_id","item_name")
        .agg(
        sum("item_quantity").alias("total_quantity")
        )
        .withColumn("rn",row_number().over(Window.partitionBy("customer_id").orderBy(desc("total_quantity"))))
        .filter(col("rn") == 1)
        .drop("rn")
        .select("customer_id",col("item_name").alias("favourite_item"))
        .orderBy("customer_id","favourite_item",desc("total_quantity"))
    )


    df_customer = dp.read("02_silver.dim_customers")

    df_c360 = (
        df_customer
        .join(df_order_stats, on = "customer_id",how = "left")
        .join(df_fav_item, on = "customer_id",how = "left" )
        .join(df_review_stats, on = "customer_id",how = "left")
        .join(df_fav_resturant, on = "customer_id",how = "left")
        .select(
            "customer_id",
            col("name").alias("customer_name"),
            "email",
            "city",
            "join_date",
            "loyality_tier",
            coalesce(col("total_orders"),lit(0)).alias("total_orders"),
            coalesce(col("lifetime_spend"),lit(0)).cast("decimal(10,2)").alias("lifetime_spend"),
            coalesce(col("avg_order_value"),lit(0)).cast("decimal(10,2)").alias("avg_order_value"),
            "last_order_date",

            coalesce(col("total_reviews"),lit(0)).cast("decimal(10,2)").alias("total_reviews"),
            coalesce(col("avg_rating"),lit(0)).cast("decimal(10,2)").alias("avg_rating"),
            "favourite_item",
            "favourite_restaurant",
            when(col("lifetime_spend") >= 5000, True).otherwise(False).alias("is_vip")
            
    
    ))


    return df_c360