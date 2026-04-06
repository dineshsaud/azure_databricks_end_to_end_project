from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark import pipelines as dp


@dp.table(name="fact_order_items",table_properties={"quality":"silver"})
@dp.expect_all_or_drop({

    "valid_order_id":"order_id IS NOT NULL",
    "valid_item_id":"item_id IS NOT NULL",
    "valid_item_name":"item_name IS NOT NULL",
    "valid_restaurant_id":"restaurant_id IS NOT NULL",
    "valid_quantity":"item_quantity > 0",
    "valid_unit_price":"item_unit_price > 0",
    "valid_subtotal":"item_subtotal > 0"
    
})
def fact_order_items():
    items_schema = ArrayType(
            StructType(
                [
                    StructField("item_id", StringType()),
                    StructField("name", StringType()),
                    StructField("category", StringType()),
                    StructField("quantity", IntegerType()),
                    StructField("unit_price", DecimalType(10, 2)),
                    StructField("subtotal", DecimalType(10, 2)),
                ]
            )
        )


    df_fact_orders = (
        dp.read_stream("01_bronze.orders")
        .withColumnRenamed("timestamp", "order_timestamp")
        .withColumn("order_timestamp", to_timestamp(col("order_timestamp")))
        .withColumn("order_date", to_date(col("order_timestamp")))
        .withColumn("items_parsed", from_json(col("items"),items_schema))
        .withColumn("items", explode(col("items_parsed")))
        .select(
            "order_id",
            "restaurant_id",
            "order_timestamp", 
            col("order_date"),
            col("items.item_id").alias("item_id"),
            col("items.name").alias("item_name"),
            col("items.category").alias("item_category"),
            col("items.quantity").alias("item_quantity"),
            col("items.unit_price").alias("item_unit_price"),
            col("items.subtotal").alias("item_subtotal")
        )
    )

    return df_fact_orders
