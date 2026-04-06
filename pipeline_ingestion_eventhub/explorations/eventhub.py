# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

EH_NAMESPACE="rg-ns-dbhproject"
EH_NAME="orders"
EH_CONN_STR = ""

# COMMAND ----------

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : "60000",
  "kafka.session.timeout.ms" : "30000",
  "maxOffsetsPerTrigger"     : "50000", ## limits how many message per batch 
  "failOnDataLoss"           : "true", ## control what happens if some kafka data is lost 
  "startingOffsets"          : "earliest" ## make sure we read from the begenning  
}

# COMMAND ----------

# DBTITLE 1,Cell 3
CHECKPOINT_PATH = "/Volumes/ws_dbxproject/00_landing/checkpoints"

df_raw = (
    spark.readStream
    .format("kafka")
    .options(**KAFKA_OPTIONS)
    .load()
)

display(df_raw, checkpointLocation=CHECKPOINT_PATH)

# COMMAND ----------

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("items", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True)
])

# COMMAND ----------

dbutils.fs.rm("/Volumes/ws_dbxproject/00_landing/checkpoints/kafka_parsed", recurse=True)

# COMMAND ----------

dbutils.fs.rm("/Volumes/ws_dbxproject/00_landing/checkpoints/kafka_parsed", recurse=True)
df_parsed = (
    df_raw
    .withColumn("key_str",col("key").cast("string"))
    .withColumn("value_str",col("value").cast("String"))
    .withColumn("data",from_json("value_str",orders_schema))
    .select("data.*")
    .withColumnRenamed("time_stamp","order_timestamp"))

display(df_parsed, checkpointLocation="/Volumes/ws_dbxproject/00_landing/checkpoints/kafka_parsed")


# COMMAND ----------


