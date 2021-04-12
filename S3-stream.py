# Databricks notebook source
from pyspark.sql.types import *

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True),
    StructField("house_number", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("postcode", StringType(), True)
])

stream_location = 'jr-streaming-submissions/submissions/users/'
output_location = 'jr-streaming-submissions/raw/users/'

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "avro") \
  .schema(schema) \
  .load("s3://" + stream_location)


df.writeStream.format("delta") \
  .option("checkpointLocation", "/tmp/" + stream_location) \
  .trigger(once=True) \
  .table("customer_raw")

df.writeStream.format("delta") \
  .option("checkpointLocation", "/tmp/" + stream_location + "external") \
  .trigger(once=True) \
  .start("s3://" + output_location)

# COMMAND ----------

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
df = spark.read.parquet("s3://jr-streaming-submissions/raw/users/*.parquet")
df.show()

# COMMAND ----------

dbutils.fs.ls("s3://jr-streaming-submissions/submissions/users")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_raw
