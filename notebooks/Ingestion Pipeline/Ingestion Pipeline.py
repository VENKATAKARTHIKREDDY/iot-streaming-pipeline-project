# Databricks notebook source
# MAGIC %md
# MAGIC ##1. Importing Required Libraries

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Defining the raw_iot_data Table

# COMMAND ----------

@dlt.table(
   name="raw_iot_data",
   comment="Raw IoT device data"
)
def raw_iot_data():
   return spark.readStream.format("delta").load("/tmp/delta/iot_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Defining the transformed_iot_data Table

# COMMAND ----------

@dlt.table(
   name="transformed_iot_data",
   comment="Transformed IoT device data with derived metrics"
)
def transformed_iot_data():
   return (
       dlt.read("raw_iot_data")
       .withColumn("temperature_fahrenheit", col("temperature") * 9/5 + 32)
       .withColumn("humidity_percentage", col("humidity") * 100)
       .withColumn("event_time", current_timestamp())
   )

