# Databricks notebook source
table_name = "orders"

raw_path = f"abfss://bronze@stdecommerceproject.dfs.core.windows.net/{table_name}/{table_name}.csv"

delta_path = f"abfss://bronze@stdecommerceproject.dfs.core.windows.net/delta/{table_name}"

# COMMAND ----------

storage_account_name = 
storage_account_key = 

spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(raw_path)
)

display(df)

# COMMAND ----------

print('Row Count:', df.count())

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

df_bronze = (
    df
    .withColumn(
        "ingestion_time",
        current_timestamp()
    )
)

# COMMAND ----------

display(df_bronze)
(
    df_bronze.write
             .format("delta")
             .mode("overwrite")
             .save(delta_path)
)

# COMMAND ----------

display(
    dbutils.fs.ls(delta_path)
)
bronze_products = (
    spark.read
         .format("delta")
         .load(delta_path)
)
display(bronze_products)

# COMMAND ----------

display(bronze_products.limit(5))
bronze_products.printSchema()