# Databricks notebook source
storage_account_name =  
storage_account_key = 

spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

silver_customers = (
    spark.read
        .format("delta")
        .load("abfss://silver@stdecommerceproject.dfs.core.windows.net/customers")
)

# COMMAND ----------

from pyspark.sql.functions import *

dim_customer = (
    silver_customers
    .withColumn(
        'customer_key',
        monotonically_increasing_id()
    )
)

# COMMAND ----------

dim_customer.write \
    .format('delta') \
    .mode('overwrite') \
    .save(
        "abfss://gold@stdecommerceproject.dfs.core.windows.net/dim_customer"
    )

# COMMAND ----------

