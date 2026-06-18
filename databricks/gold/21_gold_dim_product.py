# Databricks notebook source
storage_account_name = 
storage_account_key = 

spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

silver_products = (
    spark.read
        .format("delta")
        .load("abfss://silver@stdecommerceproject.dfs.core.windows.net/products")
)

# COMMAND ----------

from pyspark.sql.functions import *

dim_product = (
    silver_products
    .withColumn(
        'product_key',
        monotonically_increasing_id()
    )
)

# COMMAND ----------

dim_product.write \
    .format('delta') \
    .mode('overwrite') \
    .save(
        "abfss://gold@stdecommerceproject.dfs.core.windows.net/dim_product"
    )

# COMMAND ----------

