# Databricks notebook source
storage_account_name =  
storage_account_key = 

spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

bronze_products = (
    spark.read
        .format("delta")
        .load("abfss://bronze@stdecommerceproject.dfs.core.windows.net/delta/products")
)

# COMMAND ----------

from pyspark.sql.functions import *

products_clean = bronze_products.filter(
    col('product_id').isNotNull()
)

# COMMAND ----------

products_clean = (
    products_clean
    .dropDuplicates(['product_id'])
)

# COMMAND ----------

products_clean = (
    products_clean.filter(
        (col('product_weight_g') > 0)
    )
)

# COMMAND ----------

product_clean = (
    products_clean.withColumn(
        'product_category_name',
        upper(
            trim(col('product_category_name'))
        )
    )
)

# COMMAND ----------

products_clean.write \
    .format('delta') \
    .mode('overwrite') \
    .save(
        'abfss://silver@stdecommerceproject.dfs.core.windows.net/products'
    )

# COMMAND ----------

total_rows = bronze_products.count()

clean_rows = products_clean.count()

rejected_rows = total_rows - clean_rows

# COMMAND ----------

metrics = spark.createDataFrame(
    [
        (
            "products",
            total_rows,
            clean_rows,
            rejected_rows
        )
    ],
    [
        "table_name",
        "total_rows",
        "clean_rows",
        "rejected_rows"
    ]
)

# COMMAND ----------

metrics.write \
    .format('delta') \
    .mode('overwrite') \
    .save(
        'abfss://silver@stdecommerceproject.dfs.core.windows.net/logs/data_quality_metrics'
    )

# COMMAND ----------

