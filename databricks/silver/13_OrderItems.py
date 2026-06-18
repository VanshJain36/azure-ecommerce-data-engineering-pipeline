# Databricks notebook source
storage_account_name =  
storage_account_key = 

spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

bronze_OrderItems = (
    spark.read
        .format("delta")
        .load("abfss://bronze@stdecommerceproject.dfs.core.windows.net/delta/OrderItems")
)

# COMMAND ----------

from pyspark.sql.functions import *

order_items_clean = (
    bronze_OrderItems
    .filter(col('order_id').isNotNull())
    .filter(col('product_id').isNotNull())
)

# COMMAND ----------

order_items_clean = (
    order_items_clean
    .dropDuplicates(
        ['order_id', 'order_item_id']
    )
)

# COMMAND ----------

order_items_clean = (
    order_items_clean.filter(
        col('price') > 0
    )
)

# COMMAND ----------

order_items_clean = (
    order_items_clean
    .withColumn(
        'revenue',
        round(col('price'),2)
    )
)

# COMMAND ----------

order_items_clean = (
    order_items_clean.withColumn(
        "total_order_value",
        col("price") + col("freight_value")
    )
)

# COMMAND ----------

order_items_clean.write \
    .format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .save(
        'abfss://silver@stdecommerceproject.dfs.core.windows.net/OrderItems'
    )

# COMMAND ----------

total_rows = bronze_OrderItems.count()

clean_rows = order_items_clean.count()

rejected_rows = total_rows - clean_rows

# COMMAND ----------

metrics = spark.createDataFrame(
    [
        (
            "OrderItems",
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

