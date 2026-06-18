# Databricks notebook source
storage_account_name = 
storage_account_key = 

spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

bronze_orders = (
    spark.read
        .format("delta")
        .load("abfss://bronze@stdecommerceproject.dfs.core.windows.net/delta/orders")
)

# COMMAND ----------

from pyspark.sql.functions import *
orders_clean = bronze_orders.filter(
    col('order_id').isNotNull()
)

# COMMAND ----------

orders_clean = (
    orders_clean
    .dropDuplicates(['order_id'])
)

# COMMAND ----------

orders_clean = (
    orders_clean.withColumn(
        'order_status',
        upper(col('order_status'))
    )
)

# COMMAND ----------

invalid_orders = orders_clean.filter(
    col('order_delivered_customer_date')
    <
    col('order_purchase_timestamp')
)

# COMMAND ----------

from pyspark.sql.functions import datediff

orders_clean = (
    orders_clean.withColumn(
        "delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    )
)

# COMMAND ----------

orders_clean.write \
    .format('delta') \
    .mode('overwrite') \
    .options(mergeSchema='true') \
    .save(
        'abfss://silver@stdecommerceproject.dfs.core.windows.net/orders'
    )

# COMMAND ----------

total_rows = bronze_orders.count()

clean_rows = orders_clean.count()

rejected_rows = total_rows - clean_rows

# COMMAND ----------

metrics = spark.createDataFrame(
    [
        (
            "orders",
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