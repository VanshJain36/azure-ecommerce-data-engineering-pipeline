# Databricks notebook source
storage_account_name = 
storage_account_key = 

spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

fact_sales = spark.read.format('delta').load(
    'abfss://gold@stdecommerceproject.dfs.core.windows.net/fact_sales'
)

# COMMAND ----------

from pyspark.sql.functions import *

revenue_trends = (
    fact_sales
    .groupBy(
        year('order_purchase_timestamp').alias('year'),
        month('order_purchase_timestamp').alias('month')
    )
    .agg(
        sum('total_sales').alias('revenue')
    )
    .orderBy('year', 'month')
)

# COMMAND ----------

revenue_trends.count()

# COMMAND ----------

revenue_trends.write \
.format('delta') \
.mode('overwrite') \
.save(
    'abfss://gold@stdecommerceproject.dfs.core.windows.net/kpis/revenue_trends'
)