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

order_status_kpi = (
    fact_sales
    .groupBy('order_status')
    .agg(
        countDistinct('order_id').alias('order_count')
    )
)

# COMMAND ----------

total_orders = (
    fact_sales
    .select('order_id')
    .distinct()
    .count()
)

# COMMAND ----------

order_status_kpi = (
    order_status_kpi
    .withColumn(
        "percentage",
        round(
            (col("order_count") / total_orders) * 100,
            2
        )
    )
)

# COMMAND ----------

display(order_status_kpi)

# COMMAND ----------

order_status_kpi.write \
.format("delta") \
.mode("overwrite") \
.save(
"abfss://gold@stdecommerceproject.dfs.core.windows.net/kpis/order_status"
)

# COMMAND ----------

