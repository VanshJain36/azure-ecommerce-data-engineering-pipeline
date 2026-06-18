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

delivery_kpi = (
    fact_sales
    .agg(
        round(avg('delivery_delay_days'),2)
        .alias('avg_delay_days'),

        max('delivery_delay_days')
        .alias('max_delay_days')
    )
)

# COMMAND ----------

display(delivery_kpi)

# COMMAND ----------

late_orders = (
    fact_sales
    .filter(
        col("delivery_delay_days") > 0
    )
    .count()
)

# COMMAND ----------

total_orders = fact_sales.count()

# COMMAND ----------

late_percentage = (
    late_orders / total_orders
) * 100

# COMMAND ----------

from pyspark.sql.functions import *

delivery_summary = spark.createDataFrame(
    [
        (
            total_orders,
            late_orders,
            '{:.2f}'.format(late_percentage)
        )
    ],
    [
        "total_orders",
        "late_orders",
        "late_delivery_percentage"
    ]
)

# COMMAND ----------

final_delivery_kpi = (
    delivery_summary
    .crossJoin(delivery_kpi)
)

# COMMAND ----------

display(final_delivery_kpi)

# COMMAND ----------

final_delivery_kpi.write \
.format("delta") \
.mode("overwrite") \
.save(
"abfss://gold@stdecommerceproject.dfs.core.windows.net/kpis/delivery_delays"
)

# COMMAND ----------

