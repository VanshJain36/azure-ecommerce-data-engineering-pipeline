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

customer_orders = (
    fact_sales
    .groupBy('customer_key')
    .agg(
        countDistinct('order_id').alias('total_orders')
    )
)

# COMMAND ----------

display(customer_orders)

# COMMAND ----------

retained_customers = (
    customer_orders.filter(
    col('total_orders') > 1
    )
)

# COMMAND ----------

total_customers = customer_orders.count()

# COMMAND ----------

retained_count = retained_customers.count()

# COMMAND ----------

print("Total Customers:", total_customers)
print("Retained Customers:", retained_count)

# COMMAND ----------

retention_rate = (
    retained_count / total_customers
) * 100

# COMMAND ----------

print('Retention Rate:', '{:.2f}'.format(retention_rate), '%')

# COMMAND ----------

retention_df = spark.createDataFrame(
    [
        (
            total_customers,
            retained_count,
            '{:.2f}'.format(retention_rate)
        )
    ],
    [
        "total_customers",
        "retained_customers",
        "retention_rate"
    ]
)

# COMMAND ----------

display(retention_df)

# COMMAND ----------

retention_df.write \
.format("delta") \
.mode("overwrite") \
.save(
"abfss://gold@stdecommerceproject.dfs.core.windows.net/kpis/customer_retention"
)