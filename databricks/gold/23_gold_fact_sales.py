# Databricks notebook source
storage_account_name = 
storage_account_key = 

spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

silver_OrderItems = (
    spark.read
        .format("delta")
        .load("abfss://silver@stdecommerceproject.dfs.core.windows.net/OrderItems")
)

silver_orders = (
    spark.read
        .format("delta")
        .load("abfss://silver@stdecommerceproject.dfs.core.windows.net/orders")
)

dim_customer = (
    spark.read
        .format('delta')
        .load('abfss://gold@stdecommerceproject.dfs.core.windows.net/dim_customer')
)

dim_product = (
    spark.read
        .format('delta')
        .load('abfss://gold@stdecommerceproject.dfs.core.windows.net/dim_product')
)

# COMMAND ----------

fact_sales = (
    silver_OrderItems.alias('o1')
    .join(
        silver_orders.alias('o'),
        'order_id'
    )
)

# COMMAND ----------

fact_sales = (
    fact_sales.join(
        dim_customer,
        'customer_id'
    )
)

# COMMAND ----------

fact_sales = (
    fact_sales.join(
        dim_product,
        'product_id'
    )
)

# COMMAND ----------

from pyspark.sql.functions import *

fact_sales = (
    fact_sales.withColumn(
        'revenue',
        col('price')
    )
)

# COMMAND ----------

fact_sales = (
    fact_sales.withColumn(
        'shipping_cost',
        col('freight_value')
    )
)

# COMMAND ----------

fact_sales = (
    fact_sales.withColumn(
        'total_sales',
        col('price') + col('freight_value')
    )
)

# COMMAND ----------

fact_sales = (
    fact_sales.withColumn(
        'delivery_delay_days',
        datediff(
            col('order_delivered_customer_date'),
            col('order_estimated_delivery_date')
        )
    )
)

# COMMAND ----------

fact_sales.write \
    .format('delta') \
    .mode('overwrite') \
    .save(
        "abfss://gold@stdecommerceproject.dfs.core.windows.net/fact_sales"
    )

# COMMAND ----------

print(fact_sales.columns)

# COMMAND ----------

fact_sales.printSchema()

# COMMAND ----------

fact_sales_final = fact_sales.select(
    "order_id",
    "order_item_id",
    "customer_key",
    "product_key",
    "customer_id",
    "product_id",
    "seller_id",
    "order_status",
    "order_purchase_timestamp",
    "price",
    "freight_value",
    "revenue",
    "shipping_cost",
    "total_sales",
    "delivery_delay_days"
)

# COMMAND ----------

fact_sales_final.printSchema()

# COMMAND ----------

fact_sales_final.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://gold@stdecommerceproject.dfs.core.windows.net/fact_sales"
    )

# COMMAND ----------

fact_sales_final.count()

# COMMAND ----------

display(fact_sales_final.limit(10))

# COMMAND ----------

