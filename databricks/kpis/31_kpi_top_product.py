# Databricks notebook source
storage_account_name = 
storage_account_key = 

spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

fact_sales = spark.read.format('delta').load(
    'abfss://gold@stdecommerceproject.dfs.core.windows.net/fact_sales'
)

dim_product = (
    spark.read
        .format('delta')
        .load('abfss://gold@stdecommerceproject.dfs.core.windows.net/dim_product')
)

# COMMAND ----------

from pyspark.sql.functions import *

top_products = (
    fact_sales
    .groupBy("product_key")
    .agg(
        round(sum("revenue"), 2).alias("total_revenue")
    )
)

top_products = (
    top_products
    .join(
        dim_product.select(
            "product_key",
            "product_category_name"
        ),
        on="product_key",
        how="left"
    )
)

top_products = (
    top_products.select(
        "product_category_name",
        "total_revenue"
    )
    .orderBy(
        col("total_revenue").desc()
    )
)

# COMMAND ----------

top_products.write \
.format('delta') \
.mode('overwrite') \
.save(
    'abfss://gold@stdecommerceproject.dfs.core.windows.net/kpis/top_products'
)

# COMMAND ----------

top_products.printSchema()

# COMMAND ----------

