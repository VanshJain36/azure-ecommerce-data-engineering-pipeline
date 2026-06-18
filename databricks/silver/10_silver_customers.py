# Databricks notebook source
storage_account_name = 
storage_account_key = 

spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

bronze_customers = (
    spark.read
        .format("delta")
        .load("abfss://bronze@stdecommerceproject.dfs.core.windows.net/delta/customers")
)

# COMMAND ----------

from pyspark.sql.functions import *

customers_clean = (
    bronze_customers
    .filter(col('customer_id').isNotNull())
    .dropDuplicates(['customer_id'])
    .withColumn(
        'customer_city',
        upper(trim(col('customer_city')))
    )
)

# COMMAND ----------

customers_clean.write \
    .format('delta') \
    .mode('overwrite') \
    .save(
        'abfss://silver@stdecommerceproject.dfs.core.windows.net/customers'
    )

# COMMAND ----------

total_rows = bronze_customers.count()

clean_rows = customers_clean.count()

rejected_rows = total_rows - clean_rows

# COMMAND ----------

metrics = spark.createDataFrame(
    [
        (
            "customers",
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