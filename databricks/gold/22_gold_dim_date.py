# Databricks notebook source
storage_account_name =  
storage_account_key = 

spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

date_df = spark.sql("""
                    select explode(
                        sequence(
                            to_date('2016-01-01'), 
                            to_date('2020-12-31'), 
                            interval 1 day
                            )
                    ) as date
                    """)

# COMMAND ----------

date_df = (
    date_df
    .withColumn('year', year('date'))
    .withColumn('month', month('date'))
    .withColumn('day', dayofmonth('date'))
    .withColumn('quarter', quarter('date'))
    .withColumn('day_name', date_format('date', 'EEEE'))
)

# COMMAND ----------

date_df = (
    date_df.withColumn(
        'date_key',
        date_format('date', 'yyyyMMdd')
    )
)

# COMMAND ----------

date_df.write \
    .format('delta') \
    .mode('overwrite') \
    .save(
        "abfss://gold@stdecommerceproject.dfs.core.windows.net/dim_date"
    )