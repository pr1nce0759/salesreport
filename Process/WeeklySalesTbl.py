# Databricks notebook source
from pyspark.sql.functions import sum
from pyspark.sql.functions import *

# COMMAND ----------

log_this("Generating Final Report Table")

# COMMAND ----------

mview_weekly_sales_df = spark.read.table('StagedFactsTable')

# COMMAND ----------

staged_fact_df = staged_fact_df.withColumn("week", weekofyear("dt"))
mview_weekly_sales_df = staged_fact_df.groupBy("week","pos_site_id", "sku_id", "fsclwk_id", "price_substate_id", "type") \
                                      .agg(sum("sales_units").alias("total_sales_units"), sum("sales_dollars").alias("total_sales_dollars"), sum("discount_dollars").alias("total_discount_dollars"))
mview_weekly_sales_df.write.format("delta").mode("overwrite").saveAsTable("mview_weekly_sales")
log_this("Final Report Table Generated")

# COMMAND ----------

mview_weekly_sales_df.write.mode("overwrite").parquet('dbfs:/mnt/blobstorage/reports/weeklyreport.parquet')
log_this("Final Report Table Loaded to BLOB")

# COMMAND ----------

persist_logs() 
