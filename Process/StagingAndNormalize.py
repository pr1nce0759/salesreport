# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

log_this("Reading validated_clnd Delta table from Databricks Catalog.")
validated_clnd_df = spark.read.table("validated_clnd")
hier_prod_df = spark.read.table("validated_prod")
fact_transactions_df = spark.read.table("validated_fact")

# COMMAND ----------

log_this("Normalizing hier.clnd table.")
clnd_normalized_df = validated_clnd_df.select("fscldt_id", "fscldt_label", "fsclwk_id", "fsclwk_label", "fsclmth_id", "fsclmth_label", "fsclqrtr_id", "fsclqrtr_label", "fsclyr_id", "fsclyr_label", "ssn_id", "ssn_label")

# COMMAND ----------

log_this("Normalizing hier.prod table.")
prod_normalized_df = hier_prod_df.select("sku_id", "sku_label", "stylclr_id", "stylclr_label", "styl_id", "styl_label", "subcat_id", "subcat_label", "cat_id", "cat_label", "dept_id", "dept_label")

# COMMAND ----------

log_this("Creating staged fact table.")
staged_fact_df = fact_transactions_df.join(clnd_normalized_df, "fscldt_id", "left_outer") \
                                     .join(prod_normalized_df, "sku_id", "left_outer") \
                                     .select("order_id", "line_id", "type", "dt", "pos_site_id", "sku_id", "fscldt_id", "price_substate_id", "sales_units", "sales_dollars", "discount_dollars", "original_order_id", "original_line_id","fsclwk_id")

# COMMAND ----------

staged_fact_df.write.format("delta").mode("overwrite").saveAsTable('StagedFactsTable')

# COMMAND ----------

persist_logs() 
