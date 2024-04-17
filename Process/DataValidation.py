# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

log_this("Reading hier.clnd.dlm file.")
hier_clnd_df = spark.read \
    .option("delimiter", "|") \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .csv("dbfs:/mnt/blobstorage/gsynergyrawdata/hier.clnd.dlm")

# COMMAND ----------

log_this("Reading hier.prod.dlm file.")
hier_prod_df = spark.read \
    .option("delimiter", "|") \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .csv("dbfs:/mnt/blobstorage/gsynergyrawdata/hier.prod.dlm")

# COMMAND ----------

log_this("Reading fact.transactions.dlm file.")
hier_transactions_df = spark.read \
    .option("delimiter", "|") \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .csv("dbfs:/mnt/blobstorage/gsynergyrawdata/fact.transactions.dlm")

# COMMAND ----------

log_this("Data validation started.")

# COMMAND ----------

def validate_dataframe(df, df_name):
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            log_this(f"Null values found in column {column} of {df_name}: {null_count}")
            df = df.filter(col(column).isNotNull())
    return df

# COMMAND ----------

def drop_duplicate_primary_key(df, column_name, df_name):
    duplicate_count = df.groupBy(column_name).count().filter("count > 1").count()
    if duplicate_count > 0:
        log_this(f"Duplicate primary key values found in {column_name} of {df_name}.")
        df = df.dropDuplicates([column_name])
    else:
        log_this(f"No duplicate primary key values found in {column_name} of {df_name}.")
    return df

# COMMAND ----------

hier_clnd_df = validate_dataframe(hier_clnd_df,'hier_clnd_df')
hier_clnd_df = drop_duplicate_primary_key(hier_clnd_df,"fscldt_id", "hier_clnd_df")

# COMMAND ----------

hier_prod_df = validate_dataframe(hier_prod_df,'hier_prod_df')
hier_prod_df = drop_duplicate_primary_key(hier_prod_df, "sku_id", "hier_prod_df")

# COMMAND ----------

hier_transactions_df = validate_dataframe(hier_transactions_df,'hier_clnd_df')
hier_transactions_df = drop_duplicate_primary_key(hier_transactions_df, "order_id", "hier_transactions_df")

# COMMAND ----------

hier_clnd_df.write.format("delta").mode("overwrite").saveAsTable("validated_clnd")
hier_prod_df.write.format("delta").mode("overwrite").saveAsTable("validated_prod")
hier_transactions_df.write.format("delta").mode("overwrite").saveAsTable("validated_fact")

# COMMAND ----------

persist_logs()
