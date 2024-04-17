# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder\
    .appName("ETL Pipeline")\
    .getOrCreate()

# COMMAND ----------

# MAGIC %run "./CommonFunctions/LoggingMechanism"

# COMMAND ----------

# MAGIC %run "./Process/DataValidation"

# COMMAND ----------

# MAGIC %run "./Process/StagingAndNormalize"

# COMMAND ----------

# MAGIC %run "./Process/WeeklySalesTbl"
