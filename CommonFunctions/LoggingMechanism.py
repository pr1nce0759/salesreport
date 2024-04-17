# Databricks notebook source
from pyspark.sql import SparkSession
from datetime import datetime

log_schema = "log STRING, log_date TIMESTAMP"
log_df = spark.createDataFrame([], log_schema)

def log_this(log_message):
    global log_df
    log_entry = (log_message, datetime.now())
    log_entry_df = spark.createDataFrame([log_entry], log_schema)
    log_df = log_df.union(log_entry_df)
    print(log_message)

def persist_logs():
    # log_df.write \
    #       .mode("append") \
    #       .parquet("dbfs:/mnt/blobstorage/logs/log_data.parquet")
    log_df.write.format("delta").mode('append').saveAsTable("sync_logs")
    

if __name__ == "__main__":
    log_this("Logging Functions notebook initialized.")

