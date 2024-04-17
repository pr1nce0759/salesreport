# Databricks notebook source
dbutils.fs.mount(source = 'wasbs://sampledata@amandatahouse.blob.core.windows.net', 
                 mount_point='/mnt/blobstorage1',
                 extra_configs={"fs.azure.sas.sampledata.amandatahouse.blob.core.windows.net":"sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-04-17T17:01:24Z&st=2024-04-17T09:01:24Z&spr=https&sig=MrF6tMoQEJAUk76Ob4YnZCbOctu8XrVETsMLQKH8LDY%3D"})
