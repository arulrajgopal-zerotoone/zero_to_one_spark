# Databricks notebook source
spark.conf.set("fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net", "<your-access-key>")

# COMMAND ----------

# read a file

df = spark.read\
    .format("csv")\
    .
