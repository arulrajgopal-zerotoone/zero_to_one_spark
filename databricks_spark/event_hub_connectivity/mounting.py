# Databricks notebook source
storage_account_name = "dbwsmetastore"  
accountkey= ""  
container_name = "checkpoint"
fullname = "fs.azure.account.key." +storage_account_name+ ".blob.core.windows.net"

dbutils.fs.mount(  
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net", 
  mount_point =f"/mnt/{container_name}", 
  extra_configs = {fullname : accountkey}) 


dbutils.fs.mounts()


# COMMAND ----------


