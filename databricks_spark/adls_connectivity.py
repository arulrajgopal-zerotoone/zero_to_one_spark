# Databricks notebook source
# MAGIC %md
# MAGIC # ADLS connect through account key without mounting

# COMMAND ----------


spark.conf.set(
    "fs.azure.account.key.arulrajstorageaccount.blob.core.windows.net",
    "<account-key>"
)

# emp_full_df = spark.read\
#         .format("csv")\
#         .schema(schema)\
#         .option("Header",True)\
#         .option("sep", "~")\
#         .load("wasbs://private@arulrajstorageaccount.blob.core.windows.net/sample_employee/scd_implementations/emp_details.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC # ADLS connect through account key with mounting

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

storage_account_name = "sparkdemostorageaccount"
accountkey= ""  
container_name = "raw"
fullname = "fs.azure.account.key." +storage_account_name+ ".blob.core.windows.net"


dbutils.fs.mount(  
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net", 
  mount_point =f"/mnt/{container_name}", 
  extra_configs = {fullname : accountkey}) 

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# dbutils.fs.unmount('/mnt/raw')

# COMMAND ----------

# MAGIC %md
# MAGIC #ADLS connect through service principle

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.arulrajstorageaccount.blob.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.arulrajstorageaccount.blob.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.arulrajstorageaccount.blob.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.arulrajstorageaccount.blob.core.windows.net", "<client-secret>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.arulrajstorageaccount.blob.core.windows.net", "https://login.microsoftonline.com/<tenant-id>/oauth2/token")

# COMMAND ----------


