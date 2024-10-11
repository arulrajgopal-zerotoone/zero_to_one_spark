# Databricks notebook source
# MAGIC %md
# MAGIC # ADLS connect through account key without mounting

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.arulrajstorageaccount.blob.core.windows.net",
    "<account-key>"
)

read_df = spark.read\
        .format("csv")\
        .option("Header",True)\
        .option("sep", "~")\
        .load("wasbs://private@arulrajstorageaccount.blob.core.windows.net/sample_employee/emp_details/emp_details.csv")

read_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # ADLS connect through account key with mounting

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

storage_account_name = "arulrajstorageaccount"
accountkey= "account_key"  
container_name = "private"
fullname = "fs.azure.account.key." +storage_account_name+ ".blob.core.windows.net"


dbutils.fs.mount(  
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/sample_employee/emp_details/", 
  mount_point =f"/mnt/{container_name}", 
  extra_configs = {fullname : accountkey}) 

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

read_df = spark.read\
        .format("csv")\
        .option("Header",True)\
        .option("sep", "~")\
        .load("/mnt/private/emp_details.csv")


read_df.display()

# COMMAND ----------

dbutils.fs.unmount('/mnt/private')

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


