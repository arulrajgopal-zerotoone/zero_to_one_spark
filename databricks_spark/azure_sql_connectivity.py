# Databricks notebook source
spark.conf.set("fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net", "<your-access-key>")

# COMMAND ----------

# read a file

df = spark.read\
    .format("csv")\


# Databricks notebook source
jdbcHostname = "mysqlserver9898.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "mydb"
jdbcUsername = "Arulraj"
jdbcPassword = "test@1234"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"

# COMMAND ----------

races_circuits_df = spark.read.table('curated.races_circuits')
results_drivers_df = spark.read.table('curated.results_drivers')

# COMMAND ----------

races_circuits_df.write \
    .format("jdbc")\
    .option("url",jdbcUrl)\
    .mode("overwrite")\
    .option("dbtable","races_circuits")\
    .save()


# COMMAND ----------

results_drivers_df.write \
    .format("jdbc")\
    .option("url",jdbcUrl)\
    .mode("overwrite")\
    .option("dbtable","results_drivers")\
    .save()
