# Databricks notebook source
# MAGIC %md
# MAGIC #Azure SQL connect using username & password

# COMMAND ----------

# MAGIC %md
# MAGIC ## reading

# COMMAND ----------

jdbcHostname = "arulsqlserver.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "database1"
jdbcUsername = "Arulraj"
jdbcPassword = "test@1234"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};"

employee_df = spark.read \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "Employees") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .option("driver", jdbcDriver) \
    .load()

# COMMAND ----------

employee_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## writing

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema for the students dataframe
schema = StructType([
    StructField("student_id", IntegerType(), nullable=False),
    StructField("student_name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=False),
    StructField("grade", StringType(), nullable=False)
])

# Create the students dataframe with the specified schema and data
students_data = [
    (1, "John Doe", 18, "A"),
    (2, "Jane Smith", 17, "B"),
    (3, "Mike Johnson", 19, "A"),
    (4, "Emily Brown", 16, "C")
]

students_df = spark.createDataFrame(students_data, schema)

students_df.display()

# COMMAND ----------

jdbcHostname = "arulsqlserver.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "database1"
jdbcUsername = "Arulraj"
jdbcPassword = "test@1234"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};"

students_df.write \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "Students") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .option("driver", jdbcDriver) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #Azure SQL connect using spn

# COMMAND ----------

jdbcHostname = "arulsqlserver.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "database1"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

spnClientId = ""
spnClientSecret = ""

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};"

df = spark.read \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "Employees") \
    .option("user", spnClientId) \
    .option("password", spnClientSecret) \
    .option("driver", jdbcDriver)\
    .load()




