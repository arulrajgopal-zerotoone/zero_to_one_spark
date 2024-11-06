// Databricks notebook source
import org.apache.spark.sql.functions.{current_timestamp}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

// COMMAND ----------

val schema = StructType(Array(
  StructField("empid", StringType, true),
  StructField("name", StringType, true),
  StructField("designation", StringType, true),
  StructField("salary", IntegerType, true),
  StructField("city", StringType, true),
  StructField("department", StringType, true)
))

// COMMAND ----------

val df = spark.readStream
              .format("csv")
              .schema(schema)
              .load("/mnt/raw/employee_details")


val dfWithLoadedTime = df.withColumn("loaded_time", current_timestamp())

// COMMAND ----------


