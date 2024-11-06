// Databricks notebook source
import org.apache.spark.sql.functions.{current_timestamp, col}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
// import org.apache.spark.eventhubs._
import org.apache.spark.sql.streaming.Trigger

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
              .option("Header",True)
              .load("/mnt/test/employee_details")


val dfWithLoadedTime = df.withColumn("loaded_time", current_timestamp())

val filteredDf = dfWithLoadedTime.filter(!col("department").equalTo("Finance"))

// COMMAND ----------

val curatedPath = "/mnt/curated/employee_details"

val query = filteredDf
  .writeStream
  .format("delta")
  .option("path", curatedPath)
  .option("checkpointLocation", "/mnt/checkpoints/employee_details_curated")
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()

// COMMAND ----------

val df = spark.read.format("delta").load("/mnt/curated/employee_details")
display(df)

// COMMAND ----------

// val connectionString = ConnectionStringBuilder("Endpoint=sb://<your-event-hub-namespace>.servicebus.windows.net/;SharedAccessKeyName=<your-policy-name>;SharedAccessKey=<your-shared-access-key>;EntityPath=<your-event-hub-name>")
//   .setEventHubName("<your-event-hub-name>")
//   .build

// val eventHubsConf = EventHubsConf(connectionString)
//   .setStartingPosition(EventPosition.fromEndOfStream)

// val query = filteredDf
//   .writeStream
//   .format("eventhubs")
//   .options(eventHubsConf.toMap)
//   .trigger(Trigger.ProcessingTime("10 seconds"))
//   .option("checkpointLocation", "/mnt/checkpoints/employee_details")
//   .start()
