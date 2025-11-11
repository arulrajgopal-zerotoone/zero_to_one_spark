// Databricks notebook source
val data = Seq(
  (1, "Alice", 25),
  (2, "Bob", 30),
  (3, "Charlie", 35)
)

val df = data.toDF("id", "name", "age")

display(df)

// COMMAND ----------

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

val schema = StructType(Seq(
  StructField("id", IntegerType, nullable = false),
  StructField("tags", ArrayType(StringType), nullable = true),
  StructField(
    "attributes",
    ArrayType(
      StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("value", IntegerType, nullable = false)
      ))
    ),
    nullable = true
  )
))

val rows = Seq(
  Row(1, Seq("red", "large"), Seq(Row("weight", 10), Row("height", 200))),
  Row(2, Seq("blue"), Seq(Row("weight", 12))),
  Row(3, Seq.empty[String], Seq(Row("width", 30), Row("depth", 5)))
)

val df2 = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

display(df2)

// COMMAND ----------
