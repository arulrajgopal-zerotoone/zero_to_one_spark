from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import os 

spark = SparkSession.builder \
    .appName("people") \
    .getOrCreate()

account_key = os.getenv("AZURE_STORAGE_KEY")

spark.conf.set(
    "fs.azure.account.key.arulrajgopalshare.dfs.core.windows.net",
    account_key
)

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("middle_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birth_dt", TimestampType(), True),
    StructField("ssn", StringType(), True),
    StructField("salary", IntegerType(), True)
])


df = spark.read.format("csv") \
    .schema(schema) \
    .load("abfss://kaniniwitharul@arulrajgopalshare.dfs.core.windows.net/people/people_csv/people.csv")\
    .selectExpr("*","floor(months_between(current_date(), birth_dt) / 12) as age")



grouped_df = df.groupBy("age").count()

grouped_df.write.mode("overwrite").format("parquet") \
    .save("abfss://kaniniwitharul@arulrajgopalshare.dfs.core.windows.net/test_path/people")


