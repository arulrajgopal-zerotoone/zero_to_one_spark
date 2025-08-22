from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col
import os 
from utils import log_message

app_name = "people"

spark = SparkSession.builder \
    .appName(app_name) \
    .getOrCreate()

account_key = os.getenv("AZURE_STORAGE_KEY")

spark.conf.set("fs.azure.account.key.arulrajgopalshare.dfs.core.windows.net",account_key)
# spark.conf.set("spark.sql.files.maxPartitionBytes", "512m")
spark.conf.set("spark.sql.adaptive.enabled", "false")

# schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("first_name", StringType(), True),
#     StructField("middle_name", StringType(), True),
#     StructField("last_name", StringType(), True),
#     StructField("gender", StringType(), True),
#     StructField("birth_dt", TimestampType(), True),
#     StructField("ssn", StringType(), True),
#     StructField("salary", IntegerType(), True)
# ])


# people_df = spark.read.format("csv") \
#     .schema(schema) \
#     .load("abfss://kaniniwitharul@arulrajgopalshare.dfs.core.windows.net/people/people_csv/people.csv")\
#     .selectExpr("*","floor(months_between(current_date(), birth_dt) / 12) as age")\
#     .filter(col("salary")> 30000)\
#     .coalesce(3)


# log_message(app_name+" | people_df partition count :"+str(people_df.rdd.getNumPartitions()))

# grouped_df = people_df.groupBy("age").count().coalesce(3)

# log_message(app_name+" | grouped_df partition count :"+str(grouped_df.rdd.getNumPartitions()))

# people_df.write.mode("overwrite").partitionBy("age").format("parquet") \
#     .save("abfss://kaniniwitharul@arulrajgopalshare.dfs.core.windows.net/test_path/people")


read_df = spark.read.format('parquet').load("abfss://kaniniwitharul@arulrajgopalshare.dfs.core.windows.net/test_path/people")

log_message(app_name+" | read_df partition count :"+str(read_df.rdd.getNumPartitions()))

read_df.filter(col("age")==35).show()


