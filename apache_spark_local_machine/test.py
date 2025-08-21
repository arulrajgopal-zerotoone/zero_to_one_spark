from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType,DecimalType
import os 

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

account_key = os.getenv("AZURE_STORAGE_KEY")

spark.conf.set(
    "fs.azure.account.key.arulrajgopalshare.dfs.core.windows.net",
    account_key
)

schema = StructType([
    StructField("title", StringType(), True),
    StructField("directedBy", StringType(), True),
    StructField("starring", StringType(), True),
    StructField("avgRating", DecimalType(10, 5), True),
    StructField("imdbId", StringType(), True),
    StructField("item_id", IntegerType(), True)
])


df = spark.read.format("json") \
    .schema(schema) \
    .load("abfss://kaniniwitharul@arulrajgopalshare.dfs.core.windows.net/movie_lens_dataset_2e9bytes/source/metadata.json")

    
df.write.mode("overwrite").format("parquet") \
    .save("abfss://kaniniwitharul@arulrajgopalshare.dfs.core.windows.net/movie_lens_dataset_2e9bytes/target/metadata")
