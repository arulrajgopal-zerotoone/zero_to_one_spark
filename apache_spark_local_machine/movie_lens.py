from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType,DecimalType
import os 
from datetime import datetime


app_name = "movie_lens"

spark = SparkSession.builder \
    .appName(app_name) \
    .getOrCreate()

account_key = os.getenv("AZURE_STORAGE_KEY")

spark.conf.set(
    "fs.azure.account.key.arulrajgopalshare.dfs.core.windows.net",
    account_key
)

# movie_detail
movie_detail_schema = StructType([
    StructField("title", StringType(), True),
    StructField("directedBy", StringType(), True),
    StructField("starring", StringType(), True),
    StructField("avgRating", DecimalType(10, 5), True),
    StructField("imdbId", StringType(), True),
    StructField("item_id", IntegerType(), True)
])


movie_details_df = spark.read.format("json") \
    .schema(movie_detail_schema) \
    .load("abfss://kaniniwitharul@arulrajgopalshare.dfs.core.windows.net/movielens_2gb/metadata.json")

# logging
log_message = app_name+" | movie_details_df partition count :"+str(movie_details_df.rdd.getNumPartitions())
spark.createDataFrame([(str(datetime.now()), log_message)]).write.mode("append").format("parquet")\
.save("abfss://kaniniwitharul@arulrajgopalshare.dfs.core.windows.net/test_path/log_table/")

    
movie_details_df.write.mode("overwrite").format("parquet") \
    .save("abfss://kaniniwitharul@arulrajgopalshare.dfs.core.windows.net/test_path/movielens_detail/")


# movie_schema
# movie_schema = StructType([
#     StructField("title", StringType(), True),
#     StructField("directedBy", StringType(), True),
#     StructField("starring", StringType(), True),
#     StructField("avgRating", DecimalType(10, 5), True),
#     StructField("imdbId", StringType(), True),
#     StructField("item_id", IntegerType(), True)
# ])


# movie_ratings_df = spark.read.format("json") \
#     .option("inferSchema",True) \
#     .load("abfss://kaniniwitharul@arulrajgopalshare.dfs.core.windows.net/movielens_2gb/ratings.json")

# movie_ratings_df.rdd.getNumPartitions()


# movie_ratings_df.write.mode("overwrite").format("parquet") \
#     .save("abfss://kaniniwitharul@arulrajgopalshare.dfs.core.windows.net/test_path/movie_ratings/")


    








