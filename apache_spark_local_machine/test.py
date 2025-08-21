from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
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
    StructField("dateAdded", StringType(), True),
    StructField("avgRating", DecimalType(10, 5), True),
    StructField("imdbId", IntegerType(), True),
    StructField("item_id", IntegerType(), True)
])


df = spark.read.format("json") \
    .schema(schema) \
    .load("abfss://kaniniwitharul@arulrajgopalshare.dfs.core.windows.net/movie_lens_dataset_2e9bytes/metadata.json")\

    
df.show()
# title – movie title (84,484 unique titles)
# directedBy – directors separated by comma (‘,’)
# starring – actors separated by comma (‘,’)
# dateAdded – date, when the movie was added to MovieLens
# avgRating – average rating of a movie on MovieLens
# imdbId – movie id on the IMDB website (84,661 unique ids)
# item_id – movie id, which is consistent across files (84,661 unique ids)
