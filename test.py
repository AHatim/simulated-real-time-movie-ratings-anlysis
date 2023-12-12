from results import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, corr, explode, split
from pyspark.sql.types import StructType, StructField, StringType, FloatType

spark = SparkSession.builder \
    .appName("test") \
    .getOrCreate()
    
# Read in movies dataset
json_file = '/home/hadoop/movie_ratings_project/processed datasets/movies_data.json'
df_movies = spark.read.json(json_file)

# Explode the genres array to get one row per genre
df_genres = df_movies.select("id", explode("genres").alias("genre"))

print(df_genres.head())
