from results import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, corr, explode, split
from pyspark.sql.types import StructType, StructField, StringType, FloatType


# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumerApp1") \
    .getOrCreate()

# Kafka bootstrap servers
bootstrap_servers = 'localhost:9092'

# Kafka topics to consume from
ratings_topic = 'ratings_data'

# Subscribe to Kafka topics
df_ratings = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", ratings_topic) \
    .load()

# Define schema for the data in JSON format
ratings_schema = StructType([
    StructField("movieId", StringType(), True),
    StructField("rating", FloatType(), True)
])

# Parse JSON data and select required fields
df_ratings_parsed = df_ratings.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), ratings_schema).alias("data")) \
    .select("data.*")

# Group by movieId and calculate the average rating
df_avg_ratings = df_ratings_parsed \
    .groupBy("movieId") \
    .agg(expr("avg(rating)").alias("avg_rating"))

# Read in movies dataset
json_file = '/home/hadoop/movie_ratings_project/processed datasets/movies_data.json'
df_movies = spark.read.json(json_file)

# Explode the genres array to get one row per genre
df_genres = df_movies.select("id", explode("genres").alias("genre"))

# Join df_ratings_parsed with df_genres on movieId
df_joined = df_ratings_parsed.join(df_genres, col("movieId") == col("id"), "inner")

# Group by genre and calculate the average rating for each genre
df_avg_ratings_by_genre = df_joined.groupBy("genre") \
    .agg(expr("avg(rating)").alias("avg_rating_by_genre"))

# Calculate the overall average rating for all movies
df_avg_rating_all_movies = df_ratings_parsed.agg(expr("avg(rating)").alias("avg_rating_all_movies"))

# Define output sinks
output_sink_by_genre = df_avg_ratings_by_genre \
    .writeStream \
    .outputMode('complete') \
    .format('console') \
    .start()

output_sink_all_movies = df_avg_rating_all_movies \
    .writeStream \
    .outputMode('complete') \
    .format('console') \
    .start()

