from results import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, corr
from pyspark.sql.types import StructType, StructField, StringType, FloatType


# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumerApp") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

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

# Set index columns
df_movies = df_movies.withColumn('index', col('id'))
df_avg_ratings = df_avg_ratings.withColumn('index', col('movieId'))

# Join datsets on movieId
df_combined = df_movies.join(df_avg_ratings, col('id') == col('movieId'), 'inner')

# Preform correlation analysis on budget
correlation_result = df_combined.select(corr('budget', 'avg_rating').alias('correlation'))

# Define output sink
output_sink = correlation_result \
    .writeStream \
    .outputMode('update') \
    .foreachBatch(save_corr_to_json) \
    .start()
    
output_sink.awaitTermination()
