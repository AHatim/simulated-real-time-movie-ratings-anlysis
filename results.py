import json
from datetime import datetime
from pyspark.sql.functions import current_timestamp

def save_corr_to_json(df, epoch_id):
    """
    Save the current time and correlation results to a JSON file.
    """
    # Add a timestamp column with the current timestamp
    df = df.withColumn("timestamp", current_timestamp())

    # Select only the necessary columns for insertion
    df_to_insert = df.select("timestamp", "correlation")

    # Convert the DataFrame to a list of dictionaries
    data_list = df_to_insert.collect()
    data_dicts = [{"timestamp": str(row.timestamp), "correlation": row.correlation} for row in data_list]

    # Json file to store results
    json_filename = "/home/hadoop/movie_ratings_project/database/budget_correlation_results.json"

    # Write the data to the JSON file in append mode
    with open(json_filename, 'a') as json_file:
        json.dump(data_dicts, json_file)
        json_file.write('\n')

def save_genre_avg_to_json(df, epoch_id):
    """
    Save the current time and average ratings by genre to a JSON file.
    """
    # Add a timestamp column with the current timestamp
    df = df.withColumn("timestamp", current_timestamp())
    
    # Select only the necessary columns for insertion
    df_to_insert = df.select("timestamp", "correlation")

    # Convert the DataFrame to a list of dictionaries
    data_list = df_avg_ratings_by_genre.collect()
    data_dicts = [{"timestamp": str(row.timestamp), "genre": row.genre, "avg_rating": row.avg_rating_by_genre} for row in data_list]

    # Specify the JSON file path
    json_filename = "/path/to/output_folder/avg_ratings_by_genre.json"

    # Write the data to the JSON file in append mode
    with open(json_filename, 'a') as json_file:
        json.dump(data_dicts, json_file)
        json_file.write('\n')

