import pandas as pd
import json
import ast  # Import the ast module for literal evaluation

# Function to extract names from genres and production_companies columns
def extract_names(data_str):
    try:
        data_list = ast.literal_eval(data_str)
        if isinstance(data_list, list):
            return [item['name'] for item in data_list]
        else:
            return []
    except (ValueError, SyntaxError):
        return []


# Function to preprocess movies_metadata.csv
def preprocess_movies_metadata(input_file, output_file):
    # Read the CSV file
    movies_df = pd.read_csv(input_file)

    movies_df['budget'] = pd.to_numeric(movies_df['budget'], errors='coerce') 
    movies_df['id'] = movies_df['id'].astype(str)


    selected_columns = ["budget", "genres", "id", "runtime", "original_language", "production_companies"]
    movies_df = movies_df[selected_columns]
    
    movies_df['genres'] = movies_df['genres'].apply(extract_names)
    movies_df['production_companies'] = movies_df['production_companies'].apply(extract_names)

    # Convert dataframe to JSON
    movies_json = movies_df.to_json(orient='records')

    # Write JSON to a file
    with open(output_file, 'w') as json_file:
        json_file.write(movies_json)

# Function to preprocess ratings.csv
def preprocess_ratings(input_file, output_file):
    # Read the CSV file
    ratings_df = pd.read_csv(input_file)

    ratings_df['movieId'] = ratings_df['movieId'].astype(str)
    ratings_df['rating'] = ratings_df['rating'].astype(float)

    # Convert dataframe to JSON
    ratings_json = ratings_df.to_json(orient='records')

    # Write JSON to a file
    with open(output_file, 'w') as json_file:
        json_file.write(ratings_json)

# Specify input and output files
movies_metadata_input = '/home/hadoop/movie_ratings_project/archive/movies_metadata.csv'
ratings_input = '/home/hadoop/movie_ratings_project/archive/ratings_small.csv'
movies_output = '/home/hadoop/movie_ratings_project/processed datasets/movies_data.json'
ratings_output = '/home/hadoop/movie_ratings_project/processed datasets/ratings_data.json'

# Perform preprocessing
preprocess_movies_metadata(movies_metadata_input, movies_output)
preprocess_ratings(ratings_input, ratings_output)

print("Preprocessing completed.")

