from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import time
import json

def create_topic(admin_client, topic):
    # Define the new topic
    new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)

    # Create the topic
    admin_client.create_topics([new_topic])


def publish_to_kafka(producer, topic, data):
    producer.send(topic, value=json.dumps(data).encode('utf-8'))

def create_producer(bootstrap_servers):
    return KafkaProducer(bootstrap_servers=bootstrap_servers)

def flush_producer(producer):
    producer.flush()

def close_producer(producer):
    producer.close()

def publish_records_from_json(producer, json_file, topic):
    with open(json_file, 'r') as file:
        data = json.load(file)
        for record in data:
            publish_to_kafka(producer, topic, record)
            time.sleep(1)

# Set configuration
bootstrap_servers = 'localhost:9092'

# Create admin client
admin_client = KafkaAdminClient()

# Create topic
#create_topic(admin_client, 'ratings_data')

# Create producer
producer = create_producer(bootstrap_servers)

# Read ratings_data.json 
ratings_json_file = '/home/hadoop/movie_ratings_project/processed datasets/ratings_data.json'

# Publish data to kafka topic
publish_records_from_json(producer, ratings_json_file, 'ratings_data')

# Flush producer
flush_producer(producer)

# Close producer
close_producer(producer)