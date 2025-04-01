import time
import json
import logging
import pandas as pd
from kafka import KafkaProducer
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_kafka_producer():
    """Create and return a Kafka producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        return producer
    except Exception as e:
        logger.error(f"Error creating Kafka producer: {e}")
        raise

def stream_movies(producer, dataset_path, batch_size=100, delay=0.1):
    """Stream movies data to Kafka"""
    try:
        # Load movies data
        movies_file = os.path.join(dataset_path, 'movies.csv')
        logger.info(f"Loading movies data from {movies_file}")
        movies_df = pd.read_csv(movies_file)
        
        # Stream data in batches
        total_records = len(movies_df)
        for i in range(0, total_records, batch_size):
            batch = movies_df.iloc[i:i+batch_size]
            for _, row in batch.iterrows():
                # Convert row to dictionary
                movie_data = row.to_dict()
                # Send to Kafka
                producer.send('movies', value=movie_data)
            
            # Log progress
            logger.info(f"Sent {min(i+batch_size, total_records)}/{total_records} records to Kafka")
            time.sleep(delay)
        
        # Wait for all messages to be sent
        producer.flush()
        logger.info("All movie records have been streamed")
    
    except Exception as e:
        logger.error(f"Error streaming movies data: {e}")
        raise

def stream_ratings(producer, dataset_path, batch_size=1000, delay=0.05):
    """Stream ratings data to Kafka"""
    try:
        # Load ratings data
        ratings_file = os.path.join(dataset_path, 'ratings.csv')
        logger.info(f"Loading ratings data from {ratings_file}")
        
        # Stream only a sample for testing (first 100,000 rows)
        # Use chunksize for memory efficiency
        chunk_size = 50000  # Process 50k rows at a time
        chunks_processed = 0
        max_chunks = 2  # Limit to 2 chunks (100k rows) for testing
        
        for chunk in pd.read_csv(ratings_file, chunksize=chunk_size):
            if chunks_processed >= max_chunks:
                break
                
            total_in_chunk = len(chunk)
            
            for i in range(0, total_in_chunk, batch_size):
                batch = chunk.iloc[i:i+batch_size]
                for _, row in batch.iterrows():
                    # Convert row to dictionary
                    rating_data = row.to_dict()
                    # Send to Kafka
                    producer.send('ratings', value=rating_data)
                
                # Log progress
                logger.info(f"Sent {min(i+batch_size, total_in_chunk)}/{total_in_chunk} ratings to Kafka")
                time.sleep(delay)
            
            logger.info(f"Completed a chunk of {total_in_chunk} ratings")
            chunks_processed += 1
        
        # Wait for all messages to be sent
        producer.flush()
        logger.info(f"Streamed {chunks_processed * chunk_size} rating records")
    
    except Exception as e:
        logger.error(f"Error streaming ratings data: {e}")
        raise

if __name__ == "__main__":
    try:
        # Path to the MovieLens dataset
        dataset_path = 'raw/ml-20m'
        
        # Create Kafka producer
        producer = create_kafka_producer()
        
        # Stream movies data
        logger.info("Starting to stream movies data...")
        stream_movies(producer, dataset_path)
        
        # Stream ratings data
        logger.info("Starting to stream ratings data...")
        stream_ratings(producer, dataset_path)
        
        logger.info("Streaming completed successfully")
    
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
