import os
import pandas as pd
import numpy as np
import logging
import argparse
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define paths for medallion architecture
BRONZE_PATH = "output/medallion/bronze"
SILVER_PATH = "output/medallion/silver"
GOLD_PATH = "output/medallion/gold"

def setup_medallion_dirs():
    """Create directories for medallion architecture"""
    for path in [BRONZE_PATH, SILVER_PATH, GOLD_PATH]:
        os.makedirs(path, exist_ok=True)
        logger.info(f"Created directory: {path}")

def process_bronze_layer():
    """Process raw data into bronze layer (minimal transformations)"""
    # Movies
    movies_df = pd.read_csv('raw/ml-20m/movies.csv')
    movies_df['_ingestion_timestamp'] = datetime.now().isoformat()
    movies_df.to_parquet(f"{BRONZE_PATH}/movies.parquet", index=False)
    
    # Ratings (sample for performance)
    ratings_df = pd.read_csv('raw/ml-20m/ratings.csv', nrows=100000)
    ratings_df['_ingestion_timestamp'] = datetime.now().isoformat()
    ratings_df.to_parquet(f"{BRONZE_PATH}/ratings.parquet", index=False)
    
    # Tags
    tags_df = pd.read_csv('raw/ml-20m/tags.csv', nrows=50000)
    tags_df['_ingestion_timestamp'] = datetime.now().isoformat()
    tags_df.to_parquet(f"{BRONZE_PATH}/tags.parquet", index=False)
    
    logger.info("Bronze layer processing completed")

def process_silver_layer():
    """Process bronze data into silver layer (cleaned, standardized)"""
    # Movies - clean and add genre arrays
    movies_df = pd.read_parquet(f"{BRONZE_PATH}/movies.parquet")
    movies_df['genres_array'] = movies_df['genres'].str.split('|')
    movies_df['title_clean'] = movies_df['title'].str.replace(r'\s*\(\d{4}\)$', '', regex=True)
    movies_df['year'] = movies_df['title'].str.extract(r'\((\d{4})\)$')
    movies_df.to_parquet(f"{SILVER_PATH}/movies.parquet", index=False)
    
    # Ratings - add datetime
    ratings_df = pd.read_parquet(f"{BRONZE_PATH}/ratings.parquet")
    ratings_df['rating_date'] = pd.to_datetime(ratings_df['timestamp'], unit='s')
    ratings_df.to_parquet(f"{SILVER_PATH}/ratings.parquet", index=False)
    
    # Tags - clean
    tags_df = pd.read_parquet(f"{BRONZE_PATH}/tags.parquet")
    tags_df['tag_clean'] = tags_df['tag'].str.lower().str.strip()
    tags_df['tag_date'] = pd.to_datetime(tags_df['timestamp'], unit='s')
    tags_df.to_parquet(f"{SILVER_PATH}/tags.parquet", index=False)
    
    logger.info("Silver layer processing completed")

def process_gold_layer():
    """Process silver data into gold layer (analytics-ready)"""
    try:
        # Movie analytics
        movies_df = pd.read_parquet(f"{SILVER_PATH}/movies.parquet")
        ratings_df = pd.read_parquet(f"{SILVER_PATH}/ratings.parquet")
        
        # Movie ratings aggregation
        movie_ratings = ratings_df.groupby('movieId').agg(
            avg_rating=('rating', 'mean'),
            num_ratings=('rating', 'count')
        ).reset_index()
        
        # Join with movies
        movie_analytics = pd.merge(movies_df, movie_ratings, on='movieId', how='left')
        
        # Add rating categories - handling NaN values
        movie_analytics['avg_rating'] = movie_analytics['avg_rating'].fillna(0)
        movie_analytics['rating_tier'] = pd.cut(
            movie_analytics['avg_rating'], 
            bins=[0, 2.5, 3.5, 4.2, 5.0], 
            labels=['Low', 'Medium', 'High', 'Top']
        )
        
        # Save to gold layer
        movie_analytics.to_parquet(f"{GOLD_PATH}/movie_analytics.parquet", index=False)
        
        # Process genres - manual approach to avoid list handling issues
        # Extract genres from the movies dataframe
        genre_rows = []
        for _, row in movies_df.iterrows():
            movie_id = row['movieId']
            title = row['title']
            year = row.get('year', None)
            
            # Look up ratings
            movie_ratings_row = movie_ratings[movie_ratings['movieId'] == movie_id]
            avg_rating = movie_ratings_row['avg_rating'].values[0] if len(movie_ratings_row) > 0 else 0
            num_ratings = movie_ratings_row['num_ratings'].values[0] if len(movie_ratings_row) > 0 else 0
            
            # Get genres as string and split
            genres_str = row.get('genres', '')
            if pd.notna(genres_str) and genres_str != '(no genres listed)':
                genres_list = genres_str.split('|')
                
                for genre in genres_list:
                    if genre and genre != '(no genres listed)':
                        genre_rows.append({
                            'movieId': movie_id,
                            'genre': genre,
                            'title': title,
                            'year': year,
                            'avg_rating': avg_rating,
                            'num_ratings': num_ratings
                        })
        
        # Create dataframe if we have genres to process
        if genre_rows:
            genre_df = pd.DataFrame(genre_rows)
            
            # Calculate average rating per genre
            genre_stats = genre_df.groupby('genre').agg(
                avg_genre_rating=('avg_rating', 'mean'),
                total_movies=('movieId', 'nunique'),
                total_ratings=('num_ratings', 'sum')
            ).reset_index()
            
            # Save to gold layer
            genre_df.to_parquet(f"{GOLD_PATH}/movie_genres.parquet", index=False)
            genre_stats.to_parquet(f"{GOLD_PATH}/genre_stats.parquet", index=False)
            logger.info("Genre analytics created successfully")
        else:
            logger.warning("No valid genre data found to process")
                
        logger.info("Gold layer processing completed")
    
    except Exception as e:
        logger.error(f"Error in gold layer processing: {e}")
        # Continue execution rather than stopping completely
        logger.info("Skipping problematic parts in gold layer")

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Process MovieLens data through medallion architecture.')
    parser.add_argument('--layer', choices=['bronze', 'silver', 'gold', 'all'], 
                      default='all', help='Layer to process (bronze, silver, gold, or all)')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    try:
        # Set up directory structure
        setup_medallion_dirs()
        
        # Process selected layer(s)
        if args.layer == 'bronze' or args.layer == 'all':
            logger.info("Starting bronze layer processing...")
            process_bronze_layer()
            
        if args.layer == 'silver' or args.layer == 'all':
            logger.info("Starting silver layer processing...")
            process_silver_layer()
            
        if args.layer == 'gold' or args.layer == 'all':
            logger.info("Starting gold layer processing...")
            process_gold_layer()
        
        logger.info(f"Medallion architecture processing completed for layer(s): {args.layer}")
    except Exception as e:
        logger.error(f"Error implementing medallion architecture: {e}")
