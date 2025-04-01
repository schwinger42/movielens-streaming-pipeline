import os
import logging
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_movies():
    """Process movies data using pandas for simplicity"""
    # Read the CSV file
    movies_df = pd.read_csv('raw/ml-20m/movies.csv')
    
    # Process the data - split genres into a list
    movies_df['genres_array'] = movies_df['genres'].str.split('|')
    
    # Show some results
    logger.info("Movies processed. Sample data:")
    logger.info(movies_df.head())
    
    return movies_df

def process_ratings():
    """Process ratings data using pandas for simplicity"""
    # Sample only 100k ratings for performance
    ratings_df = pd.read_csv('raw/ml-20m/ratings.csv', nrows=100000)
    
    # Add a datetime column from timestamp
    ratings_df['rating_date'] = pd.to_datetime(ratings_df['timestamp'], unit='s')
    
    # Show some results
    logger.info("Ratings processed. Sample data:")
    logger.info(ratings_df.head())
    
    return ratings_df

def create_movie_analytics(movies_df, ratings_df):
    """Create movie analytics by merging movies and ratings"""
    # Group ratings by movieId
    ratings_agg = ratings_df.groupby('movieId').agg(
        avg_rating=('rating', 'mean'),
        num_ratings=('rating', 'count')
    ).reset_index()
    
    # Merge with movies
    movie_analytics = pd.merge(movies_df, ratings_agg, on='movieId', how='left')
    
    # Add rating category
    def get_category(rating):
        if pd.isna(rating):
            return 'Unknown'
        elif rating >= 4.0:
            return 'High'
        elif rating >= 3.0:
            return 'Medium'
        else:
            return 'Low'
    
    movie_analytics['rating_category'] = movie_analytics['avg_rating'].apply(get_category)
    
    # Show some results
    logger.info("Movie analytics created. Sample data:")
    logger.info(movie_analytics.head())
    
    return movie_analytics

if __name__ == "__main__":
    try:
        # Create output directory if it doesn't exist
        os.makedirs('output', exist_ok=True)
        
        # Process movies and ratings
        logger.info("Processing movies data...")
        movies_df = process_movies()
        
        logger.info("Processing ratings data...")
        ratings_df = process_ratings()
        
        # Create movie analytics
        logger.info("Creating movie analytics...")
        movie_analytics = create_movie_analytics(movies_df, ratings_df)
        
        # Write to CSV for inspection
        logger.info("Writing results to CSV...")
        movie_analytics.to_csv("output/movie_analytics.csv", index=False)
        
        logger.info("Processing completed successfully")
    
    except Exception as e:
        logger.error(f"Error in processing: {e}")
