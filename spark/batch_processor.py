import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create a Spark session for batch processing"""
    return (SparkSession.builder
            .appName("MovieLensBatchProcessor")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
            .getOrCreate())

def process_movies(spark):
    """Process movies data from the local dataset"""
    # Define schema for movies data
    movies_schema = StructType([
        StructField("movieId", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("genres", StringType(), True)
    ])
    
    # Read the CSV file
    movies_df = spark.read.csv('raw/ml-20m/movies.csv', header=True, schema=movies_schema)
    
    # Process the data
    processed_df = movies_df.withColumn(
        "genres_array", split(col("genres"), "\\|")
    )
    
    # Show some results
    logger.info("Movies processed. Sample data:")
    processed_df.show(5, truncate=False)
    
    # Return the processed data
    return processed_df

def process_ratings(spark):
    """Process ratings data from the local dataset"""
    # Define schema for ratings data
    ratings_schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", LongType(), True)
    ])
    
    # Read the CSV file
    ratings_df = spark.read.csv('raw/ml-20m/ratings.csv', header=True, schema=ratings_schema)
    
    # Process the data - convert timestamp to date
    processed_df = ratings_df.withColumn(
        "rating_date", to_date(from_unixtime(col("timestamp")))
    )
    
    # Show some results
    logger.info("Ratings processed. Sample data:")
    processed_df.show(5)
    
    # Return the processed data
    return processed_df

def create_movie_analytics(movies_df, ratings_df):
    """Create movie analytics by joining movies and ratings"""
    # Group ratings by movieId
    ratings_agg = ratings_df.groupBy("movieId").agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("num_ratings")
    )
    
    # Join with movies
    movie_analytics = movies_df.join(ratings_agg, "movieId")
    
    # Add rating category
    movie_analytics = movie_analytics.withColumn(
        "rating_category",
        when(col("avg_rating") >= 4.0, "High")
        .when(col("avg_rating") >= 3.0, "Medium")
        .otherwise("Low")
    )
    
    # Show some results
    logger.info("Movie analytics created. Sample data:")
    movie_analytics.show(5)
    
    return movie_analytics

if __name__ == "__main__":
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Process movies and ratings
        movies_df = process_movies(spark)
        ratings_df = process_ratings(spark)
        
        # Create movie analytics
        movie_analytics = create_movie_analytics(movies_df, ratings_df)
        
        # Write to CSV for inspection
        logger.info("Writing results to CSV...")
        movie_analytics.write.mode("overwrite").option("header", "true").csv("output/movie_analytics")
        
        logger.info("Processing completed successfully")
    
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")
    finally:
        # Stop the Spark session
        if 'spark' in locals():
            spark.stop()
