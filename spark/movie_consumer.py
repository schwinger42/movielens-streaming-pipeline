from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import boto3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# S3 bucket name
BUCKET_NAME = "movielens-dev-datalake"

def create_spark_session():
    """Create and return a Spark session"""
    return (SparkSession.builder
            .appName("MovieLensStreamingConsumer")
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,"
                                            "org.apache.hadoop:hadoop-aws:3.3.4")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", f"s3a://{BUCKET_NAME}/warehouse")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
            .getOrCreate())

def process_movies_stream(spark):
    """Process the movies stream from Kafka"""
    # Define schema for movies data
    movies_schema = StructType([
        StructField("movieId", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("genres", StringType(), True)
    ])
    
    # Read movies stream from Kafka
    movies_df = (spark
                 .readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", "localhost:9092")
                 .option("subscribe", "movies")
                 .option("startingOffsets", "earliest")
                 .load())
    
    # Parse the value column from Kafka
    parsed_movies = movies_df.select(
        from_json(col("value").cast("string"), movies_schema).alias("data")
    ).select("data.*")
    
    # Process genres - split into array
    processed_movies = parsed_movies.withColumn(
        "genres_array", split(col("genres"), "\\|")
    )
    
    # Write to Iceberg table in silver layer
    query = (processed_movies
             .writeStream
             .format("iceberg")
             .outputMode("append")
             .option("path", "local.silver.movies")
             .option("checkpointLocation", f"s3a://{BUCKET_NAME}/checkpoints/movies")
             .start())
    
    return query

def process_ratings_stream(spark):
    """Process the ratings stream from Kafka"""
    # Define schema for ratings data
    ratings_schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", LongType(), True)
    ])
    
    # Read ratings stream from Kafka
    ratings_df = (spark
                  .readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .option("subscribe", "ratings")
                  .option("startingOffsets", "earliest")
                  .load())
    
    # Parse the value column from Kafka
    parsed_ratings = ratings_df.select(
        from_json(col("value").cast("string"), ratings_schema).alias("data")
    ).select("data.*")
    
    # Process timestamp to a proper date
    processed_ratings = parsed_ratings.withColumn(
        "rating_date", to_date(from_unixtime(col("timestamp")))
    )
    
    # Write to Iceberg table in silver layer
    query = (processed_ratings
             .writeStream
             .format("iceberg")
             .outputMode("append")
             .option("path", "local.silver.ratings")
             .option("checkpointLocation", f"s3a://{BUCKET_NAME}/checkpoints/ratings")
             .start())
    
    return query

if __name__ == "__main__":
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Initialize Iceberg tables if they don't exist
        spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.movies (
            movieId INT,
            title STRING,
            genres STRING,
            genres_array ARRAY<STRING>
        ) USING iceberg
        """)
        
        spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.ratings (
            userId INT,
            movieId INT,
            rating FLOAT,
            timestamp LONG,
            rating_date DATE
        ) USING iceberg
        """)
        
        # Process streams
        movies_query = process_movies_stream(spark)
        ratings_query = process_ratings_stream(spark)
        
        # Wait for termination
        spark.streams.awaitAnyTermination()
    
    except Exception as e:
        logger.error(f"Error in Spark streaming: {e}")
