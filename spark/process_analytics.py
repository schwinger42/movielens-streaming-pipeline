
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split

def create_spark_session():
    """Create a simple Spark session"""
    spark = (SparkSession.builder
        .appName("MovieLens Analysis")
        .config("spark.sql.extensions", "org.apache.spark.sql.parquet")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate())
    
    return spark

def process_movie_analytics():
    """Process movie analytics data using Spark"""
    spark = create_spark_session()
    
    try:
        # Read the parquet file
        df = spark.read.parquet("output/medallion/gold/movie_analytics.parquet")
        
        # Show schema and sample data
        print("\nSchema:")
        df.printSchema()
        
        print("\nSample data:")
        df.show(5, truncate=False)
        
        # Perform some analysis
        print("\nRating statistics by tier:")
        df.groupBy("rating_tier").agg(
            {"avg_rating": "mean", "num_ratings": "sum"}
        ).show()
        
        # Create a temporary view for SQL queries
        df.createOrReplaceTempView("movie_analytics")
        
        # Run SQL query
        print("\nTop movies by average rating (min 100 ratings):")
        spark.sql("""
            SELECT movieId, title, avg_rating, num_ratings
            FROM movie_analytics
            WHERE num_ratings >= 100
            ORDER BY avg_rating DESC
            LIMIT 10
        """).show(truncate=False)
        
        # Save results to CSV for easy viewing
        top_movies = spark.sql("""
            SELECT movieId, title, avg_rating, num_ratings
            FROM movie_analytics
            WHERE num_ratings >= 100
            ORDER BY avg_rating DESC
            LIMIT 100
        """)
        
        top_movies.coalesce(1).write.mode("overwrite").option("header", "true").csv("output/analysis/top_movies")
        print("\nTop movies saved to output/analysis/top_movies/")
        
    except Exception as e:
        print(f"Error processing movie analytics: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    process_movie_analytics()
