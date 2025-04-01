import subprocess
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def install_pyspark():
    """Install PySpark if not already installed"""
    try:
        subprocess.check_call(["pip", "install", "pyspark==3.3.0"])
        logger.info("PySpark installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error installing PySpark: {e}")
        return False

def create_spark_session_script():
    """Create a script to initialize a simple Spark session"""
    script_content = '''
from pyspark.sql import SparkSession

def create_spark_session(app_name="MovieLens Analysis"):
    """Create a simple Spark session"""
    spark = (SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.spark.sql.parquet")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate())
    
    return spark

if __name__ == "__main__":
    # Test creating a Spark session
    spark = create_spark_session()
    print(f"Spark version: {spark.version}")
    print("Spark session created successfully")
    
    # List all available functions
    print("\\nAvailable Spark SQL functions:")
    print(", ".join(sorted([f for f in dir(spark.sql.functions) if not f.startswith("_")])))
    
    spark.stop()
'''
    
    with open("spark/spark_session.py", "w") as f:
        f.write(script_content)
    
    logger.info("Created Spark session initialization script")

def create_spark_dataframe_script():
    """Create a script to create Spark DataFrames from Parquet files"""
    script_content = '''
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
        print("\\nSchema:")
        df.printSchema()
        
        print("\\nSample data:")
        df.show(5, truncate=False)
        
        # Perform some analysis
        print("\\nRating statistics by tier:")
        df.groupBy("rating_tier").agg(
            {"avg_rating": "mean", "num_ratings": "sum"}
        ).show()
        
        # Create a temporary view for SQL queries
        df.createOrReplaceTempView("movie_analytics")
        
        # Run SQL query
        print("\\nTop movies by average rating (min 100 ratings):")
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
        print("\\nTop movies saved to output/analysis/top_movies/")
        
    except Exception as e:
        print(f"Error processing movie analytics: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    process_movie_analytics()
'''
    
    with open("spark/process_analytics.py", "w") as f:
        f.write(script_content)
    
    # Create output directory
    os.makedirs("output/analysis", exist_ok=True)
    
    logger.info("Created Spark DataFrame processing script")

def main():
    """Main function to set up Spark environment"""
    logger.info("Setting up Spark environment")
    
    # Install PySpark
    if install_pyspark():
        # Create scripts
        create_spark_session_script()
        create_spark_dataframe_script()
        
        logger.info("Spark setup complete. Use the following scripts:")
        logger.info("1. spark/spark_session.py - Test Spark session creation")
        logger.info("2. spark/process_analytics.py - Process movie analytics data")
    else:
        logger.error("Failed to set up Spark environment")

if __name__ == "__main__":
    main()
