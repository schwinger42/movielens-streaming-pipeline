import os
import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
WAREHOUSE_DIR = "iceberg_warehouse"
S3_BUCKET = "s3://movielens-dev-datalake"
SPARK_JAR_DIR = "jars"

def download_iceberg_jars():
    """Download required Iceberg JARs for Spark integration"""
    os.makedirs(SPARK_JAR_DIR, exist_ok=True)
    
    # List of required JAR files
    jar_urls = [
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_1.3.1/1.3.1/iceberg-spark-runtime-3.3_1.3.1-1.3.1.jar",
        "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar",
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    ]
    
    def download_jar(url):
        jar_name = url.split("/")[-1]
        jar_path = os.path.join(SPARK_JAR_DIR, jar_name)
        
        if not os.path.exists(jar_path):
            logger.info(f"Downloading {jar_name}...")
            try:
                subprocess.run(["wget", url, "-O", jar_path], check=True)
                logger.info(f"Downloaded {jar_name}")
                return True
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to download {jar_name}: {e}")
                return False
        else:
            logger.info(f"JAR already exists: {jar_name}")
            return True
    
    # Download JARs in parallel
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(download_jar, jar_urls))
    
    return all(results)

def create_spark_session_script():
    """Create a script to initialize Spark with Iceberg support"""
    script_content = '''
from pyspark.sql import SparkSession
import os

def create_spark_session(app_name="MoviLens Iceberg"):
    """Create a Spark session with Iceberg support"""
    # Get the location of the JARs directory
    jar_dir = os.path.abspath("jars")
    jars = [os.path.join(jar_dir, jar) for jar in os.listdir(jar_dir) if jar.endswith(".jar")]
    jars_str = ",".join(jars)
    
    # Create the Spark session
    spark = (SparkSession.builder
        .appName(app_name)
        .config("spark.jars", jars_str)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "iceberg_warehouse")
        .config("spark.sql.catalog.s3", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.s3.type", "hadoop")
        .config("spark.sql.catalog.s3.warehouse", "s3://movielens-dev-datalake/iceberg_warehouse")
        .config("spark.sql.catalog.s3.s3.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate())
    
    return spark
'''
    
    with open("iceberg/spark_session.py", "w") as f:
        f.write(script_content)
    
    logger.info("Created Spark session initialization script")

def create_iceberg_tables_script():
    """Create a script to create Iceberg tables from the parquet files"""
    script_content = '''
from pyspark.sql import SparkSession
import sys
import os

# Add the iceberg directory to the Python path
sys.path.append("iceberg")
from spark_session import create_spark_session

# Initialize Spark session
spark = create_spark_session("MovieLens Iceberg Tables")

def create_iceberg_table(parquet_path, table_name, catalog="local"):
    """Create an Iceberg table from a Parquet file"""
    try:
        # Read the Parquet file
        df = spark.read.parquet(parquet_path)
        
        # Create the Iceberg table
        df.writeTo(f"{catalog}.movielens.{table_name}").createOrReplace()
        
        print(f"Created Iceberg table: {catalog}.movielens.{table_name}")
        return True
    except Exception as e:
        print(f"Error creating Iceberg table {table_name}: {e}")
        return False

def main():
    # Create namespace if it doesn't exist
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.movielens")
    
    # Define the tables to create
    tables = [
        ("output/medallion/bronze/movies.parquet", "bronze_movies"),
        ("output/medallion/bronze/ratings.parquet", "bronze_ratings"),
        ("output/medallion/bronze/tags.parquet", "bronze_tags"),
        ("output/medallion/silver/movies.parquet", "silver_movies"),
        ("output/medallion/silver/ratings.parquet", "silver_ratings"),
        ("output/medallion/silver/tags.parquet", "silver_tags"),
        ("output/medallion/gold/movie_analytics.parquet", "gold_movie_analytics")
    ]
    
    # Create tables
    for parquet_path, table_name in tables:
        if os.path.exists(parquet_path):
            create_iceberg_table(parquet_path, table_name)
        else:
            print(f"Parquet file not found: {parquet_path}")

if __name__ == "__main__":
    main()
'''
    
    with open("iceberg/create_tables.py", "w") as f:
        f.write(script_content)
    
    logger.info("Created Iceberg tables creation script")

def create_iceberg_demo_script():
    """Create a script that demonstrates Iceberg features like time travel and schema evolution"""
    script_content = '''
import sys
import os

# Add the iceberg directory to the Python path
sys.path.append("iceberg")
from spark_session import create_spark_session
from pyspark.sql.functions import col, expr

# Initialize Spark session
spark = create_spark_session("MovieLens Iceberg Demo")

def show_table_history(table_name, catalog="local"):
    """Show the history of an Iceberg table"""
    print(f"\\n=== History for {catalog}.movielens.{table_name} ===")
    history = spark.sql(f"SELECT * FROM {catalog}.movielens.{table_name}.history")
    history.show(truncate=False)

def time_travel_demo(table_name, catalog="local"):
    """Demonstrate time travel capabilities of Iceberg"""
    # Get current data
    print(f"\\n=== Current data for {catalog}.movielens.{table_name} ===")
    current = spark.sql(f"SELECT * FROM {catalog}.movielens.{table_name}")
    current.show(5, truncate=False)
    
    # Get snapshot ID for time travel
    history = spark.sql(f"SELECT * FROM {catalog}.movielens.{table_name}.history")
    if history.count() > 1:
        snapshot_id = history.select("snapshot_id").collect()[1][0]
        
        print(f"\\n=== Time travel to snapshot {snapshot_id} ===")
        old_version = spark.sql(f"SELECT * FROM {catalog}.movielens.{table_name} VERSION AS OF {snapshot_id}")
        old_version.show(5, truncate=False)
    else:
        print("Need at least two snapshots for time travel demo")

def schema_evolution_demo(table_name, catalog="local"):
    """Demonstrate schema evolution capabilities of Iceberg"""
    # Add a new column
    print(f"\\n=== Adding new column to {catalog}.movielens.{table_name} ===")
    
    # Get current data
    current = spark.sql(f"SELECT * FROM {catalog}.movielens.{table_name}")
    
    # Add a new column and write back
    new_data = current.withColumn("last_updated", expr("current_timestamp()"))
    new_data.writeTo(f"{catalog}.movielens.{table_name}").append()
    
    # Show new schema
    print("New schema:")
    spark.sql(f"DESCRIBE TABLE {catalog}.movielens.{table_name}").show(truncate=False)

def main():
    """Run Iceberg demos"""
    # Verify tables exist
    tables = spark.sql("SHOW TABLES IN local.movielens").collect()
    if not tables:
        print("No tables found. Please run create_tables.py first.")
        return
    
    # Choose a table for demos
    table_name = "gold_movie_analytics"
    
    # Run demos
    show_table_history(table_name)
    
    # Add a new snapshot for time travel demo
    current = spark.sql(f"SELECT * FROM local.movielens.{table_name}")
    current.writeTo(f"local.movielens.{table_name}").append()
    
    time_travel_demo(table_name)
    schema_evolution_demo(table_name)

if __name__ == "__main__":
    main()
'''
    
    with open("iceberg/iceberg_demo.py", "w") as f:
        f.write(script_content)
    
    logger.info("Created Iceberg demo script")

def main():
    """Set up Iceberg environment and create necessary scripts"""
    try:
        logger.info("Setting up Iceberg environment")
        
        # Create directories
        os.makedirs(WAREHOUSE_DIR, exist_ok=True)
        
        # Download required JARs
        if download_iceberg_jars():
            logger.info("Successfully downloaded Iceberg JARs")
        else:
            logger.error("Failed to download some Iceberg JARs")
            return
        
        # Create scripts
        create_spark_session_script()
        create_iceberg_tables_script()
        create_iceberg_demo_script()
        
        logger.info("Iceberg setup complete. Use the following scripts:")
        logger.info("1. iceberg/spark_session.py - Utility to create Spark session with Iceberg")
        logger.info("2. iceberg/create_tables.py - Create Iceberg tables from Parquet files")
        logger.info("3. iceberg/iceberg_demo.py - Demonstrate Iceberg features")
        
    except Exception as e:
        logger.error(f"Error setting up Iceberg: {e}")

if __name__ == "__main__":
    main()
