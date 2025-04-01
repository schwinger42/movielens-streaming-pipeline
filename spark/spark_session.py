
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
    print("\nAvailable Spark SQL functions:")
    print(", ".join(sorted([f for f in dir(spark.sql.functions) if not f.startswith("_")])))
    
    spark.stop()
