import os
import requests
import zipfile
import io
import boto3
import logging
import botocore

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# S3 bucket name
BUCKET_NAME = "movielens-dev-datalake"

# MovieLens 20M dataset URL
MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-20m.zip"

def download_and_extract_movielens():
    """Download and extract MovieLens 20M dataset"""
    local_path = "raw/ml-20m"
    
    # Check if the raw directory exists, if not create it
    if not os.path.exists("raw"):
        os.makedirs("raw")
    
    # Check if dataset already exists
    if os.path.exists(local_path):
        logger.info(f"Dataset already exists at {local_path}")
        return local_path
    
    logger.info(f"Downloading MovieLens 20M dataset from {MOVIELENS_URL}")
    
    # Download the dataset
    response = requests.get(MOVIELENS_URL)
    if response.status_code != 200:
        raise Exception(f"Failed to download dataset: {response.status_code}")
    
    # Extract the dataset
    logger.info("Extracting dataset...")
    z = zipfile.ZipFile(io.BytesIO(response.content))
    z.extractall("raw")
    
    logger.info(f"Dataset extracted to {local_path}")
    return local_path

def upload_to_s3(local_path):
    """Upload the dataset to S3"""
    logger.info(f"Uploading dataset to S3 bucket: {BUCKET_NAME}")
    
    try:
        s3 = boto3.client('s3')
        
        # Test S3 connection by listing buckets
        s3.list_buckets()
        
        # Walk through the directory and upload all files
        for root, _, files in os.walk(local_path):
            for file in files:
                local_file = os.path.join(root, file)
                
                # Determine the S3 key (path in S3)
                s3_key = os.path.join("bronze/movielens", local_file[len("raw/"):])
                
                logger.info(f"Uploading {local_file} to s3://{BUCKET_NAME}/{s3_key}")
                s3.upload_file(local_file, BUCKET_NAME, s3_key)
        
        logger.info("Upload completed")
        return True
    except botocore.exceptions.NoCredentialsError:
        logger.warning("AWS credentials not found. Skipping S3 upload.")
        return False
    except botocore.exceptions.ClientError as e:
        logger.error(f"AWS client error: {e}")
        return False
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        return False

if __name__ == "__main__":
    try:
        local_path = download_and_extract_movielens()
        s3_upload_success = upload_to_s3(local_path)
        
        if s3_upload_success:
            logger.info("MovieLens dataset processing and upload completed successfully")
        else:
            logger.info("MovieLens dataset downloaded locally, but S3 upload was skipped or failed")
            logger.info(f"You can find the dataset at: {os.path.abspath(local_path)}")
    except Exception as e:
        logger.error(f"Error processing MovieLens dataset: {e}")
