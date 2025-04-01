import boto3
import os
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# S3 bucket name
S3_BUCKET = "movielens-dev-datalake"

# Local directories to upload
LOCAL_DIRS = [
    "output/medallion/bronze",
    "output/medallion/silver", 
    "output/medallion/gold"
]

def upload_file(file_path, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_path: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_path is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_path
    if object_name is None:
        object_name = file_path

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket, object_name)
        logger.info(f"Uploaded {file_path} to s3://{bucket}/{object_name}")
        return True
    except ClientError as e:
        logger.error(f"Error uploading {file_path}: {e}")
        return False

def upload_directory(local_dir, bucket, prefix=""):
    """Upload a directory to S3 bucket

    :param local_dir: Directory to upload
    :param bucket: S3 bucket
    :param prefix: Prefix to use for S3 objects
    """
    for root, dirs, files in os.walk(local_dir):
        for filename in files:
            # Local file path
            local_path = os.path.join(root, filename)
            
            # Determine S3 path
            relative_path = os.path.relpath(local_path, start=os.path.dirname(local_dir))
            s3_path = os.path.join(prefix, relative_path)
            
            # Upload file
            upload_file(local_path, bucket, s3_path)

def main():
    """Main function to upload medallion layers to S3"""
    logger.info(f"Starting upload to S3 bucket: {S3_BUCKET}")
    
    try:
        # Check if all local directories exist
        for local_dir in LOCAL_DIRS:
            if not os.path.isdir(local_dir):
                logger.error(f"Directory not found: {local_dir}")
                continue
                
            # Upload directory to S3
            logger.info(f"Uploading directory: {local_dir}")
            upload_directory(local_dir, S3_BUCKET)
            
        logger.info("S3 upload completed")
    
    except Exception as e:
        logger.error(f"Error during S3 upload: {e}")

if __name__ == "__main__":
    main()
