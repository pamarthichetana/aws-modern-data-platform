import boto3
import os
import logging
from botocore.exceptions import ClientError

# Configuration
BUCKET_NAME = "chetana-data-pipeline"
RAW_DATA_PREFIX = "raw/"
LOCAL_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data')
CSV_FILENAME = "users.csv"
PARQUET_FILENAME = "users.parquet"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def upload_file(s3_client, local_path, bucket, s3_key):
    """Upload a single file to S3."""
    try:
        s3_client.upload_file(local_path, bucket, s3_key)
        logger.info(f"Uploaded {os.path.basename(local_path)} → s3://{bucket}/{s3_key}")
    except ClientError as e:
        logger.error(f"Failed to upload {local_path}: {e}")
        raise


def upload_pipeline_outputs(s3_client):
    """Upload pipeline output files to S3 raw layer."""
    for filename in [CSV_FILENAME, PARQUET_FILENAME]:
        local_path = os.path.join(LOCAL_DATA_PATH, filename)
        s3_key = f"{RAW_DATA_PREFIX}{filename}"
        upload_file(s3_client, local_path, BUCKET_NAME, s3_key)


def main():
    s3_client = boto3.client('s3')
    upload_pipeline_outputs(s3_client)


if __name__ == "__main__":
    main()