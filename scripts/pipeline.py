import requests
import pandas as pd
import boto3
import os
import logging
from botocore.exceptions import ClientError

# Configuration
BUCKET_NAME = "chetana-data-pipeline"
RAW_DATA_PREFIX = "raw/"
DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract():
    """Pull data from API."""
    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url)
    return response.json()


def transform(data):
    """Clean and transform raw data."""
    df = pd.DataFrame(data)
    df = df[['id', 'name', 'username', 'email', 'phone', 'website']]
    df = df.rename(columns={
        'id':       'user_id',
        'name':     'full_name',
        'username': 'user_name',
        'email':    'email_address',
        'phone':    'phone_number',
        'website':  'website_url'
    })
    df['email_domain'] = df['email_address'].apply(lambda x: x.split('@')[1])
    return df


def load_local(df):
    """Save data locally as CSV and Parquet."""
    df.to_csv(os.path.join(DATA_PATH, 'users.csv'), index=False)
    df.to_parquet(os.path.join(DATA_PATH, 'users.parquet'), index=False)


def upload_to_s3(s3_client):
    """Upload local files to S3."""
    for filename in ['users.csv', 'users.parquet']:
        local_path = os.path.join(DATA_PATH, filename)
        s3_key = f"{RAW_DATA_PREFIX}{filename}"
        try:
            s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
            logger.info(f"Uploaded {filename} → s3://{BUCKET_NAME}/{s3_key}")
        except ClientError as e:
            logger.error(f"Failed to upload {filename}: {e}")
            raise


def main():
    data = extract()
    df = transform(data)
    load_local(df)
    s3_client = boto3.client('s3')
    upload_to_s3(s3_client)


if __name__ == "__main__":
    main()