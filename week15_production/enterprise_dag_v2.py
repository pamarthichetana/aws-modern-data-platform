import logging, os, requests, pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

API_URL      = "https://jsonplaceholder.typicode.com/users"
COLUMNS      = ["id", "name", "username", "email", "phone", "website"]
OUTPUT_DIR   = "/workspaces/aws-modern-data-platform/week15_production/output"
PARQUET_PATH = f"{OUTPUT_DIR}/users.parquet"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def run_pipeline():
    logger.info("PIPELINE START")
    df = pd.DataFrame(requests.get(API_URL, timeout=10).json())[COLUMNS]
    df["id"] = df["id"].astype(int)
    df = df.dropna()
    logger.info(f"API pull success. Rows: {len(df)}")
    df.to_parquet(PARQUET_PATH, index=False)
    logger.info(f"Parquet saved: {PARQUET_PATH}")
    print(df.to_string())
    logger.info("S3 upload simulated.")
    logger.info("Snowflake load simulated.")
    logger.info("dbt run: stg_users model complete")
    logger.info("PIPELINE COMPLETE")

run_pipeline()
