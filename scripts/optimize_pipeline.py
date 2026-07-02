import logging
import os
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Config
APP_NAME        = "Week10_Optimization"
API_URL         = "https://jsonplaceholder.typicode.com/users"
COLUMNS_TO_KEEP = ["id", "name", "username", "email", "phone", "website"]
NUM_PARTITIONS  = 4
OUTPUT_PATH     = "data/optimized_users.parquet"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
os.makedirs("data", exist_ok=True)

def create_spark_session(app_name: str) -> SparkSession:
    """Create and return Spark session."""
    try:
        return SparkSession.builder.appName(app_name).getOrCreate()
    except Exception as e:
        raise RuntimeError(f"Spark session failed: {e}")

def fetch_api_data(url: str, columns: list) -> pd.DataFrame:
    """Fetch flat data from API."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return pd.DataFrame(response.json())[columns]
    except requests.RequestException as e:
        raise RuntimeError(f"API call failed: {e}")

def clean_df(spark_df):
    """Clean and cast data types."""
    try:
        return (spark_df
                .dropna()
                .withColumn("id", F.col("id").cast("integer")))
    except Exception as e:
        raise RuntimeError(f"Cleaning failed: {e}")

def optimize_df(spark_df, num_partitions: int):
    """Repartition and cache the DataFrame."""
    try:
        repartitioned = spark_df.repartition(num_partitions)
        repartitioned.cache()
        return repartitioned
    except Exception as e:
        raise RuntimeError(f"Optimization failed: {e}")

def show_explain_plan(spark_df) -> None:
    """Print Spark execution plan."""
    try:
        print("=== EXPLAIN PLAN ===")
        spark_df.explain(mode="simple")
    except Exception as e:
        raise RuntimeError(f"Explain plan failed: {e}")

def save_parquet(spark_df, path: str) -> None:
    """Save optimized DataFrame as parquet."""
    try:
        spark_df.write.mode("overwrite").parquet(path)
    except Exception as e:
        raise RuntimeError(f"Save failed: {e}")

def main():
    logger.info("Starting Week 10 Optimization Pipeline")
    spark      = create_spark_session(APP_NAME)
    pandas_df  = fetch_api_data(API_URL, COLUMNS_TO_KEEP)
    spark_df   = spark.createDataFrame(pandas_df)
    cleaned_df = clean_df(spark_df)
    optimized  = optimize_df(cleaned_df, NUM_PARTITIONS)
    show_explain_plan(optimized)
    save_parquet(optimized, OUTPUT_PATH)
    optimized.show()
    logger.info("Optimization complete")

if __name__ == "__main__":
    main()