import logging
import os
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

APP_NAME        = "Week9_DataEngineering"
API_URL         = "https://jsonplaceholder.typicode.com/users"
COLUMNS_TO_KEEP = ["id", "name", "username", "email", "phone", "website"]
NUM_PARTITIONS  = 4
OUTPUT_PATH     = "data/users_spark.parquet"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

os.makedirs("data", exist_ok=True)

def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def fetch_api_data(url, columns):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return pd.DataFrame(response.json())[columns]

def clean_df(spark_df):
    return spark_df.dropna().withColumn("id", F.col("id").cast("integer"))

def optimize_df(spark_df, num_partitions):
    repartitioned = spark_df.repartition(num_partitions)
    repartitioned.cache()
    return repartitioned

def main():
    logger.info("Starting Week 9 Spark Pipeline")
    spark      = create_spark_session(APP_NAME)
    pandas_df  = fetch_api_data(API_URL, COLUMNS_TO_KEEP)
    spark_df   = spark.createDataFrame(pandas_df)
    cleaned_df = clean_df(spark_df)
    optimized  = optimize_df(cleaned_df, NUM_PARTITIONS)
    optimized.explain(mode="simple")
    optimized.write.mode("overwrite").parquet(OUTPUT_PATH)
    optimized.show()
    logger.info("Pipeline complete")

if __name__ == "__main__":
    main()