from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# ── Task 1: API Pull ──────────────────────────────────────────
def task_api_pull():
    import requests
    import pandas as pd

    url = "https://jsonplaceholder.typicode.com/users"
    columns = ["id", "name", "username", "email", "phone", "website"]

    response = requests.get(url, timeout=10)
    response.raise_for_status()
    df = pd.DataFrame(response.json())[columns]

    df.to_csv("/tmp/users.csv", index=False)
    logger.info(f"API pull complete. Rows: {len(df)}")

# ── Task 2: Upload to S3 ──────────────────────────────────────
def task_upload_s3():
    # Simulated — replace with real boto3 when AWS is configured
    import os
    file_exists = os.path.exists("/tmp/users.csv")
    if not file_exists:
        raise FileNotFoundError("users.csv not found. API pull may have failed.")
    logger.info("S3 upload simulated. File ready at /tmp/users.csv")

# ── Task 3: Trigger Spark ─────────────────────────────────────
def task_trigger_spark():
    import pandas as pd
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder \
        .appName("Week14_Spark") \
        .getOrCreate()

    pandas_df = pd.read_csv("/tmp/users.csv")
    spark_df = spark.createDataFrame(pandas_df)

    cleaned_df = (spark_df
                  .dropna()
                  .withColumn("id", F.col("id").cast("integer")))

    cleaned_df.repartition(2).write.mode("overwrite").parquet("/tmp/users.parquet")
    logger.info(f"Spark job complete. Rows: {cleaned_df.count()}")
    spark.stop()

# ── Task 4: Snowflake Load ────────────────────────────────────
def task_snowflake_load():
    # Simulated — replace with real snowflake-connector when configured
    logger.info("Snowflake load simulated. Data ready from /tmp/users.parquet")

# ── Task 5: Run dbt ───────────────────────────────────────────
def task_run_dbt():
    import subprocess
    result = subprocess.run(
        ["python3", "-c", "print('dbt run simulated - stg_users model')"],
        capture_output=True,
        text=True
    )
    logger.info(result.stdout)

# ── DAG Definition ────────────────────────────────────────────
with DAG(
    dag_id="week14_enterprise_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    api_pull = PythonOperator(
        task_id="api_pull",
        python_callable=task_api_pull
    )

    upload_s3 = PythonOperator(
        task_id="upload_s3",
        python_callable=task_upload_s3
    )

    trigger_spark = PythonOperator(
        task_id="trigger_spark",
        python_callable=task_trigger_spark
    )

    snowflake_load = PythonOperator(
        task_id="snowflake_load",
        python_callable=task_snowflake_load
    )

    run_dbt = PythonOperator(
        task_id="run_dbt",
        python_callable=task_run_dbt
    )

    # Pipeline order
    api_pull >> upload_s3 >> trigger_spark >> snowflake_load >> run_dbt
