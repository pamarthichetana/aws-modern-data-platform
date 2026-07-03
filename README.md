# Automated Data Pipeline
A production-grade ETL pipeline that extracts data from REST APIs, transforms it using Apache Spark, and loads it into Snowflake data warehouse with automated orchestration and data quality validation.
## What This Project Does
This pipeline automatically collects data from a REST API, cleans and transforms it using distributed processing, stores it efficiently in AWS S3, loads it into Snowflake for analytics, and validates data quality using dbt. The entire workflow runs automatically every day through Apache Airflow orchestration.
## Technologies Used
- **Python** - Data extraction and scripting
- **Apache Spark (PySpark)** - Distributed data transformation
- **AWS S3** - Cloud storage
- **Snowflake** - Cloud data warehouse
- **dbt** - Data transformation and testing
- **Apache Airflow** - Workflow orchestration
- **Parquet** - Optimized file format
## Architecture
REST API → Python → Apache Spark → AWS S3 → Snowflake → dbt → Analytics ↓ Apache Airflow (Orchestration)
The pipeline consists of five automated stages:
1. Extract data from REST API
2. Transform data using PySpark
3. Store as compressed Parquet in S3
4. Load into Snowflake data warehouse
5. Run dbt models and quality tests
## Key Features
- Fully automated daily execution
- 90% storage reduction through Parquet compression
- 10x faster query performance vs CSV
- Automated data quality testing
- Error handling and logging
- Modular, production-ready code
## Installation
**Prerequisites:**
- Python 3.9+
- AWS Account
- Snowflake Account
- Apache Airflow 2.8+
- Apache Spark 3.5+
**Setup:**

```bash
#Clone repository
git clone https://github.com/yourusername/automated-data-pipeline.git
cd automated-data-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure credentials
cp config/credentials.yml.example config/credentials.yml
# Edit credentials.yml with your AWS and Snowflake details

# Initialize Airflow
airflow db init
airflow users create --username admin --password admin --role Admin --email admin@example.com

# Start Airflow
airflow webserver --port 8080 &
airflow scheduler &

How to Run
Run the complete pipeline:
bash
airflow dags trigger etl_pipeline_dag

Run individual components:
bash
# API ingestion
python scripts/api_ingestion.py

# Spark transformation
spark-submit scripts/spark_transformation.py

# Snowflake load
python scripts/snowflake_loader.py

# dbt models
cd dbt_project && dbt run && dbt test

Project Structure
automated-data-pipeline/
├── dags/
│   └── etl_pipeline_dag.py          # Airflow DAG
├── scripts/
│   ├── api_ingestion.py             # API extraction
│   ├── spark_transformation.py      # Data transformation
│   ├── s3_upload.py                 # S3 upload
│   └── snowflake_loader.py          # Snowflake loading
├── dbt_project/
│   ├── models/
│   │   ├── staging/                 # Staging models
│   │   └── marts/                   # Fact and dimension tables
│   └── schema.yml                   # dbt tests
├── config/
│   └── credentials.yml              # Configuration file
├── requirements.txt
└── README.md

Performance Results

Storage Optimization:
CSV: 500 KB
Parquet: 50 KB
Reduction: 90%

Query Performance:
Full scan: 8.75x faster
Column select: 10.25x faster
Technical Highlights

Data Processing:
Distributed processing with 4 Spark partitions
In-memory DataFrame caching
Optimized execution plans

Data Quality:
Automated dbt tests
Source freshness checks
Schema validation

Production Features:
Comprehensive error handling
Timestamp and row count logging
Modular function design
Star schema data modeling

Challenges Solved
Java Version Compatibility: Resolved PySpark 4.x and Java 25 incompatibility by implementing pyarrow-based Parquet writer as a workaround for the removed Security Manager API.

Incremental Loading: Implemented dbt incremental models with timestamp filtering to avoid full table reloads and improve efficiency.

Future Enhancements
Add monitoring and alerting
Implement CI/CD pipeline
Add data lineage tracking
Integrate real-time streaming with Kafka
Build business intelligence dashboard

Contact
Chetana Pamarthi
Email: pamarthichetana@gmail.com
LinkedIn: www.linkedin.com/in/chetana-pamarthi-056677345
GitHub: https://github.com/pamarthichetana





