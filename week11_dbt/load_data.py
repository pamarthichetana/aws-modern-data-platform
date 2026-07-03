import duckdb
import pandas as pd
import requests

# Fetch data from API
url = "https://jsonplaceholder.typicode.com/users"
response = requests.get(url)
df = pd.DataFrame(response.json())[["id", "name", "username", "email", "phone", "website"]]

# Load into DuckDB
conn = duckdb.connect("week11_dbt/week11.duckdb")
conn.execute("CREATE OR REPLACE TABLE main.users AS SELECT * FROM df")
print(f"✅ Loaded {len(df)} rows into DuckDB")
conn.close()
