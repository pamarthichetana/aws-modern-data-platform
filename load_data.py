import duckdb
import pandas as pd
import requests

url = "https://jsonplaceholder.typicode.com/users"
response = requests.get(url)
df = pd.DataFrame(response.json())[["id", "name", "username", "email", "phone", "website"]]

conn = duckdb.connect("week11_dbt/dev.duckdb")
conn.execute("CREATE OR REPLACE TABLE main.users AS SELECT * FROM df")
print(f"✅ Loaded {len(df)} rows into DuckDB")
print(df.head(3))
conn.close()
