import requests
import pandas as pd

# API URL
url = "https://jsonplaceholder.typicode.com/users"

# Get data from API
response = requests.get(url)

# Convert JSON to Python object
data = response.json()

# Convert Python object to DataFrame
df = pd.DataFrame(data)

# Save DataFrame as CSV
df.to_csv("../data/users.csv", index=False)

print("Data saved successfully!")
print(df.head())