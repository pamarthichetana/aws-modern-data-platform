from datetime import datetime

# Simulate what a DAG does - for learning purposes
def say_hello():
    print("Hello from Airflow!")
    print(f"This would run daily at midnight")
    print(f"Schedule: @daily = every 24 hours")
    print(f"Ran at: {datetime.now()}")

# Run it
say_hello()
