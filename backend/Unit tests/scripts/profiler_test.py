import json
import subprocess
import duckdb
import pandas as pd

# 1️⃣ Setup: Create a persistent DuckDB database
DB_PATH = r"/my_database.duckdb"
conn = duckdb.connect(DB_PATH)

# Create and populate the employees table
data = {
    "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack"],
    "age": [25, 30, 35, None, 45, 50, 55, 60, None, 70],
    "salary": [50000, 60000, None, 80000, 90000, 100000, 110000, 120000, None, 140000],
    "department": ["HR", "IT", "Finance", "IT", "HR", "Finance", "HR", None, "IT", "Finance"]
}
df = pd.DataFrame(data)

conn.execute("CREATE OR REPLACE TABLE employees AS SELECT * FROM df")
conn.close()  # ✅ Close the connection before running the profiler

# 2️⃣ Run the profiler
PROFILER_PATH = r"/backend/data_quality_engine\src\profiler\profiler.py"
DB_CONNECTION = f"duckdb:///{DB_PATH}"
TABLE_NAME = "employees"

print("🔍 Running profiler on employees table...")

result = subprocess.run(
    ["python", PROFILER_PATH, DB_CONNECTION, TABLE_NAME],
    capture_output=True,
    text=True
)

# 3️⃣ Print profiler output
print("📄 Stdout:")
print(result.stdout)

print("⚠️ Stderr:")
print(result.stderr)

if result.stdout.strip():
    try:
        profile_results = json.loads(result.stdout)
        print("✅ Parsed JSON output:")
        print(json.dumps(profile_results, indent=4))
    except json.JSONDecodeError:
        print("❌ Failed to parse JSON output. Check raw output above.")
else:
    print("❌ No output received from profiler.")
