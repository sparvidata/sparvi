import duckdb
import pandas as pd
import numpy as np

# Create a DuckDB database with sample tables for testing
conn = duckdb.connect("backend/my_database.duckdb")

# Create employees table
employees = pd.DataFrame({
    "id": range(1, 101),
    "name": ["Employee " + str(i) for i in range(1, 101)],
    "department": np.random.choice(["Sales", "Marketing", "Engineering", "HR", "Finance"], 100),
    "salary": np.random.normal(70000, 15000, 100).astype(int),
    "hire_date": pd.date_range(start="2015-01-01", periods=100, freq='W'),
    "email": [f"employee{i}@example.com" for i in range(1, 101)]
})

# Add some nulls and anomalies
employees.loc[5:10, "salary"] = None
employees.loc[15:18, "department"] = None
employees.loc[25, "salary"] = 500000  # outlier
employees.loc[50, "email"] = "badformat"  # incorrect format

conn.execute("CREATE TABLE employees AS SELECT * FROM employees")

# Create orders table
orders = pd.DataFrame({
    "order_id": range(1, 501),
    "customer_id": np.random.randint(1, 50, 500),
    "amount": np.random.uniform(10, 500, 500).round(2),
    "order_date": pd.date_range(start="2023-01-01", periods=500, freq='D'),
    "status": np.random.choice(["Completed", "Processing", "Shipped", "Cancelled"], 500,
                        p=[0.7, 0.1, 0.15, 0.05])
})

conn.execute("CREATE TABLE orders AS SELECT * FROM orders")

print(f"Created sample tables: employees ({len(employees)} rows), orders ({len(orders)} rows)")
conn.close()