import os
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

# Define your connection parameters directly
account = 'gwzxhsi-xr39130'  # from your error message
user = 'mhardman100'  # from your error message
password = '#0zAKUKSmatt29754'  # use your actual password
warehouse = 'COMPUTE_WH'
database = 'SNOWFLAKE_SAMPLE_DATA'
schema = 'TPCH_SF1'

# Create the connection string manually
conn_str = f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"
print(f"Testing connection with string: snowflake://{user}:****@{account}/{database}/{schema}?warehouse={warehouse}")

# Alternative method using URL constructor
# This is often more reliable for handling special characters
engine = create_engine(URL(
    account=account,
    user=user,
    password=password,
    database=database,
    schema=schema,
    warehouse=warehouse
))

try:
    # Test the connection
    with engine.connect() as conn:
        result = conn.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        row = result.fetchone()
        print(f"Connected successfully!")
        print(f"User: {row[0]}, Role: {row[1]}, Database: {row[2]}, Schema: {row[3]}")
except Exception as e:
    print(f"Connection failed with error: {str(e)}")