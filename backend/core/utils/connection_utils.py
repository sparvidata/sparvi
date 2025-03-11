import os
import re
import urllib.parse


def get_snowflake_connection_from_env(prefix="SNOWFLAKE"):
    """
    Create a Snowflake connection string from environment variables

    Args:
        prefix: Prefix for the environment variables (default: SNOWFLAKE)

    Returns:
        str: Snowflake connection string or None if required variables are missing
    """
    account = os.getenv(f"{prefix}_ACCOUNT")
    user = os.getenv(f"{prefix}_USER")
    password = os.getenv(f"{prefix}_PASSWORD")
    warehouse = os.getenv(f"{prefix}_WAREHOUSE")
    database = os.getenv(f"{prefix}_DATABASE")
    schema = os.getenv(f"{prefix}_SCHEMA", "PUBLIC")

    # Check for required variables
    if not all([account, user, password, warehouse, database]):
        return None

    # URL encode password to handle special characters
    encoded_password = urllib.parse.quote_plus(password)

    # Build connection string
    connection_string = f"snowflake://{user}:{encoded_password}@{account}/{database}/{schema}?warehouse={warehouse}"

    return connection_string


def detect_connection_type(connection_string):
    """
    Detect the type of database from a connection string

    Args:
        connection_string: The connection string to check

    Returns:
        str: Database type (snowflake, duckdb, postgresql, or unknown)
    """
    if connection_string.startswith("snowflake://"):
        return "snowflake"
    elif connection_string.startswith("duckdb://"):
        return "duckdb"
    elif connection_string.startswith("postgresql://"):
        return "postgresql"
    else:
        return "unknown"


def resolve_connection_string(connection_string):
    """
    Resolve any environment variable references in the connection string

    Args:
        connection_string: The connection string to resolve

    Returns:
        str: Resolved connection string
    """
    # Check if this is an environment variable reference
    if connection_string.endswith("_CONNECTION"):
        # Extract prefix from reference (e.g., SNOWFLAKE from SNOWFLAKE_CONNECTION)
        prefix = connection_string.split("://")[-1]
        if prefix.endswith("_CONNECTION"):
            prefix = prefix.replace("_CONNECTION", "")

            # Handle Snowflake connections
            if connection_string.startswith("snowflake://"):
                resolved = get_snowflake_connection_from_env(prefix)
                if resolved:
                    return resolved

    # Return original if no resolution needed or possible
    return connection_string


def sanitize_connection_string(connection_string):
    """
    Sanitize the connection string to hide passwords

    Args:
        connection_string: The connection string to sanitize

    Returns:
        str: Sanitized connection string with password hidden
    """
    # Replace password in connection string with asterisks
    if "://" in connection_string:
        pattern = r'(://[^:]+:)[^@]+(@)'
        sanitized = re.sub(pattern, r'\1*****\2', connection_string)
        return sanitized
    return connection_string