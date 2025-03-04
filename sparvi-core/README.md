# Sparvi Core

Sparvi Core is a Python library for data profiling and validation in modern data warehouses.
Like a hawk keeping watch over your data, Sparvi monitors data pipelines, detects anomalies,
tracks schema changes, and ensures data integrity with sharp precision.

## Installation

```bash
# Basic installation
pip install sparvi-core

# With support for Snowflake
pip install sparvi-core[snowflake]

# With support for PostgreSQL
pip install sparvi-core[postgres]

# With all extras
pip install sparvi-core[snowflake,postgres]
```

## Quick Start

### Command Line Interface

Profile a table:

```bash
# Basic profiling
sparvi profile "duckdb:///path/to/database.duckdb" employees

# Save the profile to a file
sparvi profile "postgresql://user:pass@localhost/mydatabase" customers --output profile.json

# Compare with a previous profile
sparvi profile "snowflake://user:pass@account/database/schema?warehouse=wh" orders --compare previous_profile.json
```

Validate a table:

```bash
# Generate and run default validations
sparvi validate "duckdb:///path/to/database.duckdb" employees --generate-defaults

# Save the default rules to a YAML file
sparvi validate "duckdb:///path/to/database.duckdb" employees --generate-defaults --save-defaults rules.yaml

# Run validations from a file
sparvi validate "postgresql://user:pass@localhost/mydatabase" customers --rules rules.yaml

# Save validation results to a file
sparvi validate "snowflake://user:pass@account/database/schema?warehouse=wh" orders --rules rules.yaml --output results.json
```

### Python API

Profile a table:

```python
from sparvi.profiler.profile_engine import profile_table

# Run a profile
profile = profile_table("duckdb:///path/to/database.duckdb", "employees")

# Check completeness
for column, stats in profile["completeness"].items():
    print(f"{column}: {stats['null_percentage']}% null, {stats['distinct_percentage']}% distinct")

# Check for anomalies
for anomaly in profile.get("anomalies", []):
    print(f"Anomaly: {anomaly['description']}")

# Check for schema shifts
for shift in profile.get("schema_shifts", []):
    print(f"Schema shift: {shift['description']}")
```

Validate a table:

```python
from sparvi.validations.validator import run_validations, load_rules_from_file
from sparvi.validations.default_validations import get_default_validations

# Generate default validation rules
rules = get_default_validations("duckdb:///path/to/database.duckdb", "employees")

# Run the validations
results = run_validations("duckdb:///path/to/database.duckdb", rules)

# Check results
for result in results:
    status = "PASS" if result["is_valid"] else "FAIL"
    print(f"{result['rule_name']}: {status}")
    if not result["is_valid"]:
        print(f"  Expected: {result['expected_value']}, Actual: {result['actual_value']}")
```

## Features

Sparvi Core provides the following features:

### Data Profiling

- **Automated Data Profiling**: Compute essential quality metrics (null rates, duplicates, outliers) to understand your data's health at a glance
- **Schema Analysis**: Detect column types, relationships, and constraints
- **Distribution Analysis**: Understand the distribution of values in your data
- **Historical Comparisons**: Compare current profiles with previous runs to detect changes
- **Anomaly Detection**: Automatically detect anomalies in your data

### Data Validation

- **Custom Validation Rules**: Define and run your own validation rules
- **SQL-Based Rules**: Use SQL to define validation queries
- **Default Validation Rules**: Automatically generate sensible validation rules based on your data
- **Validation Results**: Get detailed information about validation failures

## License

Apache License 2.0