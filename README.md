# Sparvi

![Sparvi Logo](https://via.placeholder.com/150x50?text=Sparvi)

Sparvi is a data quality and profiling engine designed for modern data warehouses. It monitors data pipelines, detects anomalies, tracks schema changes, and ensures data integrity with sharp precision—like a hawk keeping watch over your data.

## Community and Commercial Editions

Sparvi follows an open core model:

- **Community Edition**: Open source with core functionality for data profiling and basic monitoring
- **Enterprise Edition**: Commercial offering with advanced features for larger organizations (coming soon)

This repository contains the Community Edition of Sparvi.

## Features

### Community Edition
- **🔍 Automated Data Profiling**: Compute essential quality metrics (null rates, duplicates, outliers) to understand your data's health at a glance
- **📊 Basic Monitoring**: Track data quality with anomaly detection
- **🚨 Local Alerts**: Get notifications when data quality issues arise
- **📈 Historical Trends**: View how your data evolves over time
- **🧩 Custom Validations**: Define and run your own validation rules to enforce data quality standards
- **🔄 Schema Change Detection**: Get alerted when table schemas change unexpectedly
- **🔌 Multi-Source Connectivity**: Connect to various databases and data warehouses through SQLAlchemy

### Enterprise Edition (Coming Soon)
- **🔄 Advanced Scheduling**: Set up automated profiling runs with complex schedules
- **🔔 Enterprise Notifications**: Advanced alerting integrations with SLAs and alert routing
- **🔒 Role-Based Access Control**: Granular permissions for teams and organizations
- **📊 Advanced Visualizations**: Enhanced dashboards and customizable reports
- **🤖 ML-Powered Anomaly Detection**: Intelligent anomaly detection using machine learning
- **📱 Mobile Support**: Mobile app for on-the-go monitoring
- **💬 Priority Support**: Enterprise-grade support and SLAs

## Getting Started

### Prerequisites

- Python 3.9+ (backend)
- Node.js 14+ (frontend)
- DuckDB, PostgreSQL, Snowflake, or other SQLAlchemy-compatible database

### Backend Setup

1. Clone the repository and navigate to the backend directory:
   ```bash
   git clone https://github.com/yourusername/sparvi.git
   cd sparvi/backend
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows, use: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create a `.env` file with your configuration:
   ```
   SECRET_KEY=your_secret_key_here
   DEFAULT_CONNECTION_STRING=duckdb:///path/to/your/database.duckdb
   HISTORY_DB_PATH=./history.db
   ALERT_CONFIG_PATH=./alert_config.json
   ```

5. Generate test data (optional):
   ```bash
   python scripts/generate_test_data.py
   ```

6. Run the Flask app:
   ```bash
   python app.py
   ```

The backend API will run on http://localhost:5000.

### Frontend Setup

1. Navigate to the frontend directory:
   ```bash
   cd ../frontend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Create a `.env` file with your configuration:
   ```
   REACT_APP_API_BASE_URL=http://localhost:5000
   ```

4. Run the React app:
   ```bash
   npm start
   ```

The frontend will run on http://localhost:3000.

## Using Sparvi

### First-Time Login

1. Navigate to http://localhost:3000 in your browser
2. Login with the default credentials (for the MVP):
   - Username: `admin`
   - Password: `password123`

### Connecting to a Data Source

1. After logging in, you'll see the main dashboard
2. Click the "Change Connection" button
3. Enter your connection string and table name
4. Click "Connect"

Supported connection string formats:
- DuckDB: `duckdb:///path/to/database.duckdb`
- PostgreSQL: `postgresql://username:password@hostname:port/database`
- Snowflake: `snowflake://username:password@account/database/schema?warehouse=warehouse`

### Exploring Data Profiles

The dashboard provides multiple views of your data profile:

- **Data Overview**: General statistics and completeness information
- **Numeric Statistics**: Detailed metrics for numeric columns
- **Trends & Changes**: Historical data quality metrics over time
- **Anomalies & Alerts**: Detected issues and alerts
- **Validations**: Custom validation rules and results
- **Sample Data**: Preview of your actual data

### Creating Validation Rules

1. Go to the "Validations" tab
2. Fill out the form with:
   - Rule Name: A unique identifier
   - Description: What this rule checks
   - SQL Query: The query that returns a single value
   - Operator: The comparison type (equals, greater than, etc.)
   - Expected Value: What the query result should match
3. Click "Add Rule"

Example validation rule:
- Name: `check_positive_salaries`
- Description: `Ensure all salaries are positive`
- Query: `SELECT COUNT(*) FROM employees WHERE salary <= 0`
- Operator: `equals`
- Expected Value: `0`

### Configuring Alerts

Currently, alert configuration is done via the `alert_config.json` file:

```json
{
  "email": {
    "enabled": true,
    "smtp_server": "smtp.example.com",
    "smtp_port": 587,
    "username": "alerts@yourcompany.com",
    "password": "your_password",
    "from_address": "sparvi@yourcompany.com",
    "recipients": ["data-team@yourcompany.com"]
  },
  "slack": {
    "enabled": true,
    "webhook_url": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
  },
  "alert_thresholds": {
    "row_count_change_pct": 10,
    "null_rate_change_pct": 5,
    "duplicate_rate_threshold": 5,
    "validation_failure_threshold": 1
  }
}
```

## API Reference

### Authentication

```
POST /api/login
```

Request body:
```json
{
  "username": "admin",
  "password": "password123"
}
```

Response:
```json
{
  "token": "your.jwt.token"
}
```

### Profiling

```
GET /api/profile?connection_string=duckdb:///path/to/db.duckdb&table=employees
```

Headers:
```
Authorization: Bearer your.jwt.token
```

### Validation Rules

```
GET /api/validations?table=employees
POST /api/validations?table=employees
DELETE /api/validations?table=employees&rule_name=rule_name
```

### Running Validations

```
POST /api/run-validations
```

Request body:
```json
{
  "table": "employees",
  "connection_string": "duckdb:///path/to/db.duckdb"
}
```

### History and Trends

```
GET /api/history?table=employees&periods=10
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to submit pull requests, report issues, and suggest enhancements.

## License

The Community Edition of Sparvi is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

The Enterprise Edition, when released, will be available under a commercial license.

## Acknowledgments

- Sparvi uses [DuckDB](https://duckdb.org/) for the example implementation
- Built with Flask, SQLAlchemy, React, and Chart.js