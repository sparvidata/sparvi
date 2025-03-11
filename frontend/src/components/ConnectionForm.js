import React, { useState, useEffect } from 'react';
import { directFetchTables } from '../profile-api';

function ConnectionForm({ initialConnection, initialTable, onSubmit }) {
  // State for form visibility
  const [showForm, setShowForm] = useState(false);

  // Connection form fields
  const [connectionString, setConnectionString] = useState(initialConnection || '');
  const [tableName, setTableName] = useState(initialTable || '');

  // Database type selector
  const [databaseType, setDatabaseType] = useState('snowflake');

  // Table loading state
  const [tables, setTables] = useState([]);
  const [loadingTables, setLoadingTables] = useState(false);
  const [tableError, setTableError] = useState(null);

  // Snowflake form section with completely independent state
  // This prevents cross-contamination between UI state and connection string
  const [snowflakeForm, setSnowflakeForm] = useState({
    account: '',
    username: '',
    password: '',
    warehouse: '',
    database: '',
    schema: 'PUBLIC',
    useEnvVars: false,
    envVarPrefix: 'SNOWFLAKE'
  });

  // Initialize database type based on initial connection string
  useEffect(() => {
    if (initialConnection) {
      if (initialConnection.startsWith('snowflake://')) {
        setDatabaseType('snowflake');
      } else if (initialConnection.startsWith('duckdb://')) {
        setDatabaseType('duckdb');
      } else if (initialConnection.startsWith('postgresql://')) {
        setDatabaseType('postgresql');
      } else {
        setDatabaseType('other');
      }
    }
  }, [initialConnection]);

  // Parse connection string into form fields when the form is shown
  useEffect(() => {
    if (showForm && connectionString && connectionString.startsWith('snowflake://')) {
      try {
        // Modified regex to handle more complex URL patterns
        const match = connectionString.match(/snowflake:\/\/([^:]+):([^@]+)@([^\/]+)\/([^\/]+)(?:\/([^\?]+))?(?:\?warehouse=([^&]+))?/);

        if (match) {
          const [, username, password, account, database, schema, warehouse] = match;

          setSnowflakeForm(prev => ({
            ...prev,
            username: decodeURIComponent(username || ''),
            password: decodeURIComponent(password || ''),
            account: account || '',
            database: database || '',
            schema: schema || 'PUBLIC',
            warehouse: warehouse || ''
          }));
        }
      } catch (err) {
        console.error('Error parsing Snowflake connection string:', err);
      }
    }
  }, [showForm, connectionString]);

  // Handle Snowflake form field changes
  const handleSnowflakeFieldChange = (field) => (e) => {
    setSnowflakeForm(prev => ({
      ...prev,
      [field]: e.target.value
    }));
  };

  // Handle checkbox changes for Snowflake form
  const handleSnowflakeCheckboxChange = (field) => (e) => {
    setSnowflakeForm(prev => ({
      ...prev,
      [field]: e.target.checked
    }));
  };

  // Build Snowflake connection string from form fields
  const buildSnowflakeConnectionString = () => {
    const { username, password, account, database, schema, warehouse } = snowflakeForm;

    if (username && password && account && database && warehouse) {
      // Properly encode the username and password for URL
      const encodedUsername = encodeURIComponent(username);
      const encodedPassword = encodeURIComponent(password);

      return `snowflake://${encodedUsername}:${encodedPassword}@${account}/${database}/${schema}?warehouse=${warehouse}`;
    }
    return '';
  };

  // Handle database type change
  const handleDatabaseTypeChange = (e) => {
    const newType = e.target.value;
    setDatabaseType(newType);

    // Reset connection string for different database types
    if (newType === 'snowflake') {
      const newConnString = buildSnowflakeConnectionString();
      if (newConnString) {
        setConnectionString(newConnString);
      } else {
        setConnectionString('');
      }
    } else if (newType === 'duckdb') {
      setConnectionString('duckdb:///path/to/database.duckdb');
    } else if (newType === 'postgresql') {
      setConnectionString('postgresql://username:password@hostname:port/database');
    } else {
      setConnectionString('');
    }
  };

  // Handle connection string manual changes
  const handleConnectionStringChange = (e) => {
    setConnectionString(e.target.value);
  };

  // Load tables for the selected connection
  const loadTables = async (connString) => {
    if (!connString) return;

    try {
      setLoadingTables(true);
      setTableError(null);

      console.log("Loading tables for connection:", connString);
      const result = await directFetchTables(connString);
      console.log("Tables loaded:", result.tables);

      setTables(result.tables || []);

      // Set first table as default if none selected
      if (result.tables && result.tables.length > 0 && !tableName) {
        setTableName(result.tables[0]);
      }
    } catch (error) {
      console.error("Error loading tables:", error);
      setTableError("Failed to load tables from database");
      setTables([]);
    } finally {
      setLoadingTables(false);
    }
  };

  // Load tables when showing the form and we have a connection string
  useEffect(() => {
    if (showForm && connectionString) {
      loadTables(connectionString);
    }
  }, [showForm, connectionString]);

  // Handle form submission
  const handleSubmit = (e) => {
    e.preventDefault();

    // If using Snowflake with fields, build the connection string
    if (databaseType === 'snowflake' && !snowflakeForm.useEnvVars) {
      const newConnString = buildSnowflakeConnectionString();
      if (newConnString) {
        setConnectionString(newConnString);
      } else {
        alert("Please fill in all required Snowflake fields");
        return;
      }
    }

    // If using environment variables, construct a reference string
    if (databaseType === 'snowflake' && snowflakeForm.useEnvVars) {
      setConnectionString(`snowflake://${snowflakeForm.envVarPrefix}_CONNECTION`);
    }

    // Validate
    if (!connectionString) {
      alert("Connection string is required");
      return;
    }

    if (!tableName) {
      alert("Table name is required");
      return;
    }

    // Submit to parent component
    onSubmit(connectionString, tableName);
    setShowForm(false);
  };

  // Sync connection string when Snowflake form changes and not using env vars
  useEffect(() => {
    if (databaseType === 'snowflake' && !snowflakeForm.useEnvVars && showForm) {
      const newConnString = buildSnowflakeConnectionString();
      if (newConnString) {
        setConnectionString(newConnString);
      }
    }
  }, [databaseType, snowflakeForm, showForm]);

  // Render Snowflake form fields
  const renderSnowflakeForm = () => {
    return (
      <>
        <div className="mb-3 form-check">
          <input
            type="checkbox"
            className="form-check-input"
            id="useEnvVars"
            checked={snowflakeForm.useEnvVars}
            onChange={handleSnowflakeCheckboxChange('useEnvVars')}
          />
          <label className="form-check-label" htmlFor="useEnvVars">
            Use environment variables for Snowflake credentials
          </label>
        </div>

        {snowflakeForm.useEnvVars ? (
          <>
            <div className="alert alert-info">
              <i className="bi bi-info-circle-fill me-2"></i>
              Using environment variables for secure credential storage. Make sure the following environment variables are set in your backend:
              <ul className="mt-2 mb-0">
                <li><code>{snowflakeForm.envVarPrefix}_ACCOUNT</code> - Snowflake account identifier</li>
                <li><code>{snowflakeForm.envVarPrefix}_USER</code> - Snowflake username</li>
                <li><code>{snowflakeForm.envVarPrefix}_PASSWORD</code> - Snowflake password</li>
                <li><code>{snowflakeForm.envVarPrefix}_WAREHOUSE</code> - Snowflake warehouse name</li>
                <li><code>{snowflakeForm.envVarPrefix}_DATABASE</code> - Snowflake database name</li>
                <li><code>{snowflakeForm.envVarPrefix}_SCHEMA</code> - Snowflake schema name (optional, defaults to PUBLIC)</li>
              </ul>
            </div>

            <div className="mb-3">
              <label htmlFor="envVarPrefix" className="form-label">Environment Variable Prefix</label>
              <input
                type="text"
                className="form-control"
                id="envVarPrefix"
                value={snowflakeForm.envVarPrefix}
                onChange={handleSnowflakeFieldChange('envVarPrefix')}
                placeholder="SNOWFLAKE"
              />
              <div className="form-text">
                Prefix used for environment variables (e.g., SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER)
              </div>
            </div>

            <div className="mb-3">
              <label className="form-label">Connection String</label>
              <input
                type="text"
                className="form-control"
                value={connectionString}
                onChange={handleConnectionStringChange}
                placeholder="snowflake://PREFIX_CONNECTION"
              />
            </div>
          </>
        ) : (
          <>
            <div className="row">
              <div className="col-md-6 mb-3">
                <label htmlFor="snowflakeAccount" className="form-label">Account*</label>
                <input
                  type="text"
                  className="form-control"
                  id="snowflakeAccount"
                  value={snowflakeForm.account}
                  onChange={handleSnowflakeFieldChange('account')}
                  placeholder="orgname-accountname"
                  required
                />
                <div className="form-text">
                  Your Snowflake account identifier (e.g., xy12345.us-east-1)
                </div>
              </div>

              <div className="col-md-6 mb-3">
                <label htmlFor="snowflakeWarehouse" className="form-label">Warehouse*</label>
                <input
                  type="text"
                  className="form-control"
                  id="snowflakeWarehouse"
                  value={snowflakeForm.warehouse}
                  onChange={handleSnowflakeFieldChange('warehouse')}
                  placeholder="COMPUTE_WH"
                  required
                />
              </div>
            </div>

            <div className="row">
              <div className="col-md-6 mb-3">
                <label htmlFor="snowflakeUsername" className="form-label">Username*</label>
                <input
                  type="text"
                  className="form-control"
                  id="snowflakeUsername"
                  value={snowflakeForm.username}
                  onChange={handleSnowflakeFieldChange('username')}
                  required
                />
              </div>

              <div className="col-md-6 mb-3">
                <label htmlFor="snowflakePassword" className="form-label">Password*</label>
                <input
                  type="password"
                  className="form-control"
                  id="snowflakePassword"
                  value={snowflakeForm.password}
                  onChange={handleSnowflakeFieldChange('password')}
                  required
                />
              </div>
            </div>

            <div className="row">
              <div className="col-md-6 mb-3">
                <label htmlFor="snowflakeDatabase" className="form-label">Database*</label>
                <input
                  type="text"
                  className="form-control"
                  id="snowflakeDatabase"
                  value={snowflakeForm.database}
                  onChange={handleSnowflakeFieldChange('database')}
                  required
                />
              </div>

              <div className="col-md-6 mb-3">
                <label htmlFor="snowflakeSchema" className="form-label">Schema</label>
                <input
                  type="text"
                  className="form-control"
                  id="snowflakeSchema"
                  value={snowflakeForm.schema}
                  onChange={handleSnowflakeFieldChange('schema')}
                  placeholder="PUBLIC"
                />
              </div>
            </div>

            <div className="mb-3">
              <label className="form-label">Generated Connection String</label>
              <input
                type="text"
                className="form-control font-monospace"
                value={connectionString}
                onChange={handleConnectionStringChange}
                placeholder="snowflake://username:password@account/database/schema?warehouse=warehouse"
                readOnly
              />
              <div className="form-text">
                This connection string is generated from the fields above. Special characters in the password are properly encoded.
              </div>
            </div>
          </>
        )}
      </>
    );
  };

  // Render different forms based on database type
  const renderDatabaseForm = () => {
    switch (databaseType) {
      case 'snowflake':
        return renderSnowflakeForm();
      case 'duckdb':
        return (
          <div className="mb-3">
            <label htmlFor="duckdbConnection" className="form-label">DuckDB Connection String</label>
            <input
              type="text"
              className="form-control"
              id="duckdbConnection"
              value={connectionString}
              onChange={handleConnectionStringChange}
              placeholder="duckdb:///path/to/database.duckdb"
              required
            />
            <div className="form-text">
              Path to your DuckDB database file. For in-memory database, use duckdb:///:memory:
            </div>
          </div>
        );
      case 'postgresql':
        return (
          <div className="mb-3">
            <label htmlFor="postgresConnection" className="form-label">PostgreSQL Connection String</label>
            <input
              type="text"
              className="form-control"
              id="postgresConnection"
              value={connectionString}
              onChange={handleConnectionStringChange}
              placeholder="postgresql://username:password@hostname:port/database"
              required
            />
            <div className="form-text">
              Standard PostgreSQL connection string format
            </div>
          </div>
        );
      default:
        return (
          <div className="mb-3">
            <label htmlFor="connectionString" className="form-label">Connection String</label>
            <input
              type="text"
              className="form-control"
              id="connectionString"
              value={connectionString}
              onChange={handleConnectionStringChange}
              placeholder="dialect://username:password@hostname:port/database"
              required
            />
            <div className="form-text">
              SQLAlchemy-compatible connection string for your database
            </div>
          </div>
        );
    }
  };

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header bg-light d-flex justify-content-between align-items-center">
        <h5 className="mb-0">
          <i className="bi bi-database me-2"></i>
          Data Source
        </h5>
        <button
          className="btn btn-sm btn-outline-primary"
          onClick={() => setShowForm(!showForm)}
        >
          {showForm ? 'Hide' : 'Change'} Connection
        </button>
      </div>

      {!showForm ? (
        <div className="card-body">
          <div className="row">
            <div className="col-md-6">
              <p className="mb-1"><strong>Connection:</strong></p>
              <code className="text-break">{connectionString || 'Not set'}</code>
            </div>
            <div className="col-md-6">
              <p className="mb-1"><strong>Table:</strong></p>
              <code>{tableName || 'Not set'}</code>
            </div>
          </div>
        </div>
      ) : (
        <div className="card-body">
          <form onSubmit={handleSubmit}>
            {/* Database type selector */}
            <div className="mb-3">
              <label className="form-label">Database Type</label>
              <select
                className="form-select"
                value={databaseType}
                onChange={handleDatabaseTypeChange}
              >
                <option value="snowflake">Snowflake (Recommended)</option>
                <option value="duckdb">DuckDB</option>
                <option value="postgresql">PostgreSQL</option>
                <option value="other">Other</option>
              </select>
            </div>

            {/* Database-specific form */}
            {renderDatabaseForm()}

            {/* Table selection */}
            <div className="mb-3">
              <label htmlFor="tableName" className="form-label">Table Name</label>
              {loadingTables ? (
                <div className="d-flex align-items-center">
                  <div className="spinner-border spinner-border-sm me-2" role="status">
                    <span className="visually-hidden">Loading tables...</span>
                  </div>
                  <span>Loading available tables...</span>
                </div>
              ) : tables.length > 0 ? (
                <select
                  className="form-select"
                  id="tableName"
                  value={tableName}
                  onChange={(e) => setTableName(e.target.value)}
                  required
                >
                  {tables.map(table => (
                    <option key={table} value={table}>{table}</option>
                  ))}
                </select>
              ) : (
                <>
                  <input
                    type="text"
                    className="form-control"
                    id="tableName"
                    value={tableName}
                    onChange={(e) => setTableName(e.target.value)}
                    placeholder="e.g., employees"
                    required
                  />
                  {tableError && (
                    <div className="form-text text-danger">
                      {tableError}. Please enter table name manually.
                    </div>
                  )}
                </>
              )}
            </div>

            <div className="d-flex justify-content-end">
              <button type="button" className="btn btn-secondary me-2" onClick={() => setShowForm(false)}>
                Cancel
              </button>
              <button type="submit" className="btn btn-primary">
                <i className="bi bi-lightning-charge-fill me-1"></i>
                Connect
              </button>
              {connectionString && !loadingTables && (
                <button
                  type="button"
                  className="btn btn-outline-secondary ms-2"
                  onClick={() => loadTables(connectionString)}
                >
                  <i className="bi bi-arrow-repeat me-1"></i>
                  Refresh Tables
                </button>
              )}
            </div>
          </form>
        </div>
      )}
    </div>
  );
}

export default ConnectionForm;