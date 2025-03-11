import React, { useState, useEffect } from 'react';

function ConnectionForm({ initialConnection, onSubmit, onCancel, onTest, testResults, testLoading }) {
  // State for form data
  const [formData, setFormData] = useState({
    name: '',
    connection_type: 'snowflake',
    connection_details: {
      account: '',
      username: '',
      password: '',
      warehouse: '',
      database: '',
      schema: 'PUBLIC',
      useEnvVars: false,
      envVarPrefix: 'SNOWFLAKE'
    }
  });

  // Copy initial connection data to form state when editing
  useEffect(() => {
    if (initialConnection) {
      setFormData({
        name: initialConnection.name || '',
        connection_type: initialConnection.connection_type || 'snowflake',
        connection_details: {
          ...initialConnection.connection_details,
          // Don't prefill password for security reasons
          password: ''
        }
      });
    }
  }, [initialConnection]);

  // Handle form field changes
  const handleInputChange = (e) => {
    const { name, value } = e.target;
    setFormData({
      ...formData,
      [name]: value
    });
  };

  // Handle connection detail changes
  const handleDetailChange = (e) => {
    const { name, value } = e.target;
    setFormData({
      ...formData,
      connection_details: {
        ...formData.connection_details,
        [name]: value
      }
    });
  };

  // Handle checkbox changes
  const handleCheckboxChange = (e) => {
    const { name, checked } = e.target;
    setFormData({
      ...formData,
      connection_details: {
        ...formData.connection_details,
        [name]: checked
      }
    });
  };

  // Handle form submission
  const handleSubmit = (e) => {
    e.preventDefault();

    // Validate form
    if (!formData.name) {
      alert('Connection name is required');
      return;
    }

    // For snowflake connections, validate required fields
    if (formData.connection_type === 'snowflake' && !formData.connection_details.useEnvVars) {
      const requiredFields = ['account', 'username', 'warehouse', 'database'];

      // If we're creating a new connection or updating with a new password, require password
      if (!initialConnection || formData.connection_details.password) {
        requiredFields.push('password');
      }

      for (const field of requiredFields) {
        if (!formData.connection_details[field]) {
          alert(`${field.charAt(0).toUpperCase() + field.slice(1)} is required`);
          return;
        }
      }
    }

    // Submit form data
    const dataToSubmit = {
      ...formData,
      connection_details: {
        ...formData.connection_details
      }
    };

    // If updating and no new password provided, remove password field
    if (initialConnection && !dataToSubmit.connection_details.password) {
      delete dataToSubmit.connection_details.password;
    }

    onSubmit(dataToSubmit);
  };

  // Handle test connection
  const handleTestConnection = () => {
    // Don't test if required fields are missing
    if (formData.connection_type === 'snowflake' && !formData.connection_details.useEnvVars) {
      const requiredFields = ['account', 'username', 'password', 'warehouse', 'database'];
      for (const field of requiredFields) {
        if (!formData.connection_details[field]) {
          alert(`${field.charAt(0).toUpperCase() + field.slice(1)} is required for testing`);
          return;
        }
      }
    }

    onTest(formData);
  };

  // Render different forms based on connection type
  const renderConnectionForm = () => {
    switch (formData.connection_type) {
      case 'snowflake':
        return renderSnowflakeForm();
      case 'duckdb':
        return renderDuckDBForm();
      case 'postgresql':
        return renderPostgreSQLForm();
      default:
        return null;
    }
  };

  // Render Snowflake connection form
  const renderSnowflakeForm = () => {
    return (
      <>
        <div className="mb-3 form-check">
          <input
            type="checkbox"
            className="form-check-input"
            id="useEnvVars"
            name="useEnvVars"
            checked={formData.connection_details.useEnvVars}
            onChange={handleCheckboxChange}
          />
          <label className="form-check-label" htmlFor="useEnvVars">
            Use environment variables for Snowflake credentials
          </label>
        </div>

        {formData.connection_details.useEnvVars ? (
          <>
            <div className="alert alert-info">
              <i className="bi bi-info-circle-fill me-2"></i>
              Using environment variables for secure credential storage. Make sure the following environment variables are set in your backend:
              <ul className="mt-2 mb-0">
                <li><code>{formData.connection_details.envVarPrefix}_ACCOUNT</code> - Snowflake account identifier</li>
                <li><code>{formData.connection_details.envVarPrefix}_USER</code> - Snowflake username</li>
                <li><code>{formData.connection_details.envVarPrefix}_PASSWORD</code> - Snowflake password</li>
                <li><code>{formData.connection_details.envVarPrefix}_WAREHOUSE</code> - Snowflake warehouse name</li>
                <li><code>{formData.connection_details.envVarPrefix}_DATABASE</code> - Snowflake database name</li>
                <li><code>{formData.connection_details.envVarPrefix}_SCHEMA</code> - Snowflake schema name (optional, defaults to PUBLIC)</li>
              </ul>
            </div>

            <div className="mb-3">
              <label htmlFor="envVarPrefix" className="form-label">Environment Variable Prefix</label>
              <input
                type="text"
                className="form-control"
                id="envVarPrefix"
                name="envVarPrefix"
                value={formData.connection_details.envVarPrefix}
                onChange={handleDetailChange}
                placeholder="SNOWFLAKE"
              />
              <div className="form-text">
                Prefix used for environment variables (e.g., SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER)
              </div>
            </div>
          </>
        ) : (
          <>
            <div className="row">
              <div className="col-md-6 mb-3">
                <label htmlFor="account" className="form-label">Account*</label>
                <input
                  type="text"
                  className="form-control"
                  id="account"
                  name="account"
                  value={formData.connection_details.account}
                  onChange={handleDetailChange}
                  placeholder="orgname-accountname"
                  required
                />
                <div className="form-text">
                  Your Snowflake account identifier (e.g., xy12345.us-east-1)
                </div>
              </div>

              <div className="col-md-6 mb-3">
                <label htmlFor="warehouse" className="form-label">Warehouse*</label>
                <input
                  type="text"
                  className="form-control"
                  id="warehouse"
                  name="warehouse"
                  value={formData.connection_details.warehouse}
                  onChange={handleDetailChange}
                  placeholder="COMPUTE_WH"
                  required
                />
              </div>
            </div>

            <div className="row">
              <div className="col-md-6 mb-3">
                <label htmlFor="username" className="form-label">Username*</label>
                <input
                  type="text"
                  className="form-control"
                  id="username"
                  name="username"
                  value={formData.connection_details.username}
                  onChange={handleDetailChange}
                  required
                />
              </div>

              <div className="col-md-6 mb-3">
                <label htmlFor="password" className="form-label">
                  {initialConnection ? 'Password (leave blank to keep current)' : 'Password*'}
                </label>
                <input
                  type="password"
                  className="form-control"
                  id="password"
                  name="password"
                  value={formData.connection_details.password}
                  onChange={handleDetailChange}
                  required={!initialConnection}
                />
              </div>
            </div>

            <div className="row">
              <div className="col-md-6 mb-3">
                <label htmlFor="database" className="form-label">Database*</label>
                <input
                  type="text"
                  className="form-control"
                  id="database"
                  name="database"
                  value={formData.connection_details.database}
                  onChange={handleDetailChange}
                  required
                />
              </div>

              <div className="col-md-6 mb-3">
                <label htmlFor="schema" className="form-label">Schema</label>
                <input
                  type="text"
                  className="form-control"
                  id="schema"
                  name="schema"
                  value={formData.connection_details.schema}
                  onChange={handleDetailChange}
                  placeholder="PUBLIC"
                />
              </div>
            </div>
          </>
        )}
      </>
    );
  };

  // Render DuckDB connection form
  const renderDuckDBForm = () => {
    return (
      <div className="mb-3">
        <label htmlFor="path" className="form-label">DuckDB Path*</label>
        <input
          type="text"
          className="form-control"
          id="path"
          name="path"
          value={formData.connection_details.path || ''}
          onChange={handleDetailChange}
          placeholder="/path/to/database.duckdb"
          required
        />
        <div className="form-text">
          Path to your DuckDB database file. For in-memory database, use :memory:
        </div>
      </div>
    );
  };

  // Render PostgreSQL connection form
  const renderPostgreSQLForm = () => {
    return (
      <>
        <div className="row">
          <div className="col-md-6 mb-3">
            <label htmlFor="host" className="form-label">Host*</label>
            <input
              type="text"
              className="form-control"
              id="host"
              name="host"
              value={formData.connection_details.host || ''}
              onChange={handleDetailChange}
              placeholder="localhost"
              required
            />
          </div>

          <div className="col-md-6 mb-3">
            <label htmlFor="port" className="form-label">Port*</label>
            <input
              type="text"
              className="form-control"
              id="port"
              name="port"
              value={formData.connection_details.port || '5432'}
              onChange={handleDetailChange}
              placeholder="5432"
              required
            />
          </div>
        </div>

        <div className="row">
          <div className="col-md-6 mb-3">
            <label htmlFor="username" className="form-label">Username*</label>
            <input
              type="text"
              className="form-control"
              id="username"
              name="username"
              value={formData.connection_details.username || ''}
              onChange={handleDetailChange}
              required
            />
          </div>

          <div className="col-md-6 mb-3">
            <label htmlFor="password" className="form-label">
              {initialConnection ? 'Password (leave blank to keep current)' : 'Password*'}
            </label>
            <input
              type="password"
              className="form-control"
              id="password"
              name="password"
              value={formData.connection_details.password || ''}
              onChange={handleDetailChange}
              required={!initialConnection}
            />
          </div>
        </div>

        <div className="mb-3">
          <label htmlFor="database" className="form-label">Database*</label>
          <input
            type="text"
            className="form-control"
            id="database"
            name="database"
            value={formData.connection_details.database || ''}
            onChange={handleDetailChange}
            required
          />
        </div>
      </>
    );
  };

  return (
    <form onSubmit={handleSubmit}>
      <div className="mb-3">
        <label htmlFor="name" className="form-label">Connection Name*</label>
        <input
          type="text"
          className="form-control"
          id="name"
          name="name"
          value={formData.name}
          onChange={handleInputChange}
          placeholder="My Snowflake Connection"
          required
        />
        <div className="form-text">
          A friendly name to identify this connection
        </div>
      </div>

      <div className="mb-3">
        <label htmlFor="connection_type" className="form-label">Connection Type*</label>
        <select
          className="form-select"
          id="connection_type"
          name="connection_type"
          value={formData.connection_type}
          onChange={handleInputChange}
          required
        >
          <option value="snowflake">Snowflake</option>
          <option value="duckdb">DuckDB</option>
          <option value="postgresql">PostgreSQL</option>
        </select>
      </div>

      {renderConnectionForm()}

      {/* Test Connection Results */}
      {testResults && (
        <div className={`alert ${testResults.success ? 'alert-success' : 'alert-danger'} mt-3`}>
          <i className={`bi ${testResults.success ? 'bi-check-circle-fill' : 'bi-exclamation-triangle-fill'} me-2`}></i>
          <strong>{testResults.message}</strong>
          {testResults.details && Object.keys(testResults.details).length > 0 && (
            <div className="mt-2">
              <strong>Connection Details:</strong>
              <ul className="mb-0">
                {Object.entries(testResults.details).map(([key, value]) => (
                  <li key={key}><strong>{key}:</strong> {value}</li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}

      <div className="d-flex justify-content-between mt-4">
        <button
          type="button"
          className="btn btn-outline-primary"
          onClick={handleTestConnection}
          disabled={testLoading}
        >
          {testLoading ? (
            <>
              <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
              Testing...
            </>
          ) : (
            <>
              <i className="bi bi-lightning-charge me-1"></i>
              Test Connection
            </>
          )}
        </button>

        <div>
          <button
            type="button"
            className="btn btn-secondary me-2"
            onClick={onCancel}
          >
            Cancel
          </button>
          <button
            type="submit"
            className="btn btn-primary"
          >
            {initialConnection ? 'Update Connection' : 'Create Connection'}
          </button>
        </div>
      </div>
    </form>
  );
}

export default ConnectionForm;