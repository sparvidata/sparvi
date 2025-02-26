import React, { useState } from 'react';

function ConnectionForm({ initialConnection, initialTable, onSubmit }) {
  const [connectionString, setConnectionString] = useState(initialConnection || '');
  const [tableName, setTableName] = useState(initialTable || '');
  const [showForm, setShowForm] = useState(false);

  const handleSubmit = (e) => {
    e.preventDefault();
    onSubmit(connectionString, tableName);
    setShowForm(false);
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
            <div className="mb-3">
              <label htmlFor="connectionString" className="form-label">Connection String</label>
              <input
                type="text"
                className="form-control"
                id="connectionString"
                value={connectionString}
                onChange={(e) => setConnectionString(e.target.value)}
                placeholder="e.g., duckdb:///path/to/database.duckdb"
                required
              />
              <div className="form-text">
                Supported formats: duckdb:///, postgresql://, snowflake://, etc.
              </div>
            </div>

            <div className="mb-3">
              <label htmlFor="tableName" className="form-label">Table Name</label>
              <input
                type="text"
                className="form-control"
                id="tableName"
                value={tableName}
                onChange={(e) => setTableName(e.target.value)}
                placeholder="e.g., employees"
                required
              />
            </div>

            <div className="d-flex justify-content-end">
              <button type="button" className="btn btn-secondary me-2" onClick={() => setShowForm(false)}>
                Cancel
              </button>
              <button type="submit" className="btn btn-primary">
                <i className="bi bi-lightning-charge-fill me-1"></i>
                Connect
              </button>
            </div>
          </form>
        </div>
      )}
    </div>
  );
}

export default ConnectionForm;