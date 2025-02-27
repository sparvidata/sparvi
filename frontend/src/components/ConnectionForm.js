import React, { useState, useEffect } from 'react';
import { fetchTables } from '../api';

function ConnectionForm({ initialConnection, initialTable, onSubmit }) {
  const [connectionString, setConnectionString] = useState(initialConnection || '');
  const [tableName, setTableName] = useState(initialTable || '');
  const [showForm, setShowForm] = useState(false);
  const [tables, setTables] = useState([]);
  const [loadingTables, setLoadingTables] = useState(false);
  const [tableError, setTableError] = useState(null);

  const handleSubmit = (e) => {
    e.preventDefault();
    onSubmit(connectionString, tableName);
    setShowForm(false);
  };

  const handleConnectionChange = async (e) => {
    const newConnection = e.target.value;
    setConnectionString(newConnection);

    if (newConnection) {
      await loadTables(newConnection);
    } else {
      setTables([]);
    }
  };

  const loadTables = async (connString) => {
    try {
      setLoadingTables(true);
      setTableError(null);
      const token = localStorage.getItem("token");

      if (token && connString) {
        const result = await fetchTables(token, connString);
        setTables(result.tables || []);

        // If tables are loaded and we don't have a table selected yet,
        // select the first one by default
        if (result.tables && result.tables.length > 0 && !tableName) {
          setTableName(result.tables[0]);
        }
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
  }, [showForm]);

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
                onChange={handleConnectionChange}
                placeholder="e.g., duckdb:///path/to/database.duckdb"
                required
              />
              <div className="form-text">
                Supported formats: duckdb:///, postgresql://, snowflake://, etc.
              </div>
            </div>

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