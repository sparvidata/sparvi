import React, { useState } from 'react';
import { directFetchProfile } from '../profile-api';

function TestConnection() {
  const [connectionString, setConnectionString] = useState("duckdb:///C:/Users/mhard/PycharmProjects/sparvidata/backend/my_database.duckdb");
  const [tableName, setTableName] = useState("employees");
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleTest = async () => {
    setLoading(true);
    setResult(null);
    setError(null);

    console.log("Test Connection button clicked");
    console.log("Connection String:", connectionString);
    console.log("Table Name:", tableName);

    try {
      const data = await directFetchProfile(connectionString, tableName);
      console.log("API call successful:", data);
      setResult(data);
    } catch (err) {
      console.error("API call failed:", err);
      setError(err.message || "Failed to connect to the database");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="container mt-5">
      <div className="card">
        <div className="card-header bg-primary text-white">
          <h5 className="mb-0">Test Database Connection</h5>
        </div>
        <div className="card-body">
          <div className="mb-3">
            <label className="form-label">Connection String:</label>
            <input
              type="text"
              className="form-control"
              value={connectionString}
              onChange={(e) => setConnectionString(e.target.value)}
            />
          </div>

          <div className="mb-3">
            <label className="form-label">Table Name:</label>
            <input
              type="text"
              className="form-control"
              value={tableName}
              onChange={(e) => setTableName(e.target.value)}
            />
          </div>

          <button
            className="btn btn-primary"
            onClick={handleTest}
            disabled={loading}
          >
            {loading ? (
              <>
                <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                Testing...
              </>
            ) : (
              "Test Connection"
            )}
          </button>

          {error && (
            <div className="alert alert-danger mt-3">
              <h6>Error:</h6>
              <p>{error}</p>
            </div>
          )}

          {result && (
            <div className="alert alert-success mt-3">
              <h6>Connection Successful!</h6>
              <p>Table: {result.table}</p>
              <p>Row Count: {result.row_count}</p>
              <p>Timestamp: {result.timestamp}</p>
              <details>
                <summary>Full Response (click to expand)</summary>
                <pre className="mt-2" style={{maxHeight: "200px", overflow: "auto"}}>
                  {JSON.stringify(result, null, 2)}
                </pre>
              </details>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default TestConnection;