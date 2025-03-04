import React, { useState } from 'react';
import axios from 'axios';
import { supabase } from '../lib/supabase';

function TestConnection() {
  const [connectionString, setConnectionString] = useState("duckdb:///C:/Users/mhard/PycharmProjects/sparvidata/backend/my_database.duckdb");
  const [tableName, setTableName] = useState("employees");
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const [tokenInfo, setTokenInfo] = useState(null);

  // Function to check token info
  const checkToken = async () => {
    try {
      const { data } = await supabase.auth.getSession();
      const session = data.session;

      if (!session) {
        setTokenInfo("No session found");
        return;
      }

      // Get token expiry info
      const expiresAt = new Date(session.expires_at * 1000);
      const now = new Date();
      const timeLeft = expiresAt - now;
      const minutesLeft = Math.floor(timeLeft / (1000 * 60));

      setTokenInfo(`Token expires in ${minutesLeft} minutes (at ${expiresAt.toLocaleString()})`);
    } catch (err) {
      setTokenInfo(`Error getting token: ${err.message}`);
    }
  };

  // Function to manually send a request with axios
  const handleTest = async () => {
    setLoading(true);
    setResult(null);
    setError(null);

    try {
      // First get the token
      const { data } = await supabase.auth.getSession();
      const token = data.session?.access_token;

      if (!token) {
        setError("No authentication token available");
        setLoading(false);
        return;
      }

      console.log("Got token, sending request");

      // Make request directly with axios
      const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:5000';
      const response = await axios.get(`${API_BASE_URL}/api/profile`, {
        params: {
          connection_string: connectionString,
          table: tableName
        },
        headers: {
          Authorization: `Bearer ${token}`
        }
      });

      console.log("Got response:", response.data);
      setResult(response.data);
    } catch (err) {
      console.error("Request failed:", err);
      let errorMessage = err.message;

      if (err.response) {
        errorMessage = `${err.response.status} ${err.response.statusText}: ${JSON.stringify(err.response.data)}`;
      }

      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  const handleRefreshToken = async () => {
    try {
      const { data, error } = await supabase.auth.refreshSession();

      if (error) {
        setError(`Token refresh failed: ${error.message}`);
      } else if (data.session) {
        setTokenInfo(`Token refreshed, expires at ${new Date(data.session.expires_at * 1000).toLocaleString()}`);
      } else {
        setError("Token refresh returned no session");
      }
    } catch (err) {
      setError(`Token refresh error: ${err.message}`);
    }
  };

  const handleSignOut = async () => {
    await supabase.auth.signOut();
    setTokenInfo("Signed out");
  };

  return (
    <div className="container mt-5">
      <div className="card mb-4">
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

          <div className="d-flex gap-2 mb-3">
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

            <button
              className="btn btn-info"
              onClick={checkToken}
            >
              Check Token
            </button>

            <button
              className="btn btn-warning"
              onClick={handleRefreshToken}
            >
              Refresh Token
            </button>

            <button
              className="btn btn-danger"
              onClick={handleSignOut}
            >
              Sign Out
            </button>
          </div>

          {error && (
            <div className="alert alert-danger mt-3">
              <h6>Error:</h6>
              <p>{error}</p>
            </div>
          )}

          {tokenInfo && (
            <div className="alert alert-info mt-3">
              <h6>Token Info:</h6>
              <p>{tokenInfo}</p>
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