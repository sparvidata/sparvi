import React, { useState, useEffect } from 'react';
import { fetchConnections, testConnection, createConnection, updateConnection, deleteConnection, setDefaultConnection } from '../api';
import ConnectionForm from './ConnectionForm';

function Connections() {
  const [connections, setConnections] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [editConnection, setEditConnection] = useState(null);
  const [showForm, setShowForm] = useState(false);
  const [testResults, setTestResults] = useState(null);
  const [testLoading, setTestLoading] = useState(false);

  // Load connections on mount
  useEffect(() => {
    loadConnections();
  }, []);

  // Function to load/reload connections
  const loadConnections = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await fetchConnections();
      setConnections(data.connections || []);
    } catch (err) {
      setError('Failed to load connections: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // Handle starting to create a new connection
  const handleNewConnection = () => {
    setEditConnection(null);
    setShowForm(true);
    setTestResults(null);
  };

  // Handle starting to edit a connection
  const handleEditConnection = (connection) => {
    setEditConnection(connection);
    setShowForm(true);
    setTestResults(null);
  };

  // Handle connection form submission (create or update)
  const handleSubmitConnection = async (connectionData) => {
    try {
      setLoading(true);
      setError(null);
      setSuccess(null);

      if (editConnection) {
        // Update existing connection
        await updateConnection(editConnection.id, connectionData);
        setSuccess(`Updated connection "${connectionData.name}"`);
      } else {
        // Create new connection
        await createConnection(connectionData);
        setSuccess(`Created connection "${connectionData.name}"`);
      }

      // Reload connections
      await loadConnections();

      // Close form
      setShowForm(false);
      setEditConnection(null);
    } catch (err) {
      setError('Failed to save connection: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // Handle deleting a connection
  const handleDeleteConnection = async (id, name) => {
    if (!window.confirm(`Are you sure you want to delete the connection "${name}"?`)) {
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setSuccess(null);

      await deleteConnection(id);
      setSuccess(`Deleted connection "${name}"`);

      // Reload connections
      await loadConnections();
    } catch (err) {
      setError('Failed to delete connection: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // Handle setting a connection as default
  const handleSetDefault = async (id, name) => {
    try {
      setLoading(true);
      setError(null);
      setSuccess(null);

      await setDefaultConnection(id);
      setSuccess(`Set "${name}" as default connection`);

      // Reload connections
      await loadConnections();
    } catch (err) {
      setError('Failed to set default connection: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // Handle testing a connection
  const handleTestConnection = async (connectionData) => {
    try {
      setTestLoading(true);
      setTestResults(null);
      setError(null);

      const result = await testConnection(connectionData);
      setTestResults({
        success: true,
        message: result.message || 'Connection successful!',
        details: result.details || {}
      });
    } catch (err) {
      setTestResults({
        success: false,
        message: 'Connection failed: ' + (err.response?.data?.error || err.message)
      });
    } finally {
      setTestLoading(false);
    }
  };

  return (
    <div className="container-fluid mt-3">
      <div className="d-flex justify-content-between align-items-center mb-4">
        <h2>
          <i className="bi bi-database me-2"></i>
          Database Connections
        </h2>
        <button
          className="btn btn-primary"
          onClick={handleNewConnection}
          disabled={showForm}
        >
          <i className="bi bi-plus-circle me-1"></i>
          New Connection
        </button>
      </div>

      {error && (
        <div className="alert alert-danger alert-dismissible fade show" role="alert">
          <i className="bi bi-exclamation-triangle-fill me-2"></i>
          {error}
          <button type="button" className="btn-close" data-bs-dismiss="alert" aria-label="Close" onClick={() => setError(null)}></button>
        </div>
      )}

      {success && (
        <div className="alert alert-success alert-dismissible fade show" role="alert">
          <i className="bi bi-check-circle-fill me-2"></i>
          {success}
          <button type="button" className="btn-close" data-bs-dismiss="alert" aria-label="Close" onClick={() => setSuccess(null)}></button>
        </div>
      )}

      {/* Connection Form */}
      {showForm && (
        <div className="card mb-4 shadow-sm">
          <div className="card-header bg-light">
            <h5 className="mb-0">
              {editConnection ? 'Edit Connection' : 'New Connection'}
            </h5>
          </div>
          <div className="card-body">
            <ConnectionForm
              initialConnection={editConnection}
              onSubmit={handleSubmitConnection}
              onCancel={() => setShowForm(false)}
              onTest={handleTestConnection}
              testResults={testResults}
              testLoading={testLoading}
            />
          </div>
        </div>
      )}

      {/* Connections List */}
      <div className="card shadow-sm">
        <div className="card-header">
          <h5 className="mb-0">Your Connections</h5>
        </div>
        <div className="card-body">
          {loading && !connections.length ? (
            <div className="d-flex justify-content-center my-5">
              <div className="spinner-border text-primary" role="status">
                <span className="visually-hidden">Loading...</span>
              </div>
            </div>
          ) : connections.length === 0 ? (
            <div className="alert alert-info">
              <i className="bi bi-info-circle-fill me-2"></i>
              No database connections found. Click "New Connection" to create one.
            </div>
          ) : (
            <div className="table-responsive">
              <table className="table table-hover">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Type</th>
                    <th>Host/Account</th>
                    <th>Database</th>
                    <th>Default</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {connections.map((conn) => (
                    <tr key={conn.id}>
                      <td>{conn.name}</td>
                      <td>
                        <span className="badge bg-secondary">
                          {conn.connection_type}
                        </span>
                      </td>
                      <td>{conn.connection_details.host || conn.connection_details.account}</td>
                      <td>{conn.connection_details.database}</td>
                      <td>
                        {conn.is_default ? (
                          <span className="badge bg-success">Default</span>
                        ) : (
                          <button
                            className="btn btn-sm btn-outline-primary"
                            onClick={() => handleSetDefault(conn.id, conn.name)}
                            disabled={loading}
                          >
                            Make Default
                          </button>
                        )}
                      </td>
                      <td>
                        <div className="btn-group btn-group-sm">
                          <button
                            className="btn btn-outline-primary"
                            onClick={() => handleEditConnection(conn)}
                            disabled={loading}
                          >
                            <i className="bi bi-pencil"></i>
                          </button>
                          <button
                            className="btn btn-outline-danger"
                            onClick={() => handleDeleteConnection(conn.id, conn.name)}
                            disabled={loading || conn.is_default}
                            title={conn.is_default ? "Cannot delete default connection" : "Delete connection"}
                          >
                            <i className="bi bi-trash"></i>
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default Connections;