import React, { useState, useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { fetchConnections, fetchTables } from '../api';

function DataSourcePanel({ tableName, onTableChange, onConnectionChange, activeConnection }) {
  const [connections, setConnections] = useState([]);
  const [selectedConnectionId, setSelectedConnectionId] = useState(null);
  const [tables, setTables] = useState([]);
  const [loading, setLoading] = useState(true);
  const [tablesLoading, setTablesLoading] = useState(false);
  const [connectionError, setConnectionError] = useState(null);
  const [tableError, setTableError] = useState(null);

  // Compute selected connection dynamically
  const selectedConnection = useMemo(() => {
    // If there are no connections, check if activeConnection is a string (direct connection string)
    if (!connections.length) {
      // If activeConnection is a string (connection string), return it directly
      if (activeConnection && typeof activeConnection === 'string') {
        return activeConnection;
      }
      // If activeConnection is an object, return it directly
      if (activeConnection && (activeConnection.id || activeConnection.connection_details)) {
        return activeConnection;
      }
      return null;
    }

    // If activeConnection is a string, return it as is
    if (activeConnection && typeof activeConnection === 'string') {
      return activeConnection;
    }

    // If activeConnection is an object with id or connection_details, return it directly
    if (activeConnection && (activeConnection.id || activeConnection.connection_details)) {
      return activeConnection;
    }

    // Otherwise use dropdown selection
    return selectedConnectionId
      ? connections.find(c => c.id === selectedConnectionId)
      : connections.find(c => c.is_default) || connections[0];
  }, [selectedConnectionId, connections, activeConnection]);

  // Compute selected table dynamically
  const selectedTable = useMemo(() => {
    return tableName || tables[0] || '';
  }, [tableName, tables]);

  // Load connections on mount
  useEffect(() => {
    const loadConnections = async () => {
      try {
        setLoading(true);
        setConnectionError(null);
        const data = await fetchConnections();
        setConnections(data.connections || []);

        // Set initial selected connection to activeConnection, default, or first in list
        if (data.connections && data.connections.length > 0) {
          // If we have an activeConnection with an ID, try to find it in the list
          if (activeConnection && activeConnection.id) {
            const matchingConn = data.connections.find(c => c.id === activeConnection.id);
            if (matchingConn) {
              setSelectedConnectionId(matchingConn.id);
            } else {
              // Fall back to default or first
              const defaultConn = data.connections.find(c => c.is_default) || data.connections[0];
              setSelectedConnectionId(defaultConn.id);
            }
          } else {
            // No activeConnection, use default
            const defaultConn = data.connections.find(c => c.is_default) || data.connections[0];
            setSelectedConnectionId(defaultConn.id);
          }
        }
      } catch (err) {
        setConnectionError(err.response?.data?.error || err.message || 'Failed to load connections.');
      } finally {
        setLoading(false);
      }
    };
    loadConnections();
  }, [activeConnection]);

  // Load tables when selected connection changes
  useEffect(() => {
    if (!selectedConnection) return;

    const loadTables = async () => {
      try {
        setTablesLoading(true);
        setTableError(null);

        // Handle case where connection might be a string
        const connectionString = typeof selectedConnection === 'string'
          ? selectedConnection
          : buildConnectionString(selectedConnection);

        if (!connectionString) {
          setTableError('Invalid connection information');
          return;
        }

        const response = await fetchTables(connectionString);
        setTables(response.tables || []);
      } catch (err) {
        setTableError(err.response?.data?.error || err.message || 'Failed to load tables.');
      } finally {
        setTablesLoading(false);
      }
    };
    loadTables();
  }, [selectedConnection]);

  // Notify parent when connection changes
  useEffect(() => {
    if (selectedConnection && onConnectionChange) {
      onConnectionChange(selectedConnection);
    }
  }, [selectedConnection, onConnectionChange]);

  // Notify parent when table changes
  useEffect(() => {
    if (selectedTable && onTableChange) {
      onTableChange(selectedTable);
    }
  }, [selectedTable, onTableChange]);

  useEffect(() => {
    // Clean up any direct connection strings in localStorage when component mounts
    const storedConnId = localStorage.getItem('connectionId');
      if (storedConnId && connections && connections.length > 0) {
        // Find connection by ID
        const storedConnection = connections.find(c => c.id === storedConnId);
        if (storedConnection) {
          setSelectedConnectionId(storedConnection.id);
          // Notify parent without setting localStorage (to avoid loops)
          if (onConnectionChange) {
            onConnectionChange(storedConnection);
          }
        }
      }
  }, []);

  // Handle connection selection change
  const handleConnectionChange = (e) => {
    const connId = e.target.value;
    setSelectedConnectionId(connId);

    // Find the selected connection and notify parent
    if (onConnectionChange && connections.length > 0) {
      const newConnection = connections.find(c => c.id === connId);
      if (newConnection) {
        // Store connection ID, not the full object
        localStorage.setItem('connectionId', connId);
        if (localStorage.getItem('connectionString')) {
          localStorage.removeItem('connectionString');
        }
        onConnectionChange(newConnection);
      }
    }
  };

  // Utility function to build connection string
  const buildConnectionString = (connection) => {
    if (!connection) return '';

    // If connection is already a string (from localStorage), return it directly
    if (typeof connection === 'string') {
      return connection;
    }

    const { connection_type, connection_details } = connection;
    const encode = (str) => encodeURIComponent(str || '');

    switch (connection_type) {
      case 'snowflake':
        return connection_details.useEnvVars
          ? `snowflake://${connection_details.envVarPrefix}_CONNECTION`
          : `snowflake://${encode(connection_details.username)}:${encode(connection_details.password)}@${connection_details.account}/${connection_details.database}/${connection_details.schema}?warehouse=${connection_details.warehouse}`;
      case 'duckdb':
        return `duckdb:///${connection_details.path}`;
      case 'postgresql':
        return `postgresql://${encode(connection_details.username)}:${encode(connection_details.password)}@${connection_details.host}:${connection_details.port}/${connection_details.database}`;
      default:
        return '';
    }
  };

  const handleRefreshConnection = () => {
    if (!selectedConnection || !selectedTable) {
      return;
    }

    // Set loading state
    setLoading(true);

    // Notify parent about refreshing the connection
    if (onConnectionChange) {
      // Pass the same connection object to trigger a refresh
      onConnectionChange(selectedConnection);
    }

    // Notify parent about the current table (to ensure it's loaded)
    if (onTableChange) {
      onTableChange(selectedTable);
    }

    // Reset loading state after a short delay
    setTimeout(() => {
      setLoading(false);
    }, 500);
  };

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header bg-light d-flex justify-content-between align-items-center">
        <h5 className="mb-0">
          <i className="bi bi-database me-2"></i>
          Data Source
        </h5>
        <div>
          <button
              className="btn btn-sm btn-outline-secondary me-2"
              onClick={handleRefreshConnection}
              disabled={loading}
          >
            <i className="bi bi-arrow-repeat me-1"></i>
            Refresh Connection
          </button>
          <Link to="/connections" className="btn btn-sm btn-outline-primary">
            <i className="bi bi-gear me-1"></i>
            Manage Connections
          </Link>
        </div>
      </div>

      <div className="card-body pb-2">
        {loading ? (
            <div className="d-flex justify-content-center my-3">
              <div className="spinner-border text-primary" role="status">
                <span className="visually-hidden">Loading connections...</span>
              </div>
            </div>
        ) : connectionError ? (
            <div className="alert alert-danger">
              <i className="bi bi-exclamation-triangle-fill me-2"></i>
              {connectionError}
            </div>
        ) : connections.length === 0 ? (
            <div className="alert alert-warning">
            <i className="bi bi-info-circle-fill me-2"></i>
            No database connections found. <Link to="/connections">Click here</Link> to add a connection.
          </div>
        ) : (
          <>
            <div className="row mb-3">
              <div className="col-md-6 mb-2 mb-md-0">
                <label className="form-label"><strong>Selected Connection:</strong></label>
                <div>
                  <select
                    className="form-select"
                    value={selectedConnectionId || ''}
                    onChange={handleConnectionChange}
                    disabled={typeof activeConnection === 'string'}
                  >
                    {typeof activeConnection === 'string' ? (
                      <option value="">Using direct connection string</option>
                    ) : (
                      connections.map(conn => (
                        <option key={conn.id} value={conn.id}>
                          {conn.name} {conn.is_default ? '(Default)' : ''}
                        </option>
                      ))
                    )}
                  </select>
                </div>
              </div>

              <div className="col-md-6 mb-2 mb-md-0">
                <label htmlFor="tableSelect" className="form-label"><strong>Select Table:</strong></label>
                <div>
                  {tablesLoading ? (
                    <div className="d-flex align-items-center">
                      <div className="spinner-border spinner-border-sm text-primary me-2" role="status">
                        <span className="visually-hidden">Loading tables...</span>
                      </div>
                      <span>Loading tables...</span>
                    </div>
                  ) : tableError ? (
                    <div className="alert alert-danger py-1">
                      <i className="bi bi-exclamation-triangle-fill me-2"></i>
                      {tableError}
                    </div>
                  ) : tables.length === 0 ? (
                    <div className="alert alert-info py-1">
                      <i className="bi bi-info-circle-fill me-2"></i>
                      No tables found
                    </div>
                  ) : (
                    <select
                      id="tableSelect"
                      className="form-select"
                      value={selectedTable}
                      onChange={(e) => onTableChange(e.target.value)}
                    >
                      {tables.map((table) => (
                        <option key={table} value={table}>
                          {table}
                        </option>
                      ))}
                    </select>
                  )}
                </div>
              </div>
            </div>

            {selectedConnection && typeof selectedConnection === 'object' ? (
              <div className="row mb-3">
                <div className="col-md-6 mb-2">
                  <div className="mb-1">
                    <strong>Account:</strong> <span className="text-secondary">{selectedConnection.connection_details?.account || 'N/A'}</span>
                  </div>
                  <div className="mb-1">
                    <strong>Username:</strong> <span className="text-secondary">{selectedConnection.connection_details?.username || 'N/A'}</span>
                  </div>
                  <div className="mb-1">
                    <strong>Warehouse:</strong> <span className="text-secondary">{selectedConnection.connection_details?.warehouse || 'N/A'}</span>
                  </div>
                </div>
                <div className="col-md-6 mb-2">
                  <div className="mb-1">
                    <strong>Database:</strong> <span className="text-secondary">{selectedConnection.connection_details?.database || 'N/A'}</span>
                  </div>
                  <div className="mb-1">
                    <strong>Schema:</strong> <span className="text-secondary">{selectedConnection.connection_details?.schema || 'PUBLIC'}</span>
                  </div>
                  <div className="mb-1">
                    <strong>Type:</strong> <span className="badge bg-secondary">{selectedConnection.connection_type}</span>
                  </div>
                </div>
              </div>
            ) : selectedConnection && typeof selectedConnection === 'string' ? (
              <div className="alert alert-info mb-3 py-2">
                <i className="bi bi-info-circle me-2"></i>
                Using connection string: {selectedConnection.substring(0, 20)}...
              </div>
            ) : null}
          </>
        )}
      </div>
    </div>
  );
}

export default DataSourcePanel;