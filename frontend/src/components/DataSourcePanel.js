import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { fetchConnections, fetchTables } from '../api';

function DataSourcePanel({
  activeConnection = null,
  tableName,
  onTableChange,
  onConnectionChange
}) {
  const [connections, setConnections] = useState([]);
  const [selectedConnection, setSelectedConnection] = useState(null);
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState(tableName || '');
  const [loading, setLoading] = useState(true);
  const [tablesLoading, setTablesLoading] = useState(false);
  const [error, setError] = useState(null);

  // Load connections on mount
  useEffect(() => {
    loadConnections();
  }, []);

  // Load tables when connection changes
  useEffect(() => {
    if (selectedConnection) {
      loadTables(selectedConnection);
    }
  }, [selectedConnection]);

  useEffect(() => {
    if (selectedConnection) {
      console.log("DataSourcePanel - selectedConnection structure:", {
        id: selectedConnection.id,
        name: selectedConnection.name,
        connection_type: selectedConnection.connection_type,
        connection_details: selectedConnection.connection_details ? {
          ...selectedConnection.connection_details,
          password: selectedConnection.connection_details.password ? "[PRESENT]" : "[MISSING]",
        } : "[MISSING]",
        is_default: selectedConnection.is_default
      });

      if (onConnectionChange) {
        console.log("DataSourcePanel - About to call onConnectionChange");
        onConnectionChange(selectedConnection);
      }
    }
  }, [selectedConnection]);

  // Set selectedTable when tableName prop changes
  useEffect(() => {
    if (tableName) {
      setSelectedTable(tableName);
    }
  }, [tableName]);

  // When activeConnection is provided, select it
  useEffect(() => {
    if (activeConnection && connections.length > 0) {
      const conn = connections.find(c => c.id === activeConnection.id) || connections[0];
      setSelectedConnection(conn);
    } else if (connections.length > 0) {
      // Select the default connection or the first one
      const defaultConn = connections.find(c => c.is_default) || connections[0];
      setSelectedConnection(defaultConn);
    }
  }, [activeConnection, connections]);

  // Function to load connections
  const loadConnections = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await fetchConnections();
      setConnections(data.connections || []);

      // If no active connection is set, select the default one
      if (!activeConnection && data.connections && data.connections.length > 0) {
        const defaultConn = data.connections.find(c => c.is_default) || data.connections[0];
        setSelectedConnection(defaultConn);
        if (onConnectionChange) {
          onConnectionChange(defaultConn);
        }
      }
    } catch (err) {
      setError('Failed to load connections: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // Function to load tables for a connection
  const loadTables = async (connection) => {
    if (!connection) return;

    // Build connection string based on connection type
    let connectionString = '';

    try {
      setTablesLoading(true);
      setError(null);

      switch (connection.connection_type) {
        case 'snowflake':
          if (connection.connection_details.useEnvVars) {
            connectionString = `snowflake://${connection.connection_details.envVarPrefix}_CONNECTION`;
          } else {
            const { username, password, account, database, schema, warehouse } = connection.connection_details;
            // URL encode the password for special characters
            const encodedPassword = encodeURIComponent(password || '');
            connectionString = `snowflake://${username}:${encodedPassword}@${account}/${database}/${schema}?warehouse=${warehouse}`;
          }
          break;

        case 'duckdb':
          connectionString = `duckdb:///${connection.connection_details.path}`;
          break;

        case 'postgresql':
          const { username, password, host, port, database } = connection.connection_details;
          // URL encode the password for special characters
          const encodedPassword = encodeURIComponent(password || '');
          connectionString = `postgresql://${username}:${encodedPassword}@${host}:${port}/${database}`;
          break;

        default:
          throw new Error(`Unsupported connection type: ${connection.connection_type}`);
      }

      // Fetch tables
      const response = await fetchTables(connectionString);
      setTables(response.tables || []);

      // Set first table if none selected
      if ((!selectedTable || selectedTable === '') && response.tables && response.tables.length > 0) {
        setSelectedTable(response.tables[0]);
        if (onTableChange) {
          onTableChange(response.tables[0]);
        }
      }
    } catch (err) {
      setError('Failed to load tables: ' + (err.response?.data?.error || err.message));
      setTables([]);
    } finally {
      setTablesLoading(false);
    }
  };

  // Handle table selection change
  const handleTableChange = (e) => {
    const newTable = e.target.value;
    setSelectedTable(newTable);
    if (onTableChange) {
      onTableChange(newTable);
    }
  };

  // Build the connection string for display
  const getConnectionDisplayString = () => {
    if (!selectedConnection) return 'No connection selected';

    switch (selectedConnection.connection_type) {
      case 'snowflake':
        if (selectedConnection.connection_details.useEnvVars) {
          return `Using ${selectedConnection.connection_details.envVarPrefix} environment variables`;
        }
        return `${selectedConnection.connection_details.account} / ${selectedConnection.connection_details.database}`;

      case 'duckdb':
        return selectedConnection.connection_details.path;

      case 'postgresql':
        return `${selectedConnection.connection_details.host}:${selectedConnection.connection_details.port} / ${selectedConnection.connection_details.database}`;

      default:
        return selectedConnection.name;
    }
  };

  // Get connection details for display
  const getConnectionDetails = () => {
    if (!selectedConnection) return null;

    const { connection_details, connection_type } = selectedConnection;

    switch (connection_type) {
      case 'snowflake':
        return (
          <>
            <div className="row mb-2">
              <div className="col-md-4 fw-bold">Account:</div>
              <div className="col-md-8">{connection_details.useEnvVars ? `${connection_details.envVarPrefix}_ACCOUNT` : connection_details.account}</div>
            </div>
            <div className="row mb-2">
              <div className="col-md-4 fw-bold">Username:</div>
              <div className="col-md-8">{connection_details.useEnvVars ? `${connection_details.envVarPrefix}_USER` : connection_details.username}</div>
            </div>
            <div className="row mb-2">
              <div className="col-md-4 fw-bold">Warehouse:</div>
              <div className="col-md-8">{connection_details.useEnvVars ? `${connection_details.envVarPrefix}_WAREHOUSE` : connection_details.warehouse}</div>
            </div>
            <div className="row mb-2">
              <div className="col-md-4 fw-bold">Database:</div>
              <div className="col-md-8">{connection_details.useEnvVars ? `${connection_details.envVarPrefix}_DATABASE` : connection_details.database}</div>
            </div>
            <div className="row mb-2">
              <div className="col-md-4 fw-bold">Schema:</div>
              <div className="col-md-8">{connection_details.useEnvVars ? `${connection_details.envVarPrefix}_SCHEMA` : connection_details.schema}</div>
            </div>
          </>
        );

      case 'duckdb':
        return (
          <div className="row mb-2">
            <div className="col-md-4 fw-bold">Path:</div>
            <div className="col-md-8">{connection_details.path}</div>
          </div>
        );

      case 'postgresql':
        return (
          <>
            <div className="row mb-2">
              <div className="col-md-4 fw-bold">Host:</div>
              <div className="col-md-8">{connection_details.host}:{connection_details.port}</div>
            </div>
            <div className="row mb-2">
              <div className="col-md-4 fw-bold">Username:</div>
              <div className="col-md-8">{connection_details.username}</div>
            </div>
            <div className="row mb-2">
              <div className="col-md-4 fw-bold">Database:</div>
              <div className="col-md-8">{connection_details.database}</div>
            </div>
          </>
        );

      default:
        return null;
    }
  };

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header bg-light d-flex justify-content-between align-items-center">
        <h5 className="mb-0">
          <i className="bi bi-database me-2"></i>
          Data Source
        </h5>
        <Link
          to="/connections"
          className="btn btn-sm btn-outline-primary"
        >
          <i className="bi bi-gear me-1"></i>
          Manage Connections
        </Link>
      </div>

      <div className="card-body">
        {loading ? (
          <div className="d-flex justify-content-center">
            <div className="spinner-border text-primary" role="status">
              <span className="visually-hidden">Loading...</span>
            </div>
          </div>
        ) : error ? (
          <div className="alert alert-danger">
            <i className="bi bi-exclamation-triangle-fill me-2"></i>
            {error}
          </div>
        ) : connections.length === 0 ? (
          <div className="alert alert-warning">
            <i className="bi bi-exclamation-triangle-fill me-2"></i>
            No database connections found. Please create a connection first.
            <div className="mt-3">
              <Link to="/connections" className="btn btn-primary">
                <i className="bi bi-plus-circle me-1"></i>
                Create Connection
              </Link>
            </div>
          </div>
        ) : (
          <>
            {/* Connection Details Section */}
            {selectedConnection && (
              <div className="mb-4">
                <div className="d-flex justify-content-between align-items-center mb-3">
                  <h6 className="mb-0">
                    <i className="bi bi-diagram-3 me-1"></i>
                    Connection: {selectedConnection.name}
                  </h6>
                  <span className="badge bg-secondary">
                    {selectedConnection.connection_type}
                    {selectedConnection.is_default && (
                      <span className="ms-1 badge bg-success">Default</span>
                    )}
                  </span>
                </div>

                <div className="card bg-light">
                  <div className="card-body py-2 px-3">
                    {getConnectionDetails()}
                  </div>
                </div>
              </div>
            )}

            {/* Table Selection Section */}
            <div className="mb-3">
              <label htmlFor="tableName" className="form-label">Select Table</label>
              {tablesLoading ? (
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
                  value={selectedTable}
                  onChange={handleTableChange}
                  required
                >
                  {tables.map(table => (
                    <option key={table} value={table}>{table}</option>
                  ))}
                </select>
              ) : (
                <div className="alert alert-warning">
                  <i className="bi bi-exclamation-triangle-fill me-2"></i>
                  No tables found in this database.
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default DataSourcePanel;