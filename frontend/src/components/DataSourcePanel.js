import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { fetchConnections, fetchTables, fetchConnectionById } from '../api';

function DataSourcePanel({ tableName, onTableChange, onConnectionChange, activeConnection }) {
  const [connections, setConnections] = useState([]);
  const [tables, setTables] = useState([]);
  const [selectedConnectionId, setSelectedConnectionId] = useState('');
  const [selectedTable, setSelectedTable] = useState('');
  const [loading, setLoading] = useState(true);
  const [connectionDetailsLoading, setConnectionDetailsLoading] = useState(false);
  const [tablesLoading, setTablesLoading] = useState(false);
  const [connectionError, setConnectionError] = useState(null);
  const [tableError, setTableError] = useState(null);
  const [selectedConnection, setSelectedConnection] = useState(null);

  // Fetch connections list when component mounts
  useEffect(() => {
    const loadConnections = async () => {
      setLoading(true);
      setConnectionError(null);

      try {
        const response = await fetchConnections();
        console.log("[DataSourcePanel] Fetched connections:", response);

        // Make sure we're setting an array
        const connectionsArray = Array.isArray(response) ? response :
                               (response && Array.isArray(response.connections)) ?
                               response.connections : [];

        setConnections(connectionsArray);

        // If no active connection but we have connections, set the first one as active
        if (!activeConnection && connectionsArray.length > 0) {
          const defaultConn = connectionsArray.find(c => c.is_default) || connectionsArray[0];
          console.log("[DataSourcePanel] Setting default connection:", defaultConn);
          setSelectedConnectionId(defaultConn.id);
          setSelectedConnection(defaultConn);
          onConnectionChange(defaultConn);
        }
      } catch (error) {
        console.error("[DataSourcePanel] Error fetching connections:", error);
        setConnectionError("Failed to load connections. Please try again.");
      } finally {
        setLoading(false);
      }
    };

    loadConnections();
  }, [onConnectionChange]); // Only run when component mounts

  // Fetch connection details if we only have an ID
  useEffect(() => {
    if (activeConnection && activeConnection.id && !activeConnection.connection_type) {
      console.log("[DataSourcePanel] Have connection ID but no details, fetching:", activeConnection.id);
      setConnectionDetailsLoading(true);

      fetchConnectionById(activeConnection.id)
        .then(details => {
          console.log("[DataSourcePanel] Fetched full connection details:", details);
          // Extract the connection from the response
          if (details && details.connection) {
            onConnectionChange(details.connection); // Update with the actual connection object
          } else {
            throw new Error("Invalid connection response format");
          }
        })
        .catch(err => {
          console.error("[DataSourcePanel] Error fetching connection details:", err);
          setConnectionError("Failed to load connection details. Please try again.");
        })
        .finally(() => {
          setConnectionDetailsLoading(false);
        });
    }
  }, [activeConnection, onConnectionChange]);

  // Update selected connection when activeConnection changes
  useEffect(() => {
    if (activeConnection) {
      console.log("[DataSourcePanel] Active connection changed:", activeConnection);

      if (typeof activeConnection === 'object') {
        // If it's an object with connection details
        setSelectedConnection(activeConnection);
        if (activeConnection.id) {
          setSelectedConnectionId(activeConnection.id);
        }
      } else {
        // If it's a connection string
        setSelectedConnection(activeConnection);
      }
    }
  }, [activeConnection]);

  // Update selected table when tableName changes
  useEffect(() => {
    if (tableName) {
      console.log("[DataSourcePanel] Table name changed:", tableName);
      setSelectedTable(tableName);
    }
  }, [tableName]);

  // Fetch tables when connection changes
  useEffect(() => {
    const loadTables = async () => {
      if (!selectedConnection) return;

      setTablesLoading(true);
      setTableError(null);

      try {
        console.log("[DataSourcePanel] Fetching tables for connection:", selectedConnection);
        const tablesList = await fetchTables(selectedConnection);
        console.log("[DataSourcePanel] Fetched tables:", tablesList);
        setTables(tablesList);

        // If we have tables but no selected table, select the first one
        if (tablesList.length > 0 && !selectedTable) {
          const firstTable = tablesList[0];
          console.log("[DataSourcePanel] Setting default table:", firstTable);
          setSelectedTable(firstTable);
          onTableChange(firstTable);
        } else if (tablesList.length > 0 && selectedTable) {
          // Check if current selectedTable exists in new tables list
          if (!tablesList.includes(selectedTable)) {
            const firstTable = tablesList[0];
            console.log("[DataSourcePanel] Selected table not found in new list, setting default:", firstTable);
            setSelectedTable(firstTable);
            onTableChange(firstTable);
          }
        }
      } catch (error) {
        console.error("[DataSourcePanel] Error fetching tables:", error);
        setTableError("Failed to load tables. Please check your connection settings.");
      } finally {
        setTablesLoading(false);
      }
    };

    loadTables();
  }, [selectedConnection, onTableChange]);

  // Handle connection change from dropdown
  const handleConnectionChange = (event) => {
    const connId = event.target.value;
    console.log("[DataSourcePanel] Connection dropdown changed to:", connId);

    if (connId) {
      const conn = connections.find(c => c.id === connId);
      if (conn) {
        console.log("[DataSourcePanel] Found connection:", conn);
        setSelectedConnectionId(connId);
        setSelectedConnection(conn);
        onConnectionChange(conn);
      }
    }
  };

  // Handle refresh connection button
  const handleRefreshConnection = () => {
    console.log("[DataSourcePanel] Refresh connection clicked");

    // Refresh connections list
    setLoading(true);
    fetchConnections()
      .then(response => {
        console.log("[DataSourcePanel] Refreshed connections:", response);

        // Make sure we're setting an array
        const connectionsArray = Array.isArray(response) ? response :
                               (response && Array.isArray(response.connections)) ?
                               response.connections : [];

        setConnections(connectionsArray);

        // If we have an active connection ID, find it in the refreshed list
        if (selectedConnectionId && connectionsArray.length > 0) {
          const refreshedConn = connectionsArray.find(c => c.id === selectedConnectionId);
          if (refreshedConn) {
            console.log("[DataSourcePanel] Refreshed current connection:", refreshedConn);
            setSelectedConnection(refreshedConn);
            onConnectionChange(refreshedConn);
          }
        }
      })
      .catch(error => {
        console.error("[DataSourcePanel] Error refreshing connections:", error);
        setConnectionError("Failed to refresh connections. Please try again.");
      })
      .finally(() => {
        setLoading(false);
      });
  };

  const isLoading = loading || connectionDetailsLoading;

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
            disabled={isLoading}
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
        {isLoading ? (
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
                      // Make sure connections is an array before trying to map over it
                      Array.isArray(connections) ? connections.map(conn => (
                        <option key={conn.id} value={conn.id}>
                          {conn.name} {conn.is_default ? '(Default)' : ''}
                        </option>
                      )) : <option value="">Loading connections...</option>
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
            ) : (
              // Add a message for when we have an ID but no full connection details yet
              activeConnection && activeConnection.id && !activeConnection.connection_type && (
                <div className="alert alert-info mb-3 py-2">
                  <i className="bi bi-info-circle me-2"></i>
                  Loading connection details...
                </div>
              )
            )}
          </>
        )}
      </div>
    </div>
  );
}

export default DataSourcePanel;