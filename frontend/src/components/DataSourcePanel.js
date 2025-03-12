import React, { useState, useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { fetchConnections, fetchTables } from '../api';

function DataSourcePanel({ activeConnection, tableName, onTableChange, onConnectionChange }) {
  const [connections, setConnections] = useState([]);
  const [tables, setTables] = useState([]);
  const [loading, setLoading] = useState(true);
  const [tablesLoading, setTablesLoading] = useState(false);
  const [connectionError, setConnectionError] = useState(null);
  const [tableError, setTableError] = useState(null);

  // Compute selected connection dynamically
  const selectedConnection = useMemo(() => {
    if (!connections.length) return null;
    return activeConnection
      ? connections.find(c => c.id === activeConnection.id) || connections[0]
      : connections.find(c => c.is_default) || connections[0];
  }, [activeConnection, connections]);

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
      } catch (err) {
        setConnectionError(err.message || 'Failed to load connections.');
      } finally {
        setLoading(false);
      }
    };
    loadConnections();
  }, []);

  // Load tables when selected connection changes
  useEffect(() => {
    if (!selectedConnection) return;

    const loadTables = async () => {
      try {
        setTablesLoading(true);
        setTableError(null);
        const connectionString = buildConnectionString(selectedConnection);
        const response = await fetchTables(connectionString);
        setTables(response.tables || []);
      } catch (err) {
        setTableError(err.message || 'Failed to load tables.');
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

  // Utility function to build connection string
  const buildConnectionString = (connection) => {
    if (!connection) return '';

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

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header bg-light d-flex justify-content-between align-items-center">
        <h5 className="mb-0">
          <i className="bi bi-database me-2"></i>
          Data Source
        </h5>
        <Link to="/connections" className="btn btn-sm btn-outline-primary">
          <i className="bi bi-gear me-1"></i>
          Manage Connections
        </Link>
      </div>

      <div className="card-body">
        {loading ? (
          <p>Loading connections...</p>
        ) : connectionError ? (
          <p className="text-danger">{connectionError}</p>
        ) : (
          <>
            <p><strong>Selected Connection:</strong> {selectedConnection?.name || 'None'}</p>
            <p><strong>Connection Details:</strong> {buildConnectionString(selectedConnection)}</p>
          </>
        )}

        {tablesLoading ? (
          <p>Loading tables...</p>
        ) : tableError ? (
          <p className="text-danger">{tableError}</p>
        ) : (
          <div className="form-group">
            <label htmlFor="tableSelect">Select Table:</label>
            <select
              id="tableSelect"
              className="form-control"
              value={selectedTable}
              onChange={(e) => onTableChange(e.target.value)}
            >
              {tables.map((table) => (
                <option key={table} value={table}>
                  {table}
                </option>
              ))}
            </select>
          </div>
        )}
      </div>
    </div>
  );
}

export default DataSourcePanel;
