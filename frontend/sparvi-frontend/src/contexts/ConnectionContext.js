import React, { createContext, useContext, useState, useEffect } from 'react';
import { connectionsAPI } from '../api/apiService';
import { useAuth } from './AuthContext';

// Create the connection context
const ConnectionContext = createContext();

export const useConnection = () => {
  return useContext(ConnectionContext);
};

export const ConnectionProvider = ({ children }) => {
  const { isAuthenticated } = useAuth();
  const [connections, setConnections] = useState([]);
  const [defaultConnection, setDefaultConnection] = useState(null);
  const [activeConnection, setActiveConnection] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Load connections when authenticated
  useEffect(() => {
    const fetchConnections = async () => {
      if (!isAuthenticated) {
        setConnections([]);
        setDefaultConnection(null);
        setActiveConnection(null);
        setLoading(false);
        return;
      }

      try {
        setLoading(true);
        setError(null);

        const response = await connectionsAPI.getAll();
        const connectionList = response.data.connections || [];

        setConnections(connectionList);

        // Find default connection
        const defaultConn = connectionList.find(conn => conn.is_default);
        setDefaultConnection(defaultConn || null);

        // Set active connection to default if not already set
        if (!activeConnection && defaultConn) {
          setActiveConnection(defaultConn);
        }
      } catch (err) {
        console.error('Error fetching connections:', err);
        setError(err.message || 'Failed to load connections');
      } finally {
        setLoading(false);
      }
    };

    fetchConnections();
  }, [isAuthenticated]); // eslint-disable-line react-hooks/exhaustive-deps

  // Create a new connection
  const createConnection = async (connectionData) => {
    try {
      setError(null);
      const response = await connectionsAPI.create(connectionData);
      const newConnection = response.data.connection;

      setConnections(prev => [...prev, newConnection]);

      // If this is the first connection, make it active
      if (connections.length === 0) {
        setActiveConnection(newConnection);
      }

      return newConnection;
    } catch (err) {
      setError(err.message || 'Failed to create connection');
      throw err;
    }
  };

  // Update a connection
  const updateConnection = async (id, connectionData) => {
    try {
      setError(null);
      const response = await connectionsAPI.update(id, connectionData);
      const updatedConnection = response.data.connection;

      setConnections(prev =>
        prev.map(conn => conn.id === id ? updatedConnection : conn)
      );

      // Update active connection if it's the one being updated
      if (activeConnection && activeConnection.id === id) {
        setActiveConnection(updatedConnection);
      }

      // Update default connection if it's the one being updated
      if (defaultConnection && defaultConnection.id === id) {
        setDefaultConnection(updatedConnection);
      }

      return updatedConnection;
    } catch (err) {
      setError(err.message || 'Failed to update connection');
      throw err;
    }
  };

  // Delete a connection
  const deleteConnection = async (id) => {
    try {
      setError(null);
      await connectionsAPI.delete(id);

      // Remove from connections list
      setConnections(prev => prev.filter(conn => conn.id !== id));

      // Reset active connection if it's the one being deleted
      if (activeConnection && activeConnection.id === id) {
        const newActive = connections.find(conn => conn.id !== id) || null;
        setActiveConnection(newActive);
      }

      // Reset default connection if it's the one being deleted
      if (defaultConnection && defaultConnection.id === id) {
        setDefaultConnection(null);
      }
    } catch (err) {
      setError(err.message || 'Failed to delete connection');
      throw err;
    }
  };

  // Set a connection as the default
  const setAsDefaultConnection = async (id) => {
    try {
      setError(null);
      await connectionsAPI.setDefault(id);

      // Update connections to reflect new default
      setConnections(prev =>
        prev.map(conn => ({
          ...conn,
          is_default: conn.id === id
        }))
      );

      // Update default connection reference
      const newDefault = connections.find(conn => conn.id === id) || null;
      setDefaultConnection(newDefault);

      return newDefault;
    } catch (err) {
      setError(err.message || 'Failed to set default connection');
      throw err;
    }
  };

  // Test a connection configuration without saving
  const testConnection = async (connectionData) => {
    try {
      setError(null);
      const response = await connectionsAPI.test(connectionData);
      return response.data;
    } catch (err) {
      setError(err.message || 'Connection test failed');
      throw err;
    }
  };

  // Set active connection (for current UI session)
  const setCurrentConnection = (connection) => {
    setActiveConnection(connection);
  };

  // Get a specific connection by ID
  const getConnection = async (id) => {
    try {
      setError(null);
      const response = await connectionsAPI.getById(id);
      return response.data.connection;
    } catch (err) {
      setError(err.message || 'Failed to get connection details');
      throw err;
    }
  };

  // Refresh the list of connections
  const refreshConnections = async () => {
    try {
      setLoading(true);
      setError(null);

      const response = await connectionsAPI.getAll();
      const connectionList = response.data.connections || [];

      setConnections(connectionList);

      // Update default connection reference
      const defaultConn = connectionList.find(conn => conn.is_default);
      setDefaultConnection(defaultConn || null);

      // Ensure active connection is still valid
      if (activeConnection) {
        const stillExists = connectionList.find(conn => conn.id === activeConnection.id);
        if (!stillExists) {
          setActiveConnection(defaultConn || connectionList[0] || null);
        } else {
          // Update active connection with latest data
          const updatedActive = connectionList.find(conn => conn.id === activeConnection.id);
          setActiveConnection(updatedActive);
        }
      }

      return connectionList;
    } catch (err) {
      setError(err.message || 'Failed to refresh connections');
      throw err;
    } finally {
      setLoading(false);
    }
  };

  // Context value
  const value = {
    connections,
    defaultConnection,
    activeConnection,
    loading,
    error,
    createConnection,
    updateConnection,
    deleteConnection,
    setAsDefaultConnection,
    testConnection,
    setCurrentConnection,
    getConnection,
    refreshConnections,
  };

  return <ConnectionContext.Provider value={value}>{children}</ConnectionContext.Provider>;
};

export default ConnectionContext;