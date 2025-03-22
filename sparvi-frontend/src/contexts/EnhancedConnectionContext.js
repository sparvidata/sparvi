import React, { createContext, useContext, useState, useEffect, useCallback, useRef } from 'react';
import { connectionsAPI } from '../api/enhancedApiService';
import { useAuth } from './AuthContext';
import { debounce } from '../utils/requestUtils';
import { clearCacheItem } from '../utils/cacheUtils';

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
  const [isRefreshing, setIsRefreshing] = useState(false);

  // Use refs to store the last fetch time and data
  const lastFetchTimeRef = useRef(0);
  const lastConnectionsRef = useRef([]);
  const isInitialLoadRef = useRef(true);
  const MIN_FETCH_INTERVAL = 10000; // Minimum 10 seconds between full refreshes

  // Load connections when authenticated
  useEffect(() => {
    const fetchConnections = async (forceFresh = false) => {
      if (!isAuthenticated) {
        // Don't clear existing data during initial render
        if (isInitialLoadRef.current) {
          isInitialLoadRef.current = false;
          setLoading(false);
          return;
        }

        // Only clear data if we're sure we should be logged out
        setConnections([]);
        setDefaultConnection(null);
        setActiveConnection(null);
        setLoading(false);
        return;
      }

      // Throttle fetchConnections to prevent too many calls
      const now = Date.now();
      if (!forceFresh && now - lastFetchTimeRef.current < MIN_FETCH_INTERVAL) {
        // Return previous connections if we just fetched them
        if (lastConnectionsRef.current.length > 0) {
          // Don't update state if it's already the same data to prevent re-renders
          if (connections.length !== lastConnectionsRef.current.length) {
            setConnections(lastConnectionsRef.current);

            // Find default connection
            const defaultConn = lastConnectionsRef.current.find(conn => conn.is_default);
            setDefaultConnection(defaultConn || null);

            // Set active connection to default if not already set
            if (!activeConnection && defaultConn) {
              setActiveConnection(defaultConn);
            }
          }

          setLoading(false);
          return;
        }
      }

      // If we already have data and are just refreshing, don't set loading to true
      // This prevents UI flicker during navigation
      if (lastConnectionsRef.current.length > 0 && !forceFresh) {
        setIsRefreshing(true);
      } else {
        setLoading(true);
      }

      try {
        setError(null);

        // Update last fetch time
        lastFetchTimeRef.current = now;

        const response = await connectionsAPI.getAll({ forceFresh });

        // Check if response is canceled
        if (response?.cancelled) {
          console.log('Connection fetch was cancelled');
          return;
        }

        const connectionList = response?.data?.connections || [];

        // Only update if we got different data to prevent unnecessary re-renders
        const shouldUpdate = JSON.stringify(connectionList) !== JSON.stringify(lastConnectionsRef.current);

        if (shouldUpdate) {
          // Update the connection reference
          lastConnectionsRef.current = connectionList;
          setConnections(connectionList);

          // Find default connection
          const defaultConn = connectionList.find(conn => conn.is_default);
          setDefaultConnection(defaultConn || null);

          // Set active connection to default if not already set
          if (!activeConnection && defaultConn) {
            setActiveConnection(defaultConn);
          } else if (activeConnection) {
            // If activeConnection is set, make sure it's updated with fresh data
            const updatedActiveConn = connectionList.find(conn => conn.id === activeConnection.id);
            if (updatedActiveConn) {
              setActiveConnection(updatedActiveConn);
            } else if (defaultConn) {
              // If active connection no longer exists, fall back to default
              setActiveConnection(defaultConn);
            }
          }
        }
      } catch (err) {
        // Don't set error state for cancelled requests
        if (!err.cancelled) {
          console.error('Error fetching connections:', err);
          setError(err.message || 'Failed to load connections');
        }
      } finally {
        setLoading(false);
        setIsRefreshing(false);
        isInitialLoadRef.current = false;
      }
    };

    fetchConnections();

    // Set up interval to refresh connections periodically (every 60 seconds)
    const intervalId = setInterval(() => {
      fetchConnections(true);
    }, 60000);

    return () => clearInterval(intervalId);
  }, [isAuthenticated]); // eslint-disable-line react-hooks/exhaustive-deps

  // Create a new connection
  const createConnection = useCallback(async (connectionData) => {
    try {
      setError(null);
      const response = await connectionsAPI.create(connectionData);
      const newConnection = response.connection;

      // Update connections list with optimistic update
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
  }, [connections.length]);

  // Update a connection
  const updateConnection = useCallback(async (id, connectionData) => {
    try {
      setError(null);

      // Optimistic update
      const updatedConnectionData = {
        ...connections.find(c => c.id === id),
        ...connectionData,
        updated_at: new Date().toISOString()
      };

      // Update local state immediately
      setConnections(prev =>
        prev.map(conn => conn.id === id ? updatedConnectionData : conn)
      );

      // Update active connection if it's the one being updated
      if (activeConnection && activeConnection.id === id) {
        setActiveConnection(updatedConnectionData);
      }

      // Update default connection if it's the one being updated
      if (defaultConnection && defaultConnection.id === id) {
        setDefaultConnection(updatedConnectionData);
      }

      // Make the actual API call
      const response = await connectionsAPI.update(id, connectionData);
      const serverUpdatedConnection = response.connection;

      // Update with server response (to catch any differences)
      setConnections(prev =>
        prev.map(conn => conn.id === id ? serverUpdatedConnection : conn)
      );

      // Update references again if needed
      if (activeConnection && activeConnection.id === id) {
        setActiveConnection(serverUpdatedConnection);
      }
      if (defaultConnection && defaultConnection.id === id) {
        setDefaultConnection(serverUpdatedConnection);
      }

      return serverUpdatedConnection;
    } catch (err) {
      // Revert optimistic update on error
      refreshConnections();
      setError(err.message || 'Failed to update connection');
      throw err;
    }
  }, [activeConnection, connections, defaultConnection]);

  // Delete a connection
  const deleteConnection = useCallback(async (id) => {
    // Save current state for rollback if needed - Fix for undefined variables
    const currentConnections = [...connections];
    const currentActive = activeConnection;
    const currentDefault = defaultConnection;

    try {
      setError(null);

      // Optimistic update - remove from connections list
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

      // Make the actual API call
      await connectionsAPI.delete(id);
    } catch (err) {
      // Rollback optimistic updates on error using properly defined variables
      setConnections(currentConnections);
      setActiveConnection(currentActive);
      setDefaultConnection(currentDefault);

      setError(err.message || 'Failed to delete connection');
      throw err;
    }
  }, [activeConnection, connections, defaultConnection]);

  // Set a connection as the default
  const setAsDefaultConnection = useCallback(async (id) => {
    // Store previous state with properly defined variables
    const currentConnections = [...connections];
    const currentDefault = defaultConnection;

    try {
      setError(null);

      // Optimistic update
      const updatedConnections = connections.map(conn => ({
        ...conn,
        is_default: conn.id === id
      }));

      setConnections(updatedConnections);
      const newDefault = connections.find(conn => conn.id === id) || null;
      setDefaultConnection(newDefault);

      // Make the actual API call
      await connectionsAPI.setDefault(id);

      // No need to update state again since we've done optimistic updates

      return newDefault;
    } catch (err) {
      // Rollback optimistic updates using properly defined variables
      setConnections(currentConnections);
      setDefaultConnection(currentDefault);

      setError(err.message || 'Failed to set default connection');
      throw err;
    }
  }, [connections, defaultConnection]);

  // Test a connection configuration without saving
  const testConnection = useCallback(async (connectionData) => {
    try {
      setError(null);
      const response = await connectionsAPI.test(connectionData);
      return response;
    } catch (err) {
      setError(err.message || 'Connection test failed');
      throw err;
    }
  }, []);

  // Set active connection (for current UI session)
  const setCurrentConnection = useCallback((connection) => {
    setActiveConnection(connection);

    // Store in sessionStorage for persistence across page refreshes
    try {
      sessionStorage.setItem('activeConnectionId', connection.id);
    } catch (e) {
      console.warn('Could not save active connection to sessionStorage', e);
    }
  }, []);

  // Get a specific connection by ID
  const getConnection = useCallback(async (id) => {
    try {
      setError(null);

      // Check if it's already in our local state
      const localConnection = connections.find(c => c.id === id);
      if (localConnection) {
        return localConnection;
      }

      // If not, fetch from API
      const response = await connectionsAPI.getById(id);
      return response.connection;
    } catch (err) {
      setError(err.message || 'Failed to get connection details');
      throw err;
    }
  }, [connections]);

  // Refresh the list of connections (debounced to prevent rapid calls)
  const refreshConnections = useCallback(debounce(async () => {
    if (isRefreshing) return;

    setIsRefreshing(true);
    try {
      const response = await connectionsAPI.getAll({ forceFresh: true });
      const connectionList = response.connections || [];

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
      setIsRefreshing(false);
    }
  }, 500), [activeConnection, isRefreshing]);

  // Get Connection Dashboard (combined data)
  const getConnectionDashboard = useCallback(async (connectionId, forceFresh = false) => {
    try {
      setError(null);
      const response = await connectionsAPI.getConnectionDashboard(connectionId, { forceFresh });
      return response;
    } catch (err) {
      // Check if it's a cancelled request
      if (err.cancelled) {
        console.log('Connection dashboard request was cancelled');
        return { cancelled: true };
      }

      console.error('Error fetching connection dashboard:', err);
      setError(err.message || 'Failed to get connection dashboard');

      // Return a standardized error response instead of throwing
      return {
        error: true,
        message: err.message || 'Failed to get connection dashboard',
        details: err
      };
    }
  }, []);

  // Clear connection caches
  const clearConnectionCache = useCallback((connectionId) => {
    // Clear specific connection cache
    if (connectionId) {
      clearCacheItem(`connections.${connectionId}`);
      clearCacheItem(`connections.dashboard.${connectionId}`);
      clearCacheItem(`schema.tables.${connectionId}`);
      clearCacheItem(`metadata.status.${connectionId}`);
    } else {
      // Clear all connection-related caches
      clearCacheItem('connections.list');
    }

    // Refresh the connections list
    refreshConnections();
  }, [refreshConnections]);

  // Context value
  const value = {
    connections,
    defaultConnection,
    activeConnection,
    loading,
    error,
    isRefreshing,
    createConnection,
    updateConnection,
    deleteConnection,
    setAsDefaultConnection,
    testConnection,
    setCurrentConnection,
    getConnection,
    refreshConnections,
    getConnectionDashboard,
    clearConnectionCache
  };

  return <ConnectionContext.Provider value={value}>{children}</ConnectionContext.Provider>;
};

export default ConnectionContext;