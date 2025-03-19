import React, { useState, useEffect } from 'react';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import LoadingSpinner from './LoadingSpinner';
import { loadConnectionData } from '../../utils/batchUtils';

/**
 * Component that handles loading data that depends on the current connection
 * Loads connection first, then loads dependent data once connection is available
 */
const ConnectionLoader = ({
  requests = [],
  loadingComponent = <LoadingSpinner size="lg" className="mx-auto" />,
  errorComponent = null,
  emptyConnectionComponent = null,
  children
}) => {
  const { activeConnection, loading: connectionLoading } = useConnection();
  const [data, setData] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    // Wait for connection to be ready
    if (connectionLoading || !activeConnection) {
      return;
    }

    // Skip if there are no requests
    if (requests.length === 0) {
      setData({});
      setLoading(false);
      return;
    }

    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);

        // Load data using the active connection
        const results = await loadConnectionData(
          activeConnection.id,
          requests
        );

        setData(results);
      } catch (err) {
        console.error('Error loading connection data:', err);
        setError(err);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [activeConnection, connectionLoading, requests]);

  // If still loading connection
  if (connectionLoading) {
    return loadingComponent;
  }

  // If no connection available
  if (!activeConnection) {
    return emptyConnectionComponent || (
      <div className="text-center py-10">
        <p className="text-secondary-500">Please select a connection to view data.</p>
      </div>
    );
  }

  // If loading dependent data
  if (loading) {
    return loadingComponent;
  }

  // If there's an error
  if (error && errorComponent) {
    return errorComponent;
  }

  // Render children with the data
  return typeof children === 'function' ? children(data) : children;
};

export default ConnectionLoader;