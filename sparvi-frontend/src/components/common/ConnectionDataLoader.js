import React, { useState, useEffect } from 'react';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { apiRequest } from '../../utils/apiUtils';
import LoadingSpinner from './LoadingSpinner';

/**
 * Component that handles loading data that depends on the current connection
 * One endpoint at a time - no batching
 */
const ConnectionDataLoader = ({
  endpoint,
  params = {},
  loadingComponent = <LoadingSpinner size="lg" className="mx-auto my-8" />,
  errorComponent = null,
  noConnectionComponent = null,
  children
}) => {
  const { activeConnection, loading: connectionLoading } = useConnection();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    // Don't do anything if connection is still loading or not available
    if (connectionLoading || !activeConnection) {
      return;
    }

    // Skip if no endpoint
    if (!endpoint) {
      setData(null);
      setLoading(false);
      return;
    }

    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);

        // Create the endpoint with connection ID if needed
        const formattedEndpoint = endpoint.replace(':connection_id', activeConnection.id);

        // Add connection_id to params if not included in the endpoint
        const requestParams = {
          ...params
        };

        if (!endpoint.includes(':connection_id') && !params.connection_id) {
          requestParams.connection_id = activeConnection.id;
        }

        // Make the request
        const response = await apiRequest(formattedEndpoint, {
          params: requestParams,
          timeout: 60000 // 60 second timeout
        });

        setData(response);
      } catch (err) {
        console.error(`Error loading data from ${endpoint}:`, err);
        setError(err);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [activeConnection, connectionLoading, endpoint, params]);

  // If still loading connection
  if (connectionLoading) {
    return loadingComponent;
  }

  // If no connection available
  if (!activeConnection) {
    return noConnectionComponent || (
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

export default ConnectionDataLoader;