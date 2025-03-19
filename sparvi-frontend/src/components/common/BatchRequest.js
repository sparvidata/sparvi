import React, { useState, useEffect } from 'react';
import { batchRequests } from '../../utils/requestUtils';
import LoadingSpinner from './LoadingSpinner';

/**
 * Batch Request component to execute multiple API requests in parallel
 * @param {Object} props - Component props
 * @param {Array} props.requests - Array of request configurations
 * @param {Function} props.onComplete - Callback when all requests complete
 * @param {Function} props.onError - Callback when any request fails
 * @param {React.ReactNode} props.loadingComponent - Component to show while loading
 * @param {React.ReactNode} props.children - Function as child component receiving results
 */
const BatchRequest = ({
  requests,
  onComplete,
  onError,
  loadingComponent = <LoadingSpinner size="lg" className="mx-auto" />,
  children
}) => {
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [loadingItems, setLoadingItems] = useState({});

  // In src/components/common/BatchRequest.js
  useEffect(() => {
    let isMounted = true;

    const fetchData = async () => {
      if (!requests || requests.length === 0) {
        if (isMounted) {
          setResults({});
          setLoading(false);
        }
        return;
      }

      if (isMounted) {
        setLoading(true);
        setError(null);
      }

      try {
        const batchResults = await batchRequests(requests);
        if (isMounted) {
          setResults(batchResults);
          if (onComplete) onComplete(batchResults);
        }
      } catch (err) {
        if (isMounted) {
          setError(err);
          if (onError) onError(err);
        }
      } finally {
        if (isMounted) {
          setLoading(false);
        }
      }
    };

    try {
    setLoading(true);
    setError(null);

    // Track which items are loading
    const loadingObj = {};
    requests.forEach(req => {
      loadingObj[req.id] = true;
    });
    setLoadingItems(loadingObj);

    // Rest of your code...
  } catch (err) {
    // Error handling
  } finally {
    setLoading(false);
    setLoadingItems({});
  }

    fetchData();

    // Cleanup function
    return () => {
      isMounted = false;
    };
  }, [requests, onComplete, onError]);

  if (loading) {
    return loadingComponent;
  }

  if (error) {
    return (
      <div className="text-center text-danger-600 py-4">
        <p>Error loading data: {error.message}</p>
      </div>
    );
  }

  return typeof children === 'function' ? children(results) : children;
};

export default BatchRequest;