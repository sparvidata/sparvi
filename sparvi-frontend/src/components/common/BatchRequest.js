import React, { useState, useEffect } from 'react';
import { batchRequests } from '../../utils/requestUtils';
import LoadingSpinner from './LoadingSpinner';
import { waitForAuth } from '../../api/enhancedApiService';

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
  skipAuthWait = false,
  children
}) => {
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

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

      try {
        // Wait for auth if needed
        if (!skipAuthWait) {
          await waitForAuth();
        }

        // Ensure we're still mounted
        if (!isMounted) return;

        // Use enhanced batch request with timeout
        const batchResults = await batchRequests(requests, {
          retries: 2,
          timeout: 30000,
          waitForAuthentication: !skipAuthWait
        });

        // Safely set results
        if (isMounted) {
          setResults(batchResults);
          if (onComplete) onComplete(batchResults);
        }
      } catch (err) {
        console.error('Batch request failed:', err);

        if (isMounted) {
          // Avoid setting error for cancelled requests
          if (!err.cancelled) {
            setError(err);
            if (onError) onError(err);
          }
        }
      } finally {
        if (isMounted) {
          setLoading(false);
        }
      }
    };

    fetchData();

    // Cleanup
    return () => {
      isMounted = false;
    };
  }, [requests, onComplete, onError, skipAuthWait]);

  if (loading) {
    return loadingComponent;
  }

  if (error && !error.cancelled) {
    return (
      <div className="text-center text-danger-600 py-4">
        <p>Error loading data: {error.message}</p>
      </div>
    );
  }

  return typeof children === 'function' ? children(results || {}) : children;
};

export default BatchRequest;