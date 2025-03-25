// src/components/common/BatchRequest.js
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
    const controller = new AbortController();

    const fetchData = async () => {
      if (!requests || requests.length === 0) {
        if (isMounted) {
          const emptyResults = {};
          setResults(emptyResults);
          setLoading(false);
          if (onComplete) onComplete(emptyResults);
        }
        return;
      }

      try {
        // Wait for auth if needed
        if (!skipAuthWait) {
          await waitForAuth();
        }

        // If component is no longer mounted, don't proceed
        if (!isMounted) return;

        console.log(`Making batch request with ${requests.length} requests`);
        const batchResults = await batchRequests(requests, {
          retries: 2,
          timeout: 60000,
          waitForAuthentication: !skipAuthWait,
          signal: controller.signal
        });

        if (isMounted) {
          // Ensure we always have an object, even if the API returns null or undefined
          const safeResults = batchResults || {};
          setResults(safeResults);
          if (onComplete) onComplete(safeResults);
        }
      } catch (err) {
        if (!isMounted) return;

        console.error('Batch request failed:', err);

        if (!err.cancelled) {
          if (onError) onError(err);
          setError(err);

          // Provide an empty object to onComplete so components can handle error states gracefully
          // This prevents "Cannot read properties of undefined" errors
          if (onComplete) onComplete({});
        }
      } finally {
        if (isMounted) {
          setLoading(false);
        }
      }
    };

    fetchData();

    return () => {
      isMounted = false;
      controller.abort();
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