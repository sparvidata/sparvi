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
    let ignoreCancel = false;

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

        // Check if we're still mounted before proceeding
        if (!isMounted) return;

        // Use enhanced batch request with timeout
        const batchResults = await batchRequests(requests, {
          retries: 2,
          timeout: 30000,
          waitForAuthentication: !skipAuthWait
        });

        // Safely set results if still mounted
        if (isMounted) {
          setResults(batchResults);
          if (onComplete) onComplete(batchResults);
        }
      } catch (err) {
        // Don't handle further if unmounted
        if (!isMounted) return;

        // Only handle non-cancellation errors or if ignoreCancel is true
        // ignoreCancel flag should be set to true only once
        if (!err.cancelled || ignoreCancel) {
          console.error('Batch request failed:', err);

          if (onError) onError(err);
          setError(err);
        }
      } finally {
        if (isMounted) {
          setLoading(false);
        }
      }
    };

    // Set ignoreCancel to true after a brief delay
    // This will allow one retry after a cancellation but prevent a loop
    const timeoutId = setTimeout(() => {
      ignoreCancel = true;
    }, 200);

    fetchData();

    return () => {
      isMounted = false;
      clearTimeout(timeoutId);
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