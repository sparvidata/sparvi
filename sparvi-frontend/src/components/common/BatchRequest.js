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
      // Check for empty requests
      if (!requests || requests.length === 0) {
        if (isMounted) {
          const emptyResults = {};
          setResults(emptyResults);
          setLoading(false);
          if (onComplete) {
            try {
              onComplete(emptyResults);
            } catch (err) {
              console.error('Error in onComplete callback with empty results:', err);
            }
          }
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

        // Execute batch request
        let batchResults;
        try {
          batchResults = await batchRequests(requests, {
            retries: 2,
            timeout: 60000,
            waitForAuthentication: !skipAuthWait,
            signal: controller.signal
          });
        } catch (requestErr) {
          console.error('Error during batch request:', requestErr);
          batchResults = {}; // Ensure we have an object even if request fails
        }

        // Ensure we have an object, even if the result is null or undefined
        const safeResults = batchResults || {};

        if (isMounted) {
          setResults(safeResults);
          if (onComplete) {
            try {
              // Wrap in try/catch to prevent unhandled exceptions
              onComplete(safeResults);
            } catch (callbackErr) {
              console.error('Error in onComplete callback:', callbackErr);
              if (onError) onError(callbackErr);
            }
          }
        }
      } catch (err) {
        if (!isMounted) return;

        if (!err.cancelled) {
          console.error('Batch request failed:', err);
          setError(err);

          if (onError) {
            try {
              onError(err);
            } catch (errorCallbackErr) {
              console.error('Error in onError callback:', errorCallbackErr);
            }
          }

          // Always provide an empty object to ensure onComplete has valid data
          if (onComplete && isMounted) {
            try {
              onComplete({});
            } catch (completeCallbackErr) {
              console.error('Error in onComplete fallback callback:', completeCallbackErr);
            }
          }
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
        <p>Error loading data: {error.message || 'Unknown error'}</p>
      </div>
    );
  }

  // Always ensure we pass a valid object to children function
  const safeResults = results || {};
  return typeof children === 'function' ? children(safeResults) : children;
};

export default BatchRequest;