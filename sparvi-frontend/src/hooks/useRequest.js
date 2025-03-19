import { useState, useEffect, useCallback, useRef } from 'react';
import { getRequestAbortController, requestCompleted } from '../utils/requestUtils';

/**
 * Custom hook for making API requests with automatic cancellation
 * @param {Function} requestFn - The API request function to call
 * @param {Object} options - Additional options
 * @returns {Object} Request state and execution function
 */
const useRequest = (requestFn, options = {}) => {
  const {
    executeOnMount = false,
    initialParams = null,
    onSuccess = null,
    onError = null,
    requestId = null,
  } = options;

  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Generate a unique requestId if not provided
  const generatedRequestId = useRef(`request-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`);
  const currentRequestId = requestId || generatedRequestId.current;

  // Execute request function with cancellation support
  const execute = useCallback(async (params = {}) => {
    setLoading(true);
    setError(null);

    try {
      // Get abort controller for this request
      const { signal } = getRequestAbortController(currentRequestId);

      // Execute the request with the signal
      const response = await requestFn({
        ...params,
        requestId: currentRequestId,
        signal,
      });

      setData(response);
      if (onSuccess) onSuccess(response);
      return response;
    } catch (err) {
      // Don't set error state for cancelled requests
      if (err.cancelled) return null;

      setError(err);
      if (onError) onError(err);
      return null;
    } finally {
      setLoading(false);
      requestCompleted(currentRequestId);
    }
  }, [requestFn, currentRequestId, onSuccess, onError]);

  // Execute on mount if specified
  useEffect(() => {
    if (executeOnMount && initialParams !== null) {
      execute(initialParams);
    }

    // Cleanup function to cancel request when component unmounts
    return () => {
      getRequestAbortController(currentRequestId).signal.abort();
      requestCompleted(currentRequestId);
    };
  }, [executeOnMount, initialParams, execute, currentRequestId]);

  return {
    data,
    loading,
    error,
    execute,
  };
};

export default useRequest;