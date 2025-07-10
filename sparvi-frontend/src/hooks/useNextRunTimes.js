import { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { automationAPI } from '../api/enhancedApiService';

/**
 * Enhanced hook for fetching next run times with proper error handling and loop prevention
 */
export const useNextRunTimes = (connectionId, options = {}) => {
  const {
    refreshInterval = 60000, // 1 minute default
    enabled = true,
    onError = null
  } = options;

  // Stabilize the options object to prevent dependency changes
  const stableOptions = useMemo(() => ({
    refreshInterval,
    enabled,
    onError
  }), [refreshInterval, enabled, onError]);

  const [nextRuns, setNextRuns] = useState({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [lastFetchTime, setLastFetchTime] = useState(null);

  // Refs for cleanup and control
  const intervalRef = useRef(null);
  const consecutiveErrorsRef = useRef(0);
  const isComponentMountedRef = useRef(true);
  const lastRequestIdRef = useRef(null);
  const requestInProgressRef = useRef(false);

  // Circuit breaker state
  const [circuitBreakerOpen, setCircuitBreakerOpen] = useState(false);
  const circuitBreakerTimeoutRef = useRef(null);

  // Create a stable request ID based on connectionId
  const requestId = useMemo(() => {
    return connectionId
      ? `next-runs-${connectionId}`
      : 'next-runs-all';
  }, [connectionId]);

  /**
   * Reset circuit breaker after timeout
   */
  const resetCircuitBreaker = useCallback(() => {
    console.log('Resetting circuit breaker for next runs');
    setCircuitBreakerOpen(false);
    consecutiveErrorsRef.current = 0;
  }, []);

  /**
   * Open circuit breaker to prevent further requests
   */
  const openCircuitBreaker = useCallback(() => {
    console.log('Opening circuit breaker for next runs - too many consecutive errors');
    setCircuitBreakerOpen(true);

    // Clear any existing timeout
    if (circuitBreakerTimeoutRef.current) {
      clearTimeout(circuitBreakerTimeoutRef.current);
    }

    // Set timeout to reset circuit breaker after 5 minutes
    circuitBreakerTimeoutRef.current = setTimeout(resetCircuitBreaker, 5 * 60 * 1000);
  }, [resetCircuitBreaker]);

  /**
   * Fetch next run times with proper error handling and deduplication
   */
  const fetchNextRuns = useCallback(async (isRetry = false) => {
    // Prevent multiple simultaneous requests
    if (requestInProgressRef.current && !isRetry) {
      console.log('Request already in progress, skipping');
      return;
    }

    // Check if enabled
    if (!stableOptions.enabled) {
      console.log('Hook disabled, skipping fetch');
      return;
    }

    // Check circuit breaker
    if (circuitBreakerOpen) {
      console.log('Circuit breaker open, skipping fetch');
      return;
    }

    // Set loading state only if not retrying
    if (!isRetry && isComponentMountedRef.current) {
      setLoading(true);
      setError(null);
    }

    requestInProgressRef.current = true;
    lastRequestIdRef.current = requestId;

    try {
      let response;

      if (connectionId) {
        // Fetch for specific connection
        console.log(`Fetching next runs for connection: ${connectionId}`);
        response = await automationAPI.getNextRunTimes(connectionId, {
          forceFresh: true,
          requestId: `${requestId}-${Date.now()}`
        });
      } else {
        // Fetch for all connections
        console.log('Fetching next runs for all connections');
        response = await automationAPI.getAllNextRunTimes({
          forceFresh: true,
          requestId: `${requestId}-${Date.now()}`
        });
      }

      // Only update state if component is still mounted and this is the latest request
      if (isComponentMountedRef.current && lastRequestIdRef.current === requestId) {
        let runs = {};

        if (connectionId) {
          // For single connection, extract next_runs from response
          runs = response?.next_runs || {};
          console.log(`Received next runs for ${connectionId}:`, runs);
        } else {
          // For all connections, handle the response structure
          if (response?.next_runs_by_connection) {
            runs = response.next_runs_by_connection;
          } else if (response?.schedules_by_connection) {
            runs = response.schedules_by_connection;
          } else {
            runs = {};
          }
          console.log('Received next runs for all connections:', runs);
        }

        setNextRuns(runs);
        setLastFetchTime(new Date());
        setError(null);
        consecutiveErrorsRef.current = 0;

        // Reset circuit breaker on success
        if (circuitBreakerOpen) {
          resetCircuitBreaker();
        }
      }

    } catch (err) {
      console.error('Error fetching next run times:', err);

      // Only update state if component is still mounted
      if (isComponentMountedRef.current) {
        consecutiveErrorsRef.current += 1;

        const errorInfo = {
          message: err.message || 'Failed to fetch next run times',
          type: 'api_error',
          status: err.status,
          consecutiveErrors: consecutiveErrorsRef.current
        };

        setError(errorInfo);

        // Call error handler if provided
        if (stableOptions.onError) {
          try {
            stableOptions.onError(errorInfo);
          } catch (callbackError) {
            console.error('Error in onError callback:', callbackError);
          }
        }

        // Open circuit breaker if too many consecutive errors
        if (consecutiveErrorsRef.current >= 3) {
          openCircuitBreaker();
          clearPolling(); // Stop polling when circuit breaker opens
        }
      }
    } finally {
      requestInProgressRef.current = false;

      if (isComponentMountedRef.current) {
        setLoading(false);
      }
    }
  }, [connectionId, stableOptions, circuitBreakerOpen, requestId, resetCircuitBreaker, openCircuitBreaker]);

  /**
   * Start polling for next run times
   */
  const startPolling = useCallback(() => {
    // Don't start if already polling or disabled
    if (intervalRef.current || !stableOptions.enabled || circuitBreakerOpen) {
      return;
    }

    console.log(`Starting polling for next runs (interval: ${stableOptions.refreshInterval}ms)`);

    // Initial fetch with a small delay to prevent immediate loops
    setTimeout(() => {
      if (isComponentMountedRef.current && !requestInProgressRef.current) {
        fetchNextRuns();
      }
    }, 100);

    // Set up interval
    intervalRef.current = setInterval(() => {
      if (isComponentMountedRef.current && !requestInProgressRef.current && !circuitBreakerOpen) {
        fetchNextRuns();
      }
    }, stableOptions.refreshInterval);
  }, [fetchNextRuns, stableOptions.enabled, stableOptions.refreshInterval, circuitBreakerOpen]);

  /**
   * Stop polling
   */
  const clearPolling = useCallback(() => {
    if (intervalRef.current) {
      console.log('Stopping polling for next runs');
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
  }, []);

  /**
   * Manual refresh function
   */
  const refresh = useCallback(async () => {
    console.log('Manual refresh requested for next runs');

    // Clear polling first
    clearPolling();

    // Reset circuit breaker and error state
    consecutiveErrorsRef.current = 0;
    setCircuitBreakerOpen(false);
    setError(null);

    // Wait a moment then fetch and restart polling
    setTimeout(() => {
      if (isComponentMountedRef.current) {
        fetchNextRuns(true).then(() => {
          if (isComponentMountedRef.current) {
            startPolling();
          }
        });
      }
    }, 100);
  }, [fetchNextRuns, startPolling, clearPolling]);

  /**
   * Manual trigger for automation
   */
  const triggerManualRun = useCallback(async (automationType) => {
    if (!connectionId) {
      console.warn('Cannot trigger manual run without connectionId');
      return false;
    }

    try {
      console.log(`Triggering manual run for ${automationType} on connection ${connectionId}`);
      const response = await automationAPI.triggerImmediate(connectionId, automationType);

      // Refresh next runs after triggering (with delay)
      setTimeout(() => {
        if (isComponentMountedRef.current) {
          fetchNextRuns(true);
        }
      }, 2000);

      return response?.success || response?.result || !response?.error;
    } catch (error) {
      console.error('Error triggering manual run:', error);
      return false;
    }
  }, [connectionId, fetchNextRuns]);

  /**
   * Effect to start/stop polling based on enabled state
   */
  useEffect(() => {
    if (stableOptions.enabled && !circuitBreakerOpen) {
      startPolling();
    } else {
      clearPolling();
    }

    // Cleanup on dependency change
    return () => {
      clearPolling();
    };
  }, [stableOptions.enabled, circuitBreakerOpen, startPolling, clearPolling]);

  /**
   * Cleanup on unmount
   */
  useEffect(() => {
    return () => {
      console.log('useNextRunTimes cleanup');
      isComponentMountedRef.current = false;
      clearPolling();

      // Clear circuit breaker timeout
      if (circuitBreakerTimeoutRef.current) {
        clearTimeout(circuitBreakerTimeoutRef.current);
      }
    };
  }, [clearPolling]);

  /**
   * Reset errors when connectionId changes
   */
  useEffect(() => {
    setError(null);
    setCircuitBreakerOpen(false);
    consecutiveErrorsRef.current = 0;

    // Clear any existing circuit breaker timeout
    if (circuitBreakerTimeoutRef.current) {
      clearTimeout(circuitBreakerTimeoutRef.current);
    }
  }, [connectionId]);

  return {
    nextRuns,
    loading,
    error,
    lastFetchTime,
    refresh,
    triggerManualRun,
    circuitBreakerOpen,
    consecutiveErrors: consecutiveErrorsRef.current
  };
};

// Export alias for backwards compatibility
export const useAutomationNextRun = useNextRunTimes;