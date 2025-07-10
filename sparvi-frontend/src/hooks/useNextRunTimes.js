import { useState, useEffect, useCallback, useRef } from 'react';
import { automationAPI } from '../api/enhancedApiService';

/**
 * Enhanced hook for fetching next run times with error handling and retries
 */
export const useNextRunTimes = (connectionId, options = {}) => {
  const {
    refreshInterval = 60000, // 1 minute default for next runs
    enabled = true,
    onError = null
  } = options;

  const [nextRuns, setNextRuns] = useState({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [lastFetchTime, setLastFetchTime] = useState(null);

  // Refs for cleanup and control
  const intervalRef = useRef(null);
  const consecutiveErrorsRef = useRef(0);
  const isComponentMountedRef = useRef(true);

  // Circuit breaker state
  const [circuitBreakerOpen, setCircuitBreakerOpen] = useState(false);

  /**
   * Fetch next run times with error handling
   */
  const fetchNextRuns = useCallback(async (isRetry = false) => {
    if (!enabled) return;

    if (!isRetry) {
      setLoading(true);
      setError(null);
    }

    try {
      let response;

      if (connectionId) {
        // Fetch for specific connection with force fresh
        response = await automationAPI.getNextRunTimes(connectionId, { forceFresh: true });
      } else {
        // Fetch for all connections with force fresh
        response = await automationAPI.getAllNextRunTimes({ forceFresh: true });
      }

      // Only update state if component is still mounted
      if (isComponentMountedRef.current) {
        console.log('Next runs API response:', response); // Temp Debugging - Delete Later

        if (connectionId) {
          // For single connection, extract next_runs from response
          const runs = response?.next_runs || {};
          console.log(`Setting next runs for connection ${connectionId}:`, runs); // Temp Debugging - Delete Later
          setNextRuns(runs);
        } else {
          // For all connections, use next_runs_by_connection
          const allRuns = response?.next_runs_by_connection || {};
          console.log('Setting all next runs:', allRuns); // Temp Debugging - Delete Later
          setNextRuns(allRuns);
        }

        setLastFetchTime(new Date());
        setError(null);
        setCircuitBreakerOpen(false);
        consecutiveErrorsRef.current = 0;
      }

    } catch (err) {
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
        if (onError) {
          onError(errorInfo);
        }

        // If too many consecutive errors, stop polling temporarily
        if (consecutiveErrorsRef.current >= 5) {
          console.warn(`Too many consecutive errors (${consecutiveErrorsRef.current}), stopping polling for next runs`);
          clearPolling();
        }
      }

      console.error('Error fetching next run times:', err);
    } finally {
      if (isComponentMountedRef.current) {
        setLoading(false);
      }
    }
  }, [connectionId, enabled, onError]);

  /**
   * Start polling for next run times
   */
  const startPolling = useCallback(() => {
    if (!enabled || intervalRef.current) return;

    // Initial fetch
    fetchNextRuns();

    // Set up interval
    intervalRef.current = setInterval(() => {
      fetchNextRuns();
    }, refreshInterval);
  }, [fetchNextRuns, refreshInterval, enabled]);

  /**
   * Stop polling
   */
  const clearPolling = useCallback(() => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
  }, []);

  /**
   * Manual refresh function
   */
  const refresh = useCallback(async () => {
    clearPolling();
    consecutiveErrorsRef.current = 0;
    setCircuitBreakerOpen(false);

    await fetchNextRuns();
    startPolling();
  }, [fetchNextRuns, startPolling, clearPolling]);

  /**
   * Manual trigger for automation
   */
  const triggerManualRun = useCallback(async (automationType) => {
    if (!connectionId) return false;

    try {
      const response = await automationAPI.triggerImmediate(connectionId, automationType);

      // Refresh next runs after triggering
      setTimeout(() => {
        fetchNextRuns();
      }, 2000); // Wait 2 seconds for the job to start

      return response?.success || response?.result || !response?.error;
    } catch (error) {
      console.error('Error triggering manual run:', error);
      return false;
    }
  }, [connectionId, fetchNextRuns]);

  /**
   * Effect to start/stop polling based on connectionId and enabled
   */
  useEffect(() => {
    if (enabled) {
      startPolling();
    } else {
      clearPolling();
    }

    return () => {
      clearPolling();
    };
  }, [enabled, startPolling, clearPolling]);

  /**
   * Cleanup on unmount
   */
  useEffect(() => {
    return () => {
      isComponentMountedRef.current = false;
      clearPolling();
    };
  }, [clearPolling]);

  /**
   * Reset errors when connectionId changes
   */
  useEffect(() => {
    setError(null);
    setCircuitBreakerOpen(false);
    consecutiveErrorsRef.current = 0;
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