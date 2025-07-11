import { useState, useEffect, useCallback, useRef } from 'react';
import { automationAPI } from '../api/enhancedApiService';

/**
 * Simplified hook for fetching next run times - no infinite loops
 */
export const useNextRunTimes = (connectionId, options = {}) => {
  const {
    refreshInterval = 60000, // 1 minute default
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
  const isMountedRef = useRef(true);
  const requestInProgressRef = useRef(false);

  // Circuit breaker state
  const [circuitBreakerOpen, setCircuitBreakerOpen] = useState(false);

  /**
   * Fetch next run times - simplified version
   */
  const fetchNextRuns = useCallback(async () => {
    // Prevent multiple simultaneous requests
    if (requestInProgressRef.current || !enabled || circuitBreakerOpen) {
      return;
    }

    requestInProgressRef.current = true;
    setLoading(true);
    setError(null);

    try {
      let response;

      if (connectionId) {
        console.log(`Fetching next runs for connection: ${connectionId}`);
        response = await automationAPI.getNextRunTimes(connectionId, {
          forceFresh: true,
          requestId: `next-runs-${connectionId}-${Date.now()}`
        });
      } else {
        console.log('Fetching next runs for all connections');
        response = await automationAPI.getAllNextRunTimes({
          forceFresh: true,
          requestId: `next-runs-all-${Date.now()}`
        });
      }

      // Only update state if component is still mounted
      if (isMountedRef.current) {
        let runs = {};

        if (connectionId) {
          runs = response?.next_runs || {};
        } else {
          if (response?.next_runs_by_connection) {
            runs = response.next_runs_by_connection;
          } else if (response?.schedules_by_connection) {
            runs = response.schedules_by_connection;
          }
        }

        setNextRuns(runs);
        setLastFetchTime(new Date());
        setError(null);
        consecutiveErrorsRef.current = 0;
        setCircuitBreakerOpen(false);
      }

    } catch (err) {
      console.error('Error fetching next run times:', err);

      if (isMountedRef.current) {
        consecutiveErrorsRef.current += 1;

        const errorInfo = {
          message: err.message || 'Failed to fetch next run times',
          type: 'api_error',
          consecutiveErrors: consecutiveErrorsRef.current
        };

        setError(errorInfo);

        if (onError) {
          try {
            onError(errorInfo);
          } catch (callbackError) {
            console.error('Error in onError callback:', callbackError);
          }
        }

        // Open circuit breaker if too many consecutive errors
        if (consecutiveErrorsRef.current >= 3) {
          console.warn('Opening circuit breaker due to consecutive errors');
          setCircuitBreakerOpen(true);

          // Auto-reset circuit breaker after 5 minutes
          setTimeout(() => {
            if (isMountedRef.current) {
              console.log('Auto-resetting circuit breaker');
              setCircuitBreakerOpen(false);
              consecutiveErrorsRef.current = 0;
            }
          }, 5 * 60 * 1000);
        }
      }
    } finally {
      requestInProgressRef.current = false;
      if (isMountedRef.current) {
        setLoading(false);
      }
    }
  }, [connectionId, enabled, circuitBreakerOpen, onError]);

  /**
   * Manual refresh function
   */
  const refresh = useCallback(async () => {
    console.log('Manual refresh requested');

    // Clear circuit breaker and errors
    setCircuitBreakerOpen(false);
    consecutiveErrorsRef.current = 0;
    setError(null);

    // Fetch immediately
    await fetchNextRuns();
  }, [fetchNextRuns]);

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
        if (isMountedRef.current) {
          fetchNextRuns();
        }
      }, 2000);

      return response?.success || response?.result || !response?.error;
    } catch (error) {
      console.error('Error triggering manual run:', error);
      return false;
    }
  }, [connectionId, fetchNextRuns]);

  // Single effect to manage polling - SIMPLIFIED
  useEffect(() => {
    console.log('useNextRunTimes effect triggered', {
      enabled,
      circuitBreakerOpen,
      connectionId: connectionId || 'all'
    });

    // Clear any existing interval
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }

    // Only start polling if enabled and circuit breaker is closed
    if (enabled && !circuitBreakerOpen) {
      console.log('Starting polling for next runs');

      // Initial fetch with small delay
      const initialTimeout = setTimeout(() => {
        if (isMountedRef.current && !requestInProgressRef.current) {
          fetchNextRuns();
        }
      }, 500);

      // Set up interval
      intervalRef.current = setInterval(() => {
        if (isMountedRef.current && !requestInProgressRef.current) {
          fetchNextRuns();
        }
      }, refreshInterval);

      // Cleanup function
      return () => {
        clearTimeout(initialTimeout);
        if (intervalRef.current) {
          clearInterval(intervalRef.current);
          intervalRef.current = null;
        }
      };
    }

    // If not enabled or circuit breaker is open, no cleanup needed
    return () => {};
  }, [enabled, circuitBreakerOpen, refreshInterval, fetchNextRuns, connectionId]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      console.log('useNextRunTimes cleanup on unmount');
      isMountedRef.current = false;

      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, []); // Empty dependency array - only run on mount/unmount

  // Reset errors when connectionId changes
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