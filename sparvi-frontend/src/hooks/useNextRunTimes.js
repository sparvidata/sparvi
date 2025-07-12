import { useState, useEffect, useCallback, useRef } from 'react';
import { automationAPI } from '../api/enhancedApiService';

/**
 * Fixed hook for fetching next run times - eliminates infinite loops
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

  // Use refs to avoid dependency issues
  const intervalRef = useRef(null);
  const mountedRef = useRef(true);
  const consecutiveErrorsRef = useRef(0);
  const circuitBreakerRef = useRef(false);
  const currentConnectionIdRef = useRef(connectionId);

  // Update connection ref when it changes
  useEffect(() => {
    currentConnectionIdRef.current = connectionId;
  }, [connectionId]);

  // Stable fetch function that doesn't change on every render
  const fetchNextRuns = useCallback(async (isManualRefresh = false) => {
    const currentConnectionId = currentConnectionIdRef.current;

    console.log('[useNextRunTimes] fetchNextRuns called', {
      connectionId: currentConnectionId,
      enabled,
      circuitBreakerOpen: circuitBreakerRef.current,
      isManualRefresh
    });

    // Skip if disabled or circuit breaker is open (unless manual refresh)
    if (!enabled || (!isManualRefresh && circuitBreakerRef.current)) {
      return;
    }

    if (!mountedRef.current) {
      console.log('[useNextRunTimes] Component unmounted, skipping fetch');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      let response;

      if (currentConnectionId) {
        console.log(`[useNextRunTimes] Fetching next runs for connection: ${currentConnectionId}`);
        response = await automationAPI.getNextRunTimes(currentConnectionId, {
          forceFresh: true
        });
      } else {
        console.log('[useNextRunTimes] Fetching next runs for all connections');
        response = await automationAPI.getAllNextRunTimes({
          forceFresh: true
        });
      }

      if (!mountedRef.current) return;

      let runs = {};

      if (currentConnectionId) {
        runs = response?.next_runs || {};
      } else {
        if (response?.next_runs_by_connection) {
          runs = response.next_runs_by_connection;
        } else if (response?.schedules_by_connection) {
          runs = response.schedules_by_connection;
        }
      }

      console.log('[useNextRunTimes] Successfully fetched next runs:', runs);

      setNextRuns(runs);
      setLastFetchTime(new Date());
      setError(null);
      consecutiveErrorsRef.current = 0;
      circuitBreakerRef.current = false;

    } catch (err) {
      console.error('[useNextRunTimes] Error fetching next run times:', err);

      if (!mountedRef.current) return;

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
          console.error('[useNextRunTimes] Error in onError callback:', callbackError);
        }
      }

      // Open circuit breaker after 3 consecutive errors
      if (consecutiveErrorsRef.current >= 3) {
        console.warn('[useNextRunTimes] Opening circuit breaker due to consecutive errors');
        circuitBreakerRef.current = true;

        // Auto-reset circuit breaker after 5 minutes
        setTimeout(() => {
          if (mountedRef.current) {
            console.log('[useNextRunTimes] Auto-resetting circuit breaker');
            circuitBreakerRef.current = false;
            consecutiveErrorsRef.current = 0;
          }
        }, 5 * 60 * 1000);
      }
    } finally {
      if (mountedRef.current) {
        setLoading(false);
      }
    }
  }, [enabled, onError]); // Minimal dependencies

  // Manual refresh function
  const refresh = useCallback(async () => {
    console.log('[useNextRunTimes] Manual refresh requested');
    circuitBreakerRef.current = false;
    consecutiveErrorsRef.current = 0;
    await fetchNextRuns(true);
  }, [fetchNextRuns]);

  // Manual trigger function
  const triggerManualRun = useCallback(async (automationType) => {
    const currentConnectionId = currentConnectionIdRef.current;

    if (!currentConnectionId) {
      console.warn('[useNextRunTimes] Cannot trigger manual run without connectionId');
      return false;
    }

    try {
      console.log(`[useNextRunTimes] Triggering manual run for ${automationType} on connection ${currentConnectionId}`);
      const response = await automationAPI.triggerImmediate(currentConnectionId, automationType);

      // Refresh next runs after triggering (with delay)
      setTimeout(() => {
        if (mountedRef.current) {
          fetchNextRuns();
        }
      }, 2000);

      return response?.success || response?.result || !response?.error;
    } catch (error) {
      console.error('[useNextRunTimes] Error triggering manual run:', error);
      return false;
    }
  }, [fetchNextRuns]);

  // Single effect to handle polling - FIXED
  useEffect(() => {
    console.log('[useNextRunTimes] Setting up polling effect', {
      enabled,
      connectionId,
      refreshInterval
    });

    // Clear any existing interval
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }

    // Only start if enabled
    if (!enabled) {
      console.log('[useNextRunTimes] Polling disabled');
      return;
    }

    // Initial fetch with small delay
    const initialTimeout = setTimeout(() => {
      if (mountedRef.current) {
        fetchNextRuns();
      }
    }, 500);

    // Set up recurring interval
    intervalRef.current = setInterval(() => {
      if (mountedRef.current && !circuitBreakerRef.current) {
        fetchNextRuns();
      }
    }, refreshInterval);

    console.log('[useNextRunTimes] Polling started with interval:', refreshInterval);

    // Cleanup function
    return () => {
      console.log('[useNextRunTimes] Cleaning up polling');
      clearTimeout(initialTimeout);
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [enabled, connectionId, refreshInterval]); // Remove fetchNextRuns from dependencies!

  // Reset errors when connectionId changes
  useEffect(() => {
    setError(null);
    circuitBreakerRef.current = false;
    consecutiveErrorsRef.current = 0;
  }, [connectionId]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      console.log('[useNextRunTimes] Component unmounting');
      mountedRef.current = false;
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, []); // Empty dependency array - only run on mount/unmount

  return {
    nextRuns,
    loading,
    error,
    lastFetchTime,
    refresh,
    triggerManualRun,
    circuitBreakerOpen: circuitBreakerRef.current,
    consecutiveErrors: consecutiveErrorsRef.current
  };
};