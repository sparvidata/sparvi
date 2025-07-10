import { useState, useEffect, useCallback, useRef } from 'react';
import { API } from '../services/apiService';

/**
 * Enhanced hook for fetching next run times with circuit breaker protection
 */
export const useNextRunTimes = (scheduleId, options = {}) => {
  const {
    refreshInterval = 30000, // 30 seconds default
    maxRetries = 3,
    enabled = true
  } = options;

  const [nextRuns, setNextRuns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [lastFetchTime, setLastFetchTime] = useState(null);

  // Refs for cleanup and control
  const abortControllerRef = useRef(null);
  const intervalRef = useRef(null);
  const consecutiveErrorsRef = useRef(0);
  const isComponentMountedRef = useRef(true);

  // Circuit breaker state
  const [circuitBreakerOpen, setCircuitBreakerOpen] = useState(false);
  const [nextAttemptTime, setNextAttemptTime] = useState(null);

  /**
   * Fetch next run times with error handling
   */
  const fetchNextRuns = useCallback(async (isRetry = false) => {
    if (!scheduleId || !enabled) return;

    // Check if circuit breaker is open
    if (circuitBreakerOpen && Date.now() < nextAttemptTime) {
      return;
    }

    // Abort any ongoing request
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    // Create new abort controller
    abortControllerRef.current = new AbortController();

    if (!isRetry) {
      setLoading(true);
      setError(null);
    }

    try {
      const response = await API.get(
        `/api/automation/schedules/${scheduleId}/next-runs`,
        {},
        {
          signal: abortControllerRef.current.signal,
          timeout: 10000 // 10 second timeout
        }
      );

      // Only update state if component is still mounted
      if (isComponentMountedRef.current) {
        setNextRuns(response.next_runs || []);
        setLastFetchTime(new Date());
        setError(null);
        setCircuitBreakerOpen(false);
        setNextAttemptTime(null);
        consecutiveErrorsRef.current = 0;
      }

    } catch (err) {
      if (err.name === 'AbortError') {
        // Request was cancelled, don't treat as error
        return;
      }

      // Only update state if component is still mounted
      if (isComponentMountedRef.current) {
        consecutiveErrorsRef.current += 1;

        // Handle circuit breaker errors
        if (err.circuitBreakerOpen) {
          setCircuitBreakerOpen(true);
          setNextAttemptTime(err.nextAttemptTime);
          setError({
            message: 'Service temporarily unavailable. Please try again later.',
            type: 'circuit_breaker',
            nextAttemptTime: err.nextAttemptTime
          });
        } else {
          setError({
            message: err.message || 'Failed to fetch next run times',
            type: 'api_error',
            status: err.status,
            consecutiveErrors: consecutiveErrorsRef.current
          });
        }

        // If too many consecutive errors, stop polling temporarily
        if (consecutiveErrorsRef.current >= 5) {
          console.warn(`Too many consecutive errors (${consecutiveErrorsRef.current}), stopping polling for schedule ${scheduleId}`);
          clearPolling();
        }
      }

      console.error('Error fetching next run times:', err);
    } finally {
      if (isComponentMountedRef.current) {
        setLoading(false);
      }
      abortControllerRef.current = null;
    }
  }, [scheduleId, enabled, circuitBreakerOpen, nextAttemptTime]);

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

    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      abortControllerRef.current = null;
    }
  }, []);

  /**
   * Manual refresh function
   */
  const refresh = useCallback(async () => {
    if (circuitBreakerOpen && Date.now() < nextAttemptTime) {
      return;
    }

    clearPolling();
    consecutiveErrorsRef.current = 0;
    setCircuitBreakerOpen(false);
    setNextAttemptTime(null);
    
    // Try to reset the circuit breaker for this endpoint
    API.resetCircuitBreaker(`/api/automation/schedules/${scheduleId}/next-runs`);
    
    await fetchNextRuns();
    startPolling();
  }, [scheduleId, fetchNextRuns, startPolling, clearPolling, circuitBreakerOpen, nextAttemptTime]);

  /**
   * Effect to start/stop polling based on scheduleId and enabled
   */
  useEffect(() => {
    if (scheduleId && enabled) {
      startPolling();
    } else {
      clearPolling();
    }

    return () => {
      clearPolling();
    };
  }, [scheduleId, enabled, startPolling, clearPolling]);

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
   * Reset errors and circuit breaker when scheduleId changes
   */
  useEffect(() => {
    setError(null);
    setCircuitBreakerOpen(false);
    setNextAttemptTime(null);
    consecutiveErrorsRef.current = 0;
  }, [scheduleId]);

  return {
    nextRuns,
    loading,
    error,
    lastFetchTime,
    refresh,
    circuitBreakerOpen,
    nextAttemptTime: nextAttemptTime ? new Date(nextAttemptTime) : null,
    consecutiveErrors: consecutiveErrorsRef.current
  };
};