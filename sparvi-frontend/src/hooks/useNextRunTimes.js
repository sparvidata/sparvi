import { useState, useEffect, useCallback, useRef } from 'react';
import { automationAPI } from '../api/enhancedApiService';

/**
 * Hook to manage next run times for automation
 * @param {string} connectionId - Connection ID (optional, if null gets all connections)
 * @param {object} options - Hook options
 * @returns {object} Next run times data and methods
 */
export const useNextRunTimes = (connectionId = null, options = {}) => {
  const {
    refreshInterval = 60000, // 1 minute default
    enabled = true,
    onError = null
  } = options;

  const [nextRuns, setNextRuns] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);

  // Use ref to track the current request to avoid race conditions
  const currentRequestRef = useRef(null);
  const mountedRef = useRef(true);

  // Load next run times with improved error handling
  const loadNextRuns = useCallback(async (forceFresh = false) => {
    if (!enabled || !mountedRef.current) return;

    try {
      setError(null);

      // Cancel any existing request
      if (currentRequestRef.current) {
        currentRequestRef.current = null;
      }

      // Create a new request identifier
      const requestId = Date.now();
      currentRequestRef.current = requestId;

      let response;
      if (connectionId) {
        console.log(`Loading next run times for connection: ${connectionId}`);
        response = await automationAPI.getNextRunTimes(connectionId, { forceFresh });
      } else {
        console.log('Loading next run times for all connections');
        response = await automationAPI.getAllNextRunTimes({ forceFresh });
      }

      // Check if this request is still current (not cancelled)
      if (currentRequestRef.current !== requestId || !mountedRef.current) {
        console.log('Request was cancelled or component unmounted, ignoring response');
        return;
      }

      // Handle null or undefined response
      if (!response) {
        console.warn('Received null/undefined response from next run times API');
        // Don't treat this as an error - just use empty state
        setNextRuns({});
        setLastUpdated(new Date());
        return;
      }

      // Handle error in response
      if (response.error) {
        console.error('API returned error:', response.error);

        // Only set error if it's not a "not found" type error
        if (!response.error.includes('not found')) {
          throw new Error(response.error);
        } else {
          // For "not found" errors, just use empty state
          setNextRuns({});
          setLastUpdated(new Date());
          return;
        }
      }

      // Normalize the response format
      let normalizedRuns = {};

      if (connectionId) {
        // Single connection response
        normalizedRuns = response.next_runs || {};
        console.log(`Loaded next runs for connection ${connectionId}:`, normalizedRuns);
      } else {
        // All connections response
        normalizedRuns = response.next_runs_by_connection || {};
        console.log('Loaded next runs for all connections:', Object.keys(normalizedRuns));
      }

      setNextRuns(normalizedRuns);
      setLastUpdated(new Date());

    } catch (err) {
      // Only handle error if this request is still current
      if (currentRequestRef.current === null || !mountedRef.current) {
        console.log('Ignoring error from cancelled request');
        return;
      }

      console.error('Error loading next run times:', err);

      // Categorize the error
      let errorMessage = 'Unknown error loading next run times';

      if (err?.message) {
        if (err.message.includes('cancelled') || err.message.includes('aborted')) {
          // Request was cancelled - don't set error state
          console.log('Request was cancelled, not setting error state');
          return;
        } else if (err.message.includes('network') || err.message.includes('fetch')) {
          errorMessage = 'Network error - unable to connect to automation service';
        } else if (err.message.includes('timeout')) {
          errorMessage = 'Request timeout - automation service may be busy';
        } else {
          errorMessage = err.message;
        }
      }

      setError(errorMessage);
      if (onError) onError(err);

      // Set empty state on error to prevent UI crashes
      setNextRuns({});
    } finally {
      if (mountedRef.current) {
        setLoading(false);
      }
    }
  }, [connectionId, enabled, onError]);

  // Initial load with better timing
  useEffect(() => {
    if (!enabled) {
      setLoading(false);
      return;
    }

    // Small delay to let other components settle before making requests
    const initialTimeout = setTimeout(() => {
      if (mountedRef.current) {
        loadNextRuns();
      }
    }, 100);

    return () => clearTimeout(initialTimeout);
  }, [loadNextRuns, enabled]);

  // Set up refresh interval with better cleanup
  useEffect(() => {
    if (!enabled || !refreshInterval) return;

    const interval = setInterval(() => {
      if (mountedRef.current) {
        loadNextRuns(true); // Force fresh data on interval
      }
    }, refreshInterval);

    return () => clearInterval(interval);
  }, [loadNextRuns, refreshInterval, enabled]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      mountedRef.current = false;
      currentRequestRef.current = null;
    };
  }, []);

  // Refresh function for manual refresh
  const refresh = useCallback(() => {
    if (!enabled || !mountedRef.current) return Promise.resolve();

    setLoading(true);
    return loadNextRuns(true);
  }, [loadNextRuns, enabled]);

  // Get next run for specific automation type
  const getNextRun = useCallback((automationType, targetConnectionId = null) => {
    const connId = targetConnectionId || connectionId;

    if (connectionId) {
      // Single connection mode
      return nextRuns[automationType] || null;
    } else {
      // Multi-connection mode
      if (connId && nextRuns[connId]) {
        return nextRuns[connId].next_runs?.[automationType] || null;
      }
      return null;
    }
  }, [nextRuns, connectionId]);

  // Check if any automation is overdue
  const hasOverdueRuns = useCallback(() => {
    try {
      if (connectionId) {
        // Single connection
        return Object.values(nextRuns).some(run => run?.is_overdue);
      } else {
        // Multi-connection
        return Object.values(nextRuns).some(conn =>
          Object.values(conn.next_runs || {}).some(run => run?.is_overdue)
        );
      }
    } catch (err) {
      console.error('Error checking overdue runs:', err);
      return false;
    }
  }, [nextRuns, connectionId]);

  // Get count of overdue automations
  const getOverdueCount = useCallback(() => {
    try {
      let count = 0;

      if (connectionId) {
        // Single connection
        count = Object.values(nextRuns).filter(run => run?.is_overdue).length;
      } else {
        // Multi-connection
        Object.values(nextRuns).forEach(conn => {
          count += Object.values(conn.next_runs || {}).filter(run => run?.is_overdue).length;
        });
      }

      return count;
    } catch (err) {
      console.error('Error counting overdue runs:', err);
      return 0;
    }
  }, [nextRuns, connectionId]);

  // Get next upcoming run across all automations
  const getNextUpcomingRun = useCallback(() => {
    try {
      let nextRun = null;
      let earliestTime = Infinity;

      const checkRuns = (runs, connId = null) => {
        Object.entries(runs).forEach(([automationType, runInfo]) => {
          if (runInfo?.next_run_timestamp && !runInfo.is_overdue) {
            if (runInfo.next_run_timestamp < earliestTime) {
              earliestTime = runInfo.next_run_timestamp;
              nextRun = {
                ...runInfo,
                automation_type: automationType,
                connection_id: connId
              };
            }
          }
        });
      };

      if (connectionId) {
        // Single connection
        checkRuns(nextRuns);
      } else {
        // Multi-connection
        Object.entries(nextRuns).forEach(([connId, connData]) => {
          if (connData.next_runs) {
            checkRuns(connData.next_runs, connId);
          }
        });
      }

      return nextRun;
    } catch (err) {
      console.error('Error getting next upcoming run:', err);
      return null;
    }
  }, [nextRuns, connectionId]);

  return {
    nextRuns,
    loading,
    error,
    lastUpdated,
    refresh,
    getNextRun,
    hasOverdueRuns,
    getOverdueCount,
    getNextUpcomingRun
  };
};

/**
 * Simplified hook for single automation type
 * @param {string} connectionId - Connection ID
 * @param {string} automationType - Type of automation
 * @param {object} options - Hook options
 * @returns {object} Single automation next run data
 */
export const useAutomationNextRun = (connectionId, automationType, options = {}) => {
  const { nextRuns, loading, error, refresh } = useNextRunTimes(connectionId, options);

  const nextRun = nextRuns[automationType] || null;

  return {
    nextRun,
    loading,
    error,
    refresh,
    isOverdue: nextRun?.is_overdue || false,
    timeUntil: nextRun?.time_until_next || null,
    isRunning: nextRun?.currently_running || false
  };
};