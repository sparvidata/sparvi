import { useState, useEffect, useCallback, useRef } from 'react';
import { automationAPI } from '../api/enhancedApiService';

/**
 * Hook to manage next run times for automation - FIXED VERSION
 * @param {string} connectionId - Connection ID (optional, if null gets all connections)
 * @param {object} options - Hook options
 * @returns {object} Next run times data and methods
 */
export const useNextRunTimes = (connectionId = null, options = {}) => {
  const {
    refreshInterval = 120000, // Increased to 2 minutes to reduce load
    enabled = true,
    onError = null
  } = options;

  const [nextRuns, setNextRuns] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);

  // Use refs to track state and prevent race conditions
  const mountedRef = useRef(true);
  const currentRequestRef = useRef(null);
  const refreshTimeoutRef = useRef(null);
  const lastSuccessfulFetchRef = useRef(0);
  const failureCountRef = useRef(0);

  // Minimum time between requests (prevent spam)
  const MIN_REQUEST_INTERVAL = 30000; // 30 seconds

  // Load next run times with improved error handling and rate limiting
  const loadNextRuns = useCallback(async (forceFresh = false) => {
    if (!enabled || !mountedRef.current) {
      return;
    }

    // Rate limiting - don't make requests too frequently
    const now = Date.now();
    const timeSinceLastFetch = now - lastSuccessfulFetchRef.current;

    if (!forceFresh && timeSinceLastFetch < MIN_REQUEST_INTERVAL) {
      console.log(`Skipping request - too soon since last fetch (${timeSinceLastFetch}ms < ${MIN_REQUEST_INTERVAL}ms)`);
      return;
    }

    // Cancel any existing request
    if (currentRequestRef.current) {
      currentRequestRef.current = null;
    }

    // Create a new request identifier
    const requestId = `${connectionId || 'all'}-${Date.now()}`;
    currentRequestRef.current = requestId;

    try {
      setError(null);

      console.log(`Loading next run times for ${connectionId ? `connection: ${connectionId}` : 'all connections'}`);

      let response;
      if (connectionId) {
        response = await automationAPI.getNextRunTimes(connectionId, {
          forceFresh,
          requestId: `next-runs-${connectionId}`
        });
      } else {
        response = await automationAPI.getAllNextRunTimes({
          forceFresh,
          requestId: 'next-runs-all'
        });
      }

      // Check if this request is still current
      if (currentRequestRef.current !== requestId || !mountedRef.current) {
        console.log('Request was cancelled or component unmounted, ignoring response');
        return;
      }

      // Handle null or undefined response
      if (!response) {
        console.warn('Received null/undefined response from next run times API');
        setNextRuns({});
        setLastUpdated(new Date());
        lastSuccessfulFetchRef.current = now;
        return;
      }

      // Handle error in response
      if (response.error) {
        console.error('API returned error:', response.error);

        // Only treat as real error if it's not a "not found" type
        if (!response.error.toLowerCase().includes('not found') &&
            !response.error.toLowerCase().includes('no automation')) {
          throw new Error(response.error);
        } else {
          setNextRuns({});
          setLastUpdated(new Date());
          lastSuccessfulFetchRef.current = now;
          return;
        }
      }

      // Normalize the response format
      let normalizedRuns = {};

      if (connectionId) {
        // Single connection response
        normalizedRuns = response.next_runs || {};
        console.log(`Loaded next runs for connection ${connectionId}:`, Object.keys(normalizedRuns).length, 'automation types');
      } else {
        // All connections response
        normalizedRuns = response.next_runs_by_connection || {};
        console.log('Loaded next runs for all connections:', Object.keys(normalizedRuns).length, 'connections');
      }

      setNextRuns(normalizedRuns);
      setLastUpdated(new Date());
      lastSuccessfulFetchRef.current = now;
      failureCountRef.current = 0; // Reset failure count on success

    } catch (err) {
      // Only handle error if this request is still current
      if (currentRequestRef.current !== requestId || !mountedRef.current) {
        console.log('Ignoring error from cancelled request');
        return;
      }

      failureCountRef.current += 1;
      console.error('Error loading next run times (attempt', failureCountRef.current, '):', err);

      // Categorize the error
      let errorMessage = 'Unknown error loading next run times';

      if (err?.cancelled || err?.name === 'CanceledError') {
        // Request was cancelled - don't set error state
        console.log('Request was cancelled, not setting error state');
        return;
      } else if (err?.message) {
        if (err.message.includes('network') || err.message.includes('fetch')) {
          errorMessage = 'Network error - unable to connect to automation service';
        } else if (err.message.includes('timeout')) {
          errorMessage = 'Request timeout - automation service may be busy';
        } else {
          errorMessage = err.message;
        }
      }

      // Only set error state if we've had multiple failures
      if (failureCountRef.current >= 2) {
        setError(errorMessage);
        if (onError) onError(err);
      }

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

    // Clear any existing timeout
    if (refreshTimeoutRef.current) {
      clearTimeout(refreshTimeoutRef.current);
    }

    // Small delay to let other components settle before making requests
    refreshTimeoutRef.current = setTimeout(() => {
      if (mountedRef.current) {
        loadNextRuns();
      }
    }, 1000); // Increased delay to 1 second

    return () => {
      if (refreshTimeoutRef.current) {
        clearTimeout(refreshTimeoutRef.current);
      }
    };
  }, [loadNextRuns, enabled]);

  // Set up refresh interval with better cleanup and exponential backoff
  useEffect(() => {
    if (!enabled || !refreshInterval) return;

    const startInterval = () => {
      // Clear any existing interval
      if (refreshTimeoutRef.current) {
        clearTimeout(refreshTimeoutRef.current);
      }

      const scheduleNext = () => {
        if (!mountedRef.current) return;

        // Calculate delay with exponential backoff on failures
        let delay = refreshInterval;
        if (failureCountRef.current > 0) {
          delay = Math.min(refreshInterval * Math.pow(2, failureCountRef.current - 1), 300000); // Max 5 minutes
          console.log(`Using exponential backoff: ${delay}ms due to ${failureCountRef.current} failures`);
        }

        refreshTimeoutRef.current = setTimeout(() => {
          if (mountedRef.current) {
            loadNextRuns(true).finally(() => {
              scheduleNext(); // Schedule the next refresh
            });
          }
        }, delay);
      };

      scheduleNext();
    };

    // Start the interval after initial load
    const initialTimer = setTimeout(startInterval, 5000); // Wait 5 seconds after mount

    return () => {
      clearTimeout(initialTimer);
      if (refreshTimeoutRef.current) {
        clearTimeout(refreshTimeoutRef.current);
      }
    };
  }, [loadNextRuns, refreshInterval, enabled]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      mountedRef.current = false;
      currentRequestRef.current = null;
      if (refreshTimeoutRef.current) {
        clearTimeout(refreshTimeoutRef.current);
      }
    };
  }, []);

  // Refresh function for manual refresh
  const refresh = useCallback(async () => {
    if (!enabled || !mountedRef.current) return Promise.resolve();

    setLoading(true);
    setError(null);
    failureCountRef.current = 0; // Reset failure count on manual refresh
    return loadNextRuns(true);
  }, [loadNextRuns, enabled]);

  // Get next run for specific automation type
  const getNextRun = useCallback((automationType, targetConnectionId = null) => {
    const connId = targetConnectionId || connectionId;

    try {
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
    } catch (err) {
      console.error('Error in getNextRun:', err);
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
  const { nextRuns, loading, error, refresh } = useNextRunTimes(connectionId, {
    ...options,
    refreshInterval: options.refreshInterval || 180000 // 3 minutes for single automation
  });

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