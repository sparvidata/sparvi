// Create src/hooks/useNextRunTimes.js

import { useState, useEffect, useCallback } from 'react';
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

  // Load next run times
  const loadNextRuns = useCallback(async (forceFresh = false) => {
    if (!enabled) return;

    try {
      setError(null);

      let response;
      if (connectionId) {
        response = await automationAPI.getNextRunTimes(connectionId, { forceFresh });
      } else {
        response = await automationAPI.getAllNextRunTimes({ forceFresh });
      }

      // Handle null response
      if (!response) {
        console.warn('Received null response from next run times API');
        setNextRuns({});
        setLastUpdated(new Date());
        return;
      }

      // Handle error in response
      if (response.error) {
        throw new Error(response.error);
      }

      // Normalize the response format
      if (connectionId) {
        // Single connection response
        setNextRuns(response.next_runs || {});
      } else {
        // All connections response
        setNextRuns(response.next_runs_by_connection || {});
      }

      setLastUpdated(new Date());
    } catch (err) {
      console.error('Error loading next run times:', err);

      // Handle different error types
      const errorMessage = err?.message || 'Unknown error loading next run times';

      // If response is null, it might be a network issue
      if (err?.message?.includes('null')) {
        setError('Unable to connect to automation service');
      } else {
        setError(errorMessage);
      }

      if (onError) onError(err);

      // Set empty state on error to prevent UI crashes
      setNextRuns({});
    } finally {
      setLoading(false);
    }
  }, [connectionId, enabled, onError]);

  // Initial load
  useEffect(() => {
    loadNextRuns();
  }, [loadNextRuns]);

  // Set up refresh interval
  useEffect(() => {
    if (!enabled || !refreshInterval) return;

    const interval = setInterval(() => {
      loadNextRuns(true); // Force fresh data on interval
    }, refreshInterval);

    return () => clearInterval(interval);
  }, [loadNextRuns, refreshInterval, enabled]);

  // Refresh function for manual refresh
  const refresh = useCallback(() => {
    setLoading(true);
    return loadNextRuns(true);
  }, [loadNextRuns]);

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
    if (connectionId) {
      // Single connection
      return Object.values(nextRuns).some(run => run?.is_overdue);
    } else {
      // Multi-connection
      return Object.values(nextRuns).some(conn =>
        Object.values(conn.next_runs || {}).some(run => run?.is_overdue)
      );
    }
  }, [nextRuns, connectionId]);

  // Get count of overdue automations
  const getOverdueCount = useCallback(() => {
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
  }, [nextRuns, connectionId]);

  // Get next upcoming run across all automations
  const getNextUpcomingRun = useCallback(() => {
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