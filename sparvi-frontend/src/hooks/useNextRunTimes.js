import { useState, useEffect, useCallback, useRef } from 'react';
import { automationAPI } from '../api/enhancedApiService';

export const useNextRunTimes = (connectionId = null, options = {}) => {
  const {
    enabled = true,
    refreshInterval = 60000, // 1 minute default
    onError = null
  } = options;

  const [nextRuns, setNextRuns] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const intervalRef = useRef(null);

  const loadNextRuns = useCallback(async (forceFresh = false) => {
    if (!enabled) {
      setLoading(false);
      return;
    }

    try {
      setError(null);

      if (connectionId) {
        // Load for specific connection
        const response = await automationAPI.getNextRunTimes(connectionId, { forceFresh });

        setNextRuns(response?.next_runs || {});
      } else {
        // Load for all connections
        const response = await automationAPI.getAllNextRunTimes({ forceFresh });

        setNextRuns(response?.next_runs_by_connection || {});
      }
    } catch (err) {
      console.error('Error loading next run times:', err);
      setError(err);

      if (onError) {
        onError(err);
      }
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
    if (!enabled || refreshInterval <= 0) return;

    intervalRef.current = setInterval(() => {
      loadNextRuns(true); // Force fresh on interval
    }, refreshInterval);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
    };
  }, [loadNextRuns, enabled, refreshInterval]);

  const refresh = useCallback(() => {
    loadNextRuns(true);
  }, [loadNextRuns]);

  const triggerManualRun = async (automationType) => {
    if (!connectionId) return false;

    try {
      const response = await automationAPI.triggerImmediate(connectionId, automationType);

      // Refresh next runs after triggering
      setTimeout(() => refresh(), 1000);

      return response?.success || !response?.error;
    } catch (err) {
      console.error('Error triggering manual run:', err);
      return false;
    }
  };

  return {
    nextRuns,
    loading,
    error,
    refresh,
    triggerManualRun
  };
};

export const useAutomationNextRun = useNextRunTimes;
