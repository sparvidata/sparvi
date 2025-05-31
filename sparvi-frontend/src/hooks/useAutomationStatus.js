import { useState, useEffect } from 'react';
import { automationAPI } from '../api/enhancedApiService';

export const useAutomationStatus = (connectionId) => {
  const [status, setStatus] = useState({
    global_enabled: false,
    connection_config: null,
    active_jobs: [],
    last_runs: {}
  });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!connectionId) {
      setLoading(false);
      return;
    }

    const loadStatus = async () => {
      try {
        setLoading(true);

        // Load both status and config in parallel
        const [statusResponse, configResponse] = await Promise.all([
          automationAPI.getAutomationStatus(null, connectionId).catch(err => {
            console.error('Error loading automation status:', err);
            return { global_enabled: false, active_jobs: 0 };
          }),
          automationAPI.getConnectionConfig(connectionId, { forceFresh: true }).catch(err => {
            console.error('Error loading connection config:', err);
            return null;
          })
        ]);

        console.log('Automation status response:', statusResponse);
        console.log('Connection config response:', configResponse);

        // Extract status data - the API returns {"status": {...}}
        const statusData = statusResponse?.status || statusResponse || {};

        // Extract config data - the API returns {"config": {...}}
        const configData = configResponse?.config || configResponse;

        setStatus({
          global_enabled: statusData.global_enabled || false,
          connection_config: configData,
          active_jobs: statusData.active_jobs || [],
          last_runs: statusData.last_runs || {},
          // Additional status fields
          pending_jobs: statusData.pending_jobs || 0,
          failed_jobs_24h: statusData.failed_jobs_24h || 0,
          last_run: statusData.last_run
        });
      } catch (error) {
        console.error('Error loading automation status:', error);
        // Set default values on error
        setStatus({
          global_enabled: false,
          connection_config: null,
          active_jobs: [],
          last_runs: {}
        });
      } finally {
        setLoading(false);
      }
    };

    loadStatus();

    // Poll for status updates every 30 seconds
    const interval = setInterval(loadStatus, 30000);
    return () => clearInterval(interval);
  }, [connectionId]);

  const toggleAutomation = async (automationType, enabled) => {
    try {
      const response = await automationAPI.toggleConnectionAutomation(connectionId, enabled);

      if (response?.result || response?.success) {
        // Reload status after toggle
        const [statusResponse, configResponse] = await Promise.all([
          automationAPI.getAutomationStatus(null, connectionId),
          automationAPI.getConnectionConfig(connectionId, { forceFresh: true })
        ]);

        const statusData = statusResponse?.status || statusResponse || {};
        const configData = configResponse?.config || configResponse;

        setStatus(prev => ({
          ...prev,
          connection_config: configData,
          global_enabled: statusData.global_enabled || prev.global_enabled
        }));

        return true;
      }
      return false;
    } catch (error) {
      console.error('Error toggling automation:', error);
      return false;
    }
  };

  const triggerManualRun = async (automationType) => {
    try {
      const response = await automationAPI.triggerAutomation(connectionId, automationType);
      return response?.success || response?.result || !response?.error;
    } catch (error) {
      console.error('Error triggering manual run:', error);
      return false;
    }
  };

  return {
    status,
    loading,
    toggleAutomation,
    triggerManualRun
  };
};