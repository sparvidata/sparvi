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

        const response = await automationAPI.getAutomationStatus(null, connectionId);

        if (response?.status) {
          setStatus(response.status);
        } else if (response && !response.error) {
          // Handle direct status response
          setStatus(response);
        }
      } catch (error) {
        console.error('Error loading automation status:', error);
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
        const updatedResponse = await automationAPI.getAutomationStatus(null, connectionId);

        if (updatedResponse?.status) {
          setStatus(updatedResponse.status);
        } else if (updatedResponse && !updatedResponse.error) {
          setStatus(updatedResponse);
        }

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