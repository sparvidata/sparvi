import { useState, useEffect } from 'react';
import { getSession } from '../api/supabase';

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
        const session = await getSession();
        const token = session?.access_token;

        if (!token) {
          setLoading(false);
          return;
        }

        const response = await fetch(`/api/automation/status/${connectionId}`, {
          headers: { 'Authorization': `Bearer ${token}` }
        });

        if (response.ok) {
          const data = await response.json();
          setStatus(data);
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
      const session = await getSession();
      const token = session?.access_token;

      if (!token) return false;

      const response = await fetch(`/api/automation/toggle/${connectionId}`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          automation_type: automationType,
          enabled
        })
      });

      if (response.ok) {
        // Reload status after toggle
        const updatedResponse = await fetch(`/api/automation/status/${connectionId}`, {
          headers: { 'Authorization': `Bearer ${token}` }
        });

        if (updatedResponse.ok) {
          const updatedData = await updatedResponse.json();
          setStatus(updatedData);
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
      const session = await getSession();
      const token = session?.access_token;

      if (!token) return false;

      const response = await fetch(`/api/automation/trigger/${connectionId}`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          automation_type: automationType,
          manual_trigger: true
        })
      });

      return response.ok;
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