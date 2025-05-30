import { useState, useEffect } from 'react';
import { getSession } from '../api/supabase';

export const useAutomationConfig = (connectionId = null) => {
  const [globalConfig, setGlobalConfig] = useState(null);
  const [connectionConfigs, setConnectionConfigs] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const loadConfigs = async () => {
      try {
        const session = await getSession();
        const token = session?.access_token;

        if (!token) {
          setLoading(false);
          return;
        }

        // Load global config
        const globalResponse = await fetch('/api/automation/global-config', {
          headers: { 'Authorization': `Bearer ${token}` }
        });

        if (globalResponse.ok) {
          const global = await globalResponse.json();
          setGlobalConfig(global);
        }

        // Load connection-specific configs
        if (connectionId) {
          const connResponse = await fetch(`/api/automation/connection-configs/${connectionId}`, {
            headers: { 'Authorization': `Bearer ${token}` }
          });

          if (connResponse.ok) {
            const connConfig = await connResponse.json();
            setConnectionConfigs({ [connectionId]: connConfig });
          }
        } else {
          // Load all connection configs
          const allConfigsResponse = await fetch('/api/automation/connection-configs', {
            headers: { 'Authorization': `Bearer ${token}` }
          });

          if (allConfigsResponse.ok) {
            const allConfigs = await allConfigsResponse.json();
            setConnectionConfigs(allConfigs);
          }
        }
      } catch (err) {
        console.error('Error loading automation configs:', err);
        setError(err);
      } finally {
        setLoading(false);
      }
    };

    loadConfigs();
  }, [connectionId]);

  const updateGlobalConfig = async (newConfig) => {
    try {
      const session = await getSession();
      const token = session?.access_token;

      if (!token) return false;

      const response = await fetch('/api/automation/global-config', {
        method: 'PUT',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(newConfig)
      });

      if (response.ok) {
        const updated = await response.json();
        setGlobalConfig(updated);
        return true;
      }
      return false;
    } catch (error) {
      console.error('Error updating global config:', error);
      return false;
    }
  };

  const updateConnectionConfig = async (connId, newConfig) => {
    try {
      const session = await getSession();
      const token = session?.access_token;

      if (!token) return false;

      const response = await fetch(`/api/automation/connection-configs/${connId}`, {
        method: 'PUT',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(newConfig)
      });

      if (response.ok) {
        const updated = await response.json();
        setConnectionConfigs(prev => ({
          ...prev,
          [connId]: updated
        }));
        return true;
      }
      return false;
    } catch (error) {
      console.error('Error updating connection config:', error);
      return false;
    }
  };

  return {
    globalConfig,
    connectionConfigs,
    loading,
    error,
    updateGlobalConfig,
    updateConnectionConfig
  };
};