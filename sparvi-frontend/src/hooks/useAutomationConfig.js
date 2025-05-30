import { useState, useEffect, useCallback } from 'react';
import { automationAPI } from '../api/enhancedApiService';

export const useAutomationConfig = (connectionId = null) => {
  const [globalConfig, setGlobalConfig] = useState(null);
  const [connectionConfigs, setConnectionConfigs] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const loadConfigs = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);

      // Load global config
      try {
        const globalResponse = await automationAPI.getGlobalConfig();
        if (globalResponse?.config) {
          setGlobalConfig(globalResponse.config);
        }
      } catch (err) {
        console.error('Error loading global config:', err);
        // Don't fail completely if global config fails
      }

      // Load connection-specific configs
      if (connectionId) {
        try {
          const connResponse = await automationAPI.getConnectionConfig(connectionId);
          if (connResponse?.config) {
            setConnectionConfigs({ [connectionId]: connResponse.config });
          }
        } catch (err) {
          console.error('Error loading connection config:', err);
        }
      } else {
        // Load all connection configs
        try {
          const allConfigsResponse = await automationAPI.getConnectionConfigs();
          if (allConfigsResponse?.configs) {
            // Convert array to object keyed by connection_id
            const configsObject = {};
            allConfigsResponse.configs.forEach(config => {
              if (config.connection_id) {
                configsObject[config.connection_id] = config;
              }
            });
            setConnectionConfigs(configsObject);
          }
        } catch (err) {
          console.error('Error loading all connection configs:', err);
        }
      }
    } catch (err) {
      console.error('Error loading automation configs:', err);
      setError(err);
    } finally {
      setLoading(false);
    }
  }, [connectionId]);

  useEffect(() => {
    loadConfigs();
  }, [loadConfigs]);

  const updateGlobalConfig = async (newConfig) => {
    try {
      const response = await automationAPI.updateGlobalConfig(newConfig);

      if (response?.config) {
        setGlobalConfig(response.config);
        return true;
      } else if (response && !response.error) {
        // Some APIs return the config directly
        setGlobalConfig(response);
        return true;
      }

      return false;
    } catch (error) {
      console.error('Error updating global config:', error);
      setError(error);
      return false;
    }
  };

  const updateConnectionConfig = async (connId, newConfig) => {
    try {
      const response = await automationAPI.updateConnectionConfig(connId, newConfig);

      if (response?.config) {
        setConnectionConfigs(prev => ({
          ...prev,
          [connId]: response.config
        }));
        return true;
      } else if (response && !response.error) {
        // Some APIs return the config directly
        setConnectionConfigs(prev => ({
          ...prev,
          [connId]: response
        }));
        return true;
      }

      return false;
    } catch (error) {
      console.error('Error updating connection config:', error);
      setError(error);
      return false;
    }
  };

  // Add refresh function
  const refreshConfigs = useCallback(async () => {
    await loadConfigs();
  }, [loadConfigs]);

  return {
    globalConfig,
    connectionConfigs,
    loading,
    error,
    updateGlobalConfig,
    updateConnectionConfig,
    refreshConfigs
  };
};