// src/hooks/useAnomalyData.js

import { useState, useEffect, useCallback } from 'react';
import anomalyService from '../services/anomalyService';
import { useUI } from '../contexts/UIContext';

/**
 * Custom hook for anomaly dashboard data
 * @param {string} connectionId - The connection ID
 * @param {number} timeRange - Time range in days
 * @returns {Object} Dashboard data and methods
 */
export const useAnomalyDashboard = (connectionId, timeRange = 30) => {
  const { showNotification } = useUI();
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState(null);
  const [data, setData] = useState({
    summary: {},
    dashboard: {},
    configs: []
  });

  const loadData = useCallback(async () => {
    if (!connectionId || connectionId === 'undefined') return;

    try {
      setLoading(true);
      setError(null);

      console.log(`Loading anomaly dashboard data for connection ${connectionId}`);

      // Load all data in parallel
      const [summaryData, dashboardData, configsData] = await Promise.all([
        anomalyService.getSummary(connectionId, timeRange).catch(err => {
          console.error('Error loading summary:', err);
          return {};
        }),
        anomalyService.getDashboardData(connectionId, timeRange).catch(err => {
          console.error('Error loading dashboard:', err);
          return {};
        }),
        anomalyService.getConfigs(connectionId).catch(err => {
          console.error('Error loading configs:', err);
          return [];
        })
      ]);

      setData({
        summary: summaryData,
        dashboard: dashboardData,
        configs: configsData
      });

      console.log('Dashboard data loaded successfully');
    } catch (err) {
      console.error('Error loading dashboard data:', err);
      setError(`Failed to load dashboard data: ${err.message}`);
      showNotification(`Failed to load dashboard data: ${err.message}`, 'error');
    } finally {
      setLoading(false);
    }
  }, [connectionId, timeRange, showNotification]);

  const refresh = useCallback(async () => {
    if (refreshing || !connectionId || connectionId === 'undefined') return;

    setRefreshing(true);
    try {
      // Trigger a manual detection run
      const result = await anomalyService.runDetection(connectionId, { force: true });

      showNotification(
        `Anomaly detection completed: ${result.anomalies_detected || 0} anomalies found`,
        'success'
      );

      // Reload dashboard data
      await loadData();

    } catch (error) {
      console.error('Error refreshing anomaly data:', error);
      showNotification('Failed to refresh anomaly data', 'error');
    } finally {
      setRefreshing(false);
    }
  }, [connectionId, refreshing, showNotification, loadData]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  return {
    data,
    loading,
    refreshing,
    error,
    refresh,
    reload: loadData
  };
};

/**
 * Custom hook for anomaly explorer data
 * @param {string} connectionId - The connection ID
 * @param {Object} filters - Filter options
 * @returns {Object} Anomaly data and methods
 */
export const useAnomalyExplorer = (connectionId, filters = {}) => {
  const { showNotification } = useUI();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [anomalies, setAnomalies] = useState([]);

  const loadAnomalies = useCallback(async () => {
    if (!connectionId || connectionId === 'undefined') return;

    try {
      setLoading(true);
      setError(null);

      console.log(`Loading anomalies for connection ${connectionId}`);

      const anomaliesData = await anomalyService.getAnomalies(connectionId, {
        days: filters.timeRange || 30,
        status: filters.status !== 'all' ? filters.status : undefined,
        table_name: filters.table || undefined,
        limit: 100
      });

      setAnomalies(anomaliesData);
      console.log(`Loaded ${anomaliesData.length} anomalies`);
    } catch (err) {
      console.error('Error loading anomalies:', err);
      setError(`Failed to load anomalies: ${err.message}`);
      showNotification(`Failed to load anomalies: ${err.message}`, 'error');
    } finally {
      setLoading(false);
    }
  }, [connectionId, filters, showNotification]);

  const updateAnomalyStatus = useCallback(async (anomalyId, newStatus, note) => {
    if (!connectionId || connectionId === 'undefined') {
      showNotification('Cannot update status: No connection selected', 'error');
      return;
    }

    try {
      await anomalyService.updateAnomalyStatus(connectionId, anomalyId, newStatus, note);
      showNotification(`Anomaly status updated to ${newStatus}`, 'success');

      // Refresh anomalies list
      await loadAnomalies();

      return true;
    } catch (error) {
      console.error('Error updating anomaly status:', error);
      showNotification('Failed to update anomaly status', 'error');
      return false;
    }
  }, [connectionId, showNotification, loadAnomalies]);

  useEffect(() => {
    loadAnomalies();
  }, [loadAnomalies]);

  return {
    anomalies,
    loading,
    error,
    refresh: loadAnomalies,
    updateStatus: updateAnomalyStatus
  };
};

/**
 * Custom hook for anomaly configurations
 * @param {string} connectionId - The connection ID
 * @returns {Object} Config data and CRUD methods
 */
export const useAnomalyConfigs = (connectionId) => {
  const { showNotification } = useUI();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [configs, setConfigs] = useState([]);

  const loadConfigs = useCallback(async () => {
    if (!connectionId || connectionId === 'undefined') return;

    try {
      setLoading(true);
      setError(null);

      console.log(`Loading configs for connection ${connectionId}`);
      const configsData = await anomalyService.getConfigs(connectionId);

      setConfigs(configsData);
      console.log(`Loaded ${configsData.length} configurations`);
    } catch (err) {
      console.error('Error loading configs:', err);
      setError(`Failed to load configurations: ${err.message}`);
      showNotification(`Failed to load configurations: ${err.message}`, 'error');
    } finally {
      setLoading(false);
    }
  }, [connectionId, showNotification]);

  const createConfig = useCallback(async (configData) => {
    try {
      await anomalyService.createConfig(connectionId, configData);
      showNotification('Configuration created successfully', 'success');
      await loadConfigs();
      return true;
    } catch (error) {
      console.error('Error creating config:', error);
      showNotification(`Failed to create configuration: ${error.message}`, 'error');
      return false;
    }
  }, [connectionId, showNotification, loadConfigs]);

  const updateConfig = useCallback(async (configId, configData) => {
    try {
      await anomalyService.updateConfig(connectionId, configId, configData);
      showNotification('Configuration updated successfully', 'success');
      await loadConfigs();
      return true;
    } catch (error) {
      console.error('Error updating config:', error);
      showNotification(`Failed to update configuration: ${error.message}`, 'error');
      return false;
    }
  }, [connectionId, showNotification, loadConfigs]);

  const deleteConfig = useCallback(async (configId) => {
    try {
      await anomalyService.deleteConfig(connectionId, configId);
      showNotification('Configuration deleted successfully', 'success');
      await loadConfigs();
      return true;
    } catch (error) {
      console.error('Error deleting config:', error);
      showNotification(`Failed to delete configuration: ${error.message}`, 'error');
      return false;
    }
  }, [connectionId, showNotification, loadConfigs]);

  useEffect(() => {
    loadConfigs();
  }, [loadConfigs]);

  return {
    configs,
    loading,
    error,
    refresh: loadConfigs,
    create: createConfig,
    update: updateConfig,
    delete: deleteConfig
  };
};