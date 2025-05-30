import React, { useState, useEffect } from 'react';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import {
  ClockIcon,
  BellIcon,
  PlayIcon,
  PauseIcon,
  Cog6ToothIcon,
  ExclamationTriangleIcon,
  ClipboardDocumentCheckIcon,
  TableCellsIcon
} from '@heroicons/react/24/outline';

const AutomationSettings = () => {
  const { connections, activeConnection, setCurrentConnection } = useConnection();
  const { showNotification } = useUI();

  const [globalConfig, setGlobalConfig] = useState({
    automation_enabled: true,
    max_concurrent_jobs: 3,
    default_retry_attempts: 2,
    notification_settings: {
      email_on_changes: true,
      email_on_failures: true,
      slack_webhook: '',
      notification_quiet_hours: {
        enabled: false,
        start: '22:00',
        end: '06:00'
      }
    }
  });

  const [connectionConfigs, setConnectionConfigs] = useState({});
  const [automationStatus, setAutomationStatus] = useState({});
  const [loading, setLoading] = useState(true);

  // Load all automation configurations
  useEffect(() => {
    loadAutomationConfigs();
  }, []);

  const loadAutomationConfigs = async () => {
    setLoading(true);
    try {
      // Load global config
      const globalResponse = await fetch('/api/automation/global-config');
      if (globalResponse.ok) {
        const global = await globalResponse.json();
        setGlobalConfig(global);
      }

      // Load per-connection configs
      const configsResponse = await fetch('/api/automation/connection-configs');
      if (configsResponse.ok) {
        const configs = await configsResponse.json();
        setConnectionConfigs(configs);
      }

      // Load current automation status
      const statusResponse = await fetch('/api/automation/status');
      if (statusResponse.ok) {
        const status = await statusResponse.json();
        setAutomationStatus(status);
      }
    } catch (error) {
      console.error('Error loading automation configs:', error);
      showNotification('Failed to load automation settings', 'error');
    } finally {
      setLoading(false);
    }
  };

  const saveGlobalConfig = async () => {
    try {
      const response = await fetch('/api/automation/global-config', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(globalConfig)
      });

      if (response.ok) {
        showNotification('Global automation settings saved', 'success');
      } else {
        throw new Error('Failed to save');
      }
    } catch (error) {
      showNotification('Failed to save global settings', 'error');
    }
  };

  const saveConnectionConfig = async (connectionId, config) => {
    try {
      const response = await fetch(`/api/automation/connection-configs/${connectionId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config)
      });

      if (response.ok) {
        setConnectionConfigs(prev => ({
          ...prev,
          [connectionId]: config
        }));
        showNotification('Connection automation settings saved', 'success');
      } else {
        throw new Error('Failed to save');
      }
    } catch (error) {
      showNotification('Failed to save connection settings', 'error');
    }
  };

  const toggleGlobalAutomation = async () => {
    const newConfig = {
      ...globalConfig,
      automation_enabled: !globalConfig.automation_enabled
    };
    setGlobalConfig(newConfig);

    try {
      await fetch('/api/automation/global-toggle', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled: newConfig.automation_enabled })
      });

      showNotification(
        `Automation ${newConfig.automation_enabled ? 'enabled' : 'disabled'} globally`,
        'success'
      );
    } catch (error) {
      showNotification('Failed to toggle automation', 'error');
    }
  };

  if (loading) {
    return <div>Loading automation settings...</div>;
  }

  return (
    <div className="space-y-6">
      {/* Global Automation Controls */}
      <div className="bg-white shadow rounded-lg">
        <div className="px-6 py-4 border-b border-gray-200">
          <h3 className="text-lg font-medium text-gray-900 flex items-center">
            <Cog6ToothIcon className="h-5 w-5 mr-2 text-primary-600" />
            Global Automation Settings
          </h3>
        </div>

        <div className="p-6 space-y-4">
          {/* Master Enable/Disable */}
          <div className="flex items-center justify-between">
            <div>
              <h4 className="text-sm font-medium text-gray-900">Master Automation Control</h4>
              <p className="text-sm text-gray-500">Enable or disable all automated processes</p>
            </div>
            <button
              onClick={toggleGlobalAutomation}
              className={`flex items-center px-4 py-2 rounded-md text-sm font-medium ${
                globalConfig.automation_enabled
                  ? 'bg-green-100 text-green-800 hover:bg-green-200'
                  : 'bg-red-100 text-red-800 hover:bg-red-200'
              }`}
            >
              {globalConfig.automation_enabled ? (
                <>
                  <PlayIcon className="h-4 w-4 mr-2" />
                  Enabled
                </>
              ) : (
                <>
                  <PauseIcon className="h-4 w-4 mr-2" />
                  Disabled
                </>
              )}
            </button>
          </div>

          {/* System Limits */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700">
                Max Concurrent Jobs
              </label>
              <input
                type="number"
                min="1"
                max="10"
                value={globalConfig.max_concurrent_jobs}
                onChange={(e) => setGlobalConfig(prev => ({
                  ...prev,
                  max_concurrent_jobs: parseInt(e.target.value)
                }))}
                className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700">
                Default Retry Attempts
              </label>
              <input
                type="number"
                min="0"
                max="5"
                value={globalConfig.default_retry_attempts}
                onChange={(e) => setGlobalConfig(prev => ({
                  ...prev,
                  default_retry_attempts: parseInt(e.target.value)
                }))}
                className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500"
              />
            </div>
          </div>

          {/* Notification Settings */}
          <div className="border-t pt-4">
            <h4 className="text-sm font-medium text-gray-900 mb-3 flex items-center">
              <BellIcon className="h-4 w-4 mr-2" />
              Notification Settings
            </h4>

            <div className="space-y-3">
              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={globalConfig.notification_settings.email_on_changes}
                  onChange={(e) => setGlobalConfig(prev => ({
                    ...prev,
                    notification_settings: {
                      ...prev.notification_settings,
                      email_on_changes: e.target.checked
                    }
                  }))}
                  className="rounded border-gray-300 text-primary-600"
                />
                <span className="ml-2 text-sm text-gray-900">
                  Email notifications when changes are detected
                </span>
              </label>

              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={globalConfig.notification_settings.email_on_failures}
                  onChange={(e) => setGlobalConfig(prev => ({
                    ...prev,
                    notification_settings: {
                      ...prev.notification_settings,
                      email_on_failures: e.target.checked
                    }
                  }))}
                  className="rounded border-gray-300 text-primary-600"
                />
                <span className="ml-2 text-sm text-gray-900">
                  Email notifications when automation tasks fail
                </span>
              </label>

              <div>
                <label className="block text-sm font-medium text-gray-700">
                  Slack Webhook URL (optional)
                </label>
                <input
                  type="url"
                  value={globalConfig.notification_settings.slack_webhook}
                  onChange={(e) => setGlobalConfig(prev => ({
                    ...prev,
                    notification_settings: {
                      ...prev.notification_settings,
                      slack_webhook: e.target.value
                    }
                  }))}
                  placeholder="https://hooks.slack.com/..."
                  className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500"
                />
              </div>
            </div>
          </div>

          <div className="flex justify-end">
            <button
              onClick={saveGlobalConfig}
              className="px-4 py-2 bg-primary-600 text-white rounded-md hover:bg-primary-700"
            >
              Save Global Settings
            </button>
          </div>
        </div>
      </div>

      {/* Per-Connection Automation */}
      <div className="bg-white shadow rounded-lg">
        <div className="px-6 py-4 border-b border-gray-200">
          <h3 className="text-lg font-medium text-gray-900">Connection-Specific Automation</h3>
        </div>

        <div className="p-6">
          {connections.map(connection => (
            <ConnectionAutomationConfig
              key={connection.id}
              connection={connection}
              config={connectionConfigs[connection.id]}
              onSave={(config) => saveConnectionConfig(connection.id, config)}
              automationStatus={automationStatus[connection.id]}
            />
          ))}
        </div>
      </div>

      {/* Active Jobs Status */}
      <AutomationStatusPanel automationStatus={automationStatus} />
    </div>
  );
};

// Component for individual connection automation config
const ConnectionAutomationConfig = ({ connection, config, onSave, automationStatus }) => {
  const [localConfig, setLocalConfig] = useState({
    metadata_refresh: {
      enabled: false,
      interval_hours: 24,
      types: ['tables', 'columns', 'statistics']
    },
    schema_change_detection: {
      enabled: false,
      interval_hours: 6,
      auto_acknowledge_safe_changes: false
    },
    validation_automation: {
      enabled: false,
      interval_hours: 12,
      auto_generate_for_new_tables: true,
      notification_threshold: 'failures_only' // 'all', 'failures_only', 'none'
    },
    ...config
  });

  const updateConfig = (section, field, value) => {
    setLocalConfig(prev => ({
      ...prev,
      [section]: {
        ...prev[section],
        [field]: value
      }
    }));
  };

  const handleSave = () => {
    onSave(localConfig);
  };

  const getStatusIndicator = (type) => {
    const status = automationStatus?.[type];
    if (!status) return null;

    const statusColors = {
      running: 'text-blue-500',
      success: 'text-green-500',
      failed: 'text-red-500',
      scheduled: 'text-yellow-500'
    };

    return (
      <span className={`text-xs ${statusColors[status.status]} ml-2`}>
        {status.status} {status.last_run && `(${new Date(status.last_run).toLocaleString()})`}
      </span>
    );
  };

  return (
    <div className="border rounded-lg p-4 mb-4">
      <h4 className="font-medium text-gray-900 mb-4">{connection.name}</h4>

      <div className="space-y-4">
        {/* Metadata Refresh */}
        <div className="flex items-center justify-between">
          <div className="flex items-center">
            <TableCellsIcon className="h-5 w-5 text-gray-400 mr-2" />
            <div>
              <div className="text-sm font-medium text-gray-900">
                Metadata Refresh
                {getStatusIndicator('metadata')}
              </div>
              <div className="text-xs text-gray-500">
                Automatic refresh of table, column, and statistics metadata
              </div>
            </div>
          </div>
          <div className="flex items-center space-x-2">
            <select
              value={localConfig.metadata_refresh.interval_hours}
              onChange={(e) => updateConfig('metadata_refresh', 'interval_hours', parseInt(e.target.value))}
              disabled={!localConfig.metadata_refresh.enabled}
              className="text-xs border-gray-300 rounded"
            >
              <option value={6}>Every 6 hours</option>
              <option value={12}>Every 12 hours</option>
              <option value={24}>Daily</option>
              <option value={168}>Weekly</option>
            </select>
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={localConfig.metadata_refresh.enabled}
                onChange={(e) => updateConfig('metadata_refresh', 'enabled', e.target.checked)}
                className="rounded border-gray-300 text-primary-600"
              />
              <span className="ml-1 text-xs">Enable</span>
            </label>
          </div>
        </div>

        {/* Schema Change Detection */}
        <div className="flex items-center justify-between">
          <div className="flex items-center">
            <ExclamationTriangleIcon className="h-5 w-5 text-gray-400 mr-2" />
            <div>
              <div className="text-sm font-medium text-gray-900">
                Schema Change Detection
                {getStatusIndicator('schema_changes')}
              </div>
              <div className="text-xs text-gray-500">
                Automatic detection of database schema changes
              </div>
            </div>
          </div>
          <div className="flex items-center space-x-2">
            <select
              value={localConfig.schema_change_detection.interval_hours}
              onChange={(e) => updateConfig('schema_change_detection', 'interval_hours', parseInt(e.target.value))}
              disabled={!localConfig.schema_change_detection.enabled}
              className="text-xs border-gray-300 rounded"
            >
              <option value={1}>Hourly</option>
              <option value={6}>Every 6 hours</option>
              <option value={12}>Every 12 hours</option>
              <option value={24}>Daily</option>
            </select>
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={localConfig.schema_change_detection.enabled}
                onChange={(e) => updateConfig('schema_change_detection', 'enabled', e.target.checked)}
                className="rounded border-gray-300 text-primary-600"
              />
              <span className="ml-1 text-xs">Enable</span>
            </label>
          </div>
        </div>

        {/* Validation Automation */}
        <div className="flex items-center justify-between">
          <div className="flex items-center">
            <ClipboardDocumentCheckIcon className="h-5 w-5 text-gray-400 mr-2" />
            <div>
              <div className="text-sm font-medium text-gray-900">
                Validation Automation
                {getStatusIndicator('validations')}
              </div>
              <div className="text-xs text-gray-500">
                Automatic validation rule execution and monitoring
              </div>
            </div>
          </div>
          <div className="flex items-center space-x-2">
            <select
              value={localConfig.validation_automation.interval_hours}
              onChange={(e) => updateConfig('validation_automation', 'interval_hours', parseInt(e.target.value))}
              disabled={!localConfig.validation_automation.enabled}
              className="text-xs border-gray-300 rounded"
            >
              <option value={6}>Every 6 hours</option>
              <option value={12}>Every 12 hours</option>
              <option value={24}>Daily</option>
              <option value={168}>Weekly</option>
            </select>
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={localConfig.validation_automation.enabled}
                onChange={(e) => updateConfig('validation_automation', 'enabled', e.target.checked)}
                className="rounded border-gray-300 text-primary-600"
              />
              <span className="ml-1 text-xs">Enable</span>
            </label>
          </div>
        </div>
      </div>

      <div className="mt-4 flex justify-end">
        <button
          onClick={handleSave}
          className="px-3 py-1 bg-primary-600 text-white text-xs rounded hover:bg-primary-700"
        >
          Save
        </button>
      </div>
    </div>
  );
};

// Automation Status Panel
const AutomationStatusPanel = ({ automationStatus }) => {
  return (
    <div className="bg-white shadow rounded-lg">
      <div className="px-6 py-4 border-b border-gray-200">
        <h3 className="text-lg font-medium text-gray-900 flex items-center">
          <ClockIcon className="h-5 w-5 mr-2 text-primary-600" />
          Active Automation Jobs
        </h3>
      </div>

      <div className="p-6">
        {Object.keys(automationStatus).length === 0 ? (
          <p className="text-gray-500 text-center py-4">No active automation jobs</p>
        ) : (
          <div className="space-y-2">
            {Object.entries(automationStatus).map(([connectionId, status]) => (
              <div key={connectionId} className="flex items-center justify-between p-2 bg-gray-50 rounded">
                <span className="text-sm">{status.connection_name}</span>
                <span className="text-xs text-gray-500">{status.job_type}</span>
                <span className={`text-xs px-2 py-1 rounded ${
                  status.status === 'running' ? 'bg-blue-100 text-blue-800' :
                  status.status === 'success' ? 'bg-green-100 text-green-800' :
                  'bg-red-100 text-red-800'
                }`}>
                  {status.status}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default AutomationSettings;