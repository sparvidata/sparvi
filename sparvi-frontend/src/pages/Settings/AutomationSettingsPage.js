import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useAutomationConfig } from '../../hooks/useAutomationConfig';
import { getSession } from '../../api/supabase';
import {
  ClockIcon,
  BellIcon,
  PlayIcon,
  PauseIcon,
  Cog6ToothIcon,
  ExclamationTriangleIcon,
  ClipboardDocumentCheckIcon,
  TableCellsIcon,
  CommandLineIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../components/common/LoadingSpinner';

const AutomationSettingsPage = () => {
  const [searchParams] = useSearchParams();
  const { connections, activeConnection, setCurrentConnection } = useConnection();
  const { showNotification } = useUI();
  const { globalConfig, connectionConfigs, loading, updateGlobalConfig, updateConnectionConfig } = useAutomationConfig();

  // Get connection from URL params if specified
  const urlConnectionId = searchParams.get('connection');
  const [selectedConnectionId, setSelectedConnectionId] = useState(urlConnectionId || activeConnection?.id);

  // Auto-select connection if specified in URL
  useEffect(() => {
    if (urlConnectionId && connections.length > 0) {
      const connection = connections.find(c => c.id === urlConnectionId);
      if (connection) {
        setCurrentConnection(connection);
        setSelectedConnectionId(urlConnectionId);
      }
    }
  }, [urlConnectionId, connections, setCurrentConnection]);

  const [automationStatus, setAutomationStatus] = useState({});
  const [statusLoading, setStatusLoading] = useState(true);

  // Load automation status for selected connection
  useEffect(() => {
    if (!selectedConnectionId) {
      setStatusLoading(false);
      return;
    }

    const loadStatus = async () => {
      try {
        const session = await getSession();
        const token = session?.access_token;

        if (!token) return;

        const response = await fetch(`/api/automation/status/${selectedConnectionId}`, {
          headers: { 'Authorization': `Bearer ${token}` }
        });

        if (response.ok) {
          const data = await response.json();
          setAutomationStatus(data);
        }
      } catch (error) {
        console.error('Error loading automation status:', error);
      } finally {
        setStatusLoading(false);
      }
    };

    loadStatus();

    // Refresh status every 30 seconds
    const interval = setInterval(loadStatus, 30000);
    return () => clearInterval(interval);
  }, [selectedConnectionId]);

  const saveGlobalConfig = async (newConfig) => {
    const success = await updateGlobalConfig(newConfig);
    if (success) {
      showNotification('Global automation settings saved', 'success');
    } else {
      showNotification('Failed to save global settings', 'error');
    }
  };

  const saveConnectionConfig = async (connectionId, config) => {
    const success = await updateConnectionConfig(connectionId, config);
    if (success) {
      showNotification('Connection automation settings saved', 'success');
    } else {
      showNotification('Failed to save connection settings', 'error');
    }
  };

  const toggleGlobalAutomation = async () => {
    if (!globalConfig) return;

    const newConfig = {
      ...globalConfig,
      automation_enabled: !globalConfig.automation_enabled
    };

    await saveGlobalConfig(newConfig);
  };

  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <LoadingSpinner size="lg" />
        <span className="ml-3 text-secondary-600">Loading automation settings...</span>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Global Automation Controls */}
      <div className="bg-white border border-secondary-200 rounded-lg">
        <div className="px-6 py-4 border-b border-secondary-200">
          <h3 className="text-lg font-medium text-secondary-900 flex items-center">
            <Cog6ToothIcon className="h-5 w-5 mr-2 text-primary-600" />
            Global Automation Settings
          </h3>
          <p className="mt-1 text-sm text-secondary-500">
            Master controls for all automated processes across your connections
          </p>
        </div>

        <div className="p-6 space-y-4">
          {/* Master Enable/Disable */}
          <div className="flex items-center justify-between">
            <div>
              <h4 className="text-sm font-medium text-secondary-900">Master Automation Control</h4>
              <p className="text-sm text-secondary-500">Enable or disable all automated processes</p>
            </div>
            <button
              onClick={toggleGlobalAutomation}
              className={`flex items-center px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                globalConfig?.automation_enabled
                  ? 'bg-accent-100 text-accent-800 hover:bg-accent-200'
                  : 'bg-secondary-100 text-secondary-600 hover:bg-secondary-200'
              }`}
            >
              {globalConfig?.automation_enabled ? (
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
          <div className="border-t pt-4">
            <h4 className="text-sm font-medium text-secondary-900 mb-3">System Limits</h4>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-secondary-700">
                  Max Concurrent Jobs
                </label>
                <input
                  type="number"
                  min="1"
                  max="10"
                  value={globalConfig?.max_concurrent_jobs || 3}
                  onChange={(e) => {
                    const newConfig = {
                      ...globalConfig,
                      max_concurrent_jobs: parseInt(e.target.value)
                    };
                    saveGlobalConfig(newConfig);
                  }}
                  className="mt-1 block w-full border-secondary-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                />
                <p className="mt-1 text-xs text-secondary-500">
                  Maximum number of automation jobs that can run simultaneously
                </p>
              </div>

              <div>
                <label className="block text-sm font-medium text-secondary-700">
                  Default Retry Attempts
                </label>
                <input
                  type="number"
                  min="0"
                  max="5"
                  value={globalConfig?.default_retry_attempts || 2}
                  onChange={(e) => {
                    const newConfig = {
                      ...globalConfig,
                      default_retry_attempts: parseInt(e.target.value)
                    };
                    saveGlobalConfig(newConfig);
                  }}
                  className="mt-1 block w-full border-secondary-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                />
                <p className="mt-1 text-xs text-secondary-500">
                  Number of times to retry failed automation jobs
                </p>
              </div>
            </div>
          </div>

          {/* Notification Settings */}
          <div className="border-t pt-4">
            <h4 className="text-sm font-medium text-secondary-900 mb-3 flex items-center">
              <BellIcon className="h-4 w-4 mr-2" />
              Notification Settings
            </h4>

            <div className="space-y-3">
              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={globalConfig?.notification_settings?.email_on_changes || false}
                  onChange={(e) => {
                    const newConfig = {
                      ...globalConfig,
                      notification_settings: {
                        ...globalConfig?.notification_settings,
                        email_on_changes: e.target.checked
                      }
                    };
                    saveGlobalConfig(newConfig);
                  }}
                  className="rounded border-secondary-300 text-primary-600 shadow-sm focus:border-primary-300 focus:ring focus:ring-primary-200 focus:ring-opacity-50"
                />
                <span className="ml-2 text-sm text-secondary-900">
                  Email notifications when changes are detected
                </span>
              </label>

              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={globalConfig?.notification_settings?.email_on_failures || false}
                  onChange={(e) => {
                    const newConfig = {
                      ...globalConfig,
                      notification_settings: {
                        ...globalConfig?.notification_settings,
                        email_on_failures: e.target.checked
                      }
                    };
                    saveGlobalConfig(newConfig);
                  }}
                  className="rounded border-secondary-300 text-primary-600 shadow-sm focus:border-primary-300 focus:ring focus:ring-primary-200 focus:ring-opacity-50"
                />
                <span className="ml-2 text-sm text-secondary-900">
                  Email notifications when automation tasks fail
                </span>
              </label>

              <div>
                <label className="block text-sm font-medium text-secondary-700">
                  Slack Webhook URL (optional)
                </label>
                <input
                  type="url"
                  value={globalConfig?.notification_settings?.slack_webhook || ''}
                  onChange={(e) => {
                    const newConfig = {
                      ...globalConfig,
                      notification_settings: {
                        ...globalConfig?.notification_settings,
                        slack_webhook: e.target.value
                      }
                    };
                    saveGlobalConfig(newConfig);
                  }}
                  placeholder="https://hooks.slack.com/..."
                  className="mt-1 block w-full border-secondary-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                />
                <p className="mt-1 text-xs text-secondary-500">
                  Send automation notifications to a Slack channel
                </p>
              </div>

              {/* Quiet Hours */}
              <div className="border rounded-md p-3 bg-secondary-50">
                <label className="flex items-center mb-2">
                  <input
                    type="checkbox"
                    checked={globalConfig?.notification_settings?.notification_quiet_hours?.enabled || false}
                    onChange={(e) => {
                      const newConfig = {
                        ...globalConfig,
                        notification_settings: {
                          ...globalConfig?.notification_settings,
                          notification_quiet_hours: {
                            ...globalConfig?.notification_settings?.notification_quiet_hours,
                            enabled: e.target.checked
                          }
                        }
                      };
                      saveGlobalConfig(newConfig);
                    }}
                    className="rounded border-secondary-300 text-primary-600"
                  />
                  <span className="ml-2 text-sm font-medium text-secondary-900">
                    Enable quiet hours
                  </span>
                </label>

                {globalConfig?.notification_settings?.notification_quiet_hours?.enabled && (
                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <label className="block text-xs font-medium text-secondary-700">Start</label>
                      <input
                        type="time"
                        value={globalConfig?.notification_settings?.notification_quiet_hours?.start || '22:00'}
                        onChange={(e) => {
                          const newConfig = {
                            ...globalConfig,
                            notification_settings: {
                              ...globalConfig?.notification_settings,
                              notification_quiet_hours: {
                                ...globalConfig?.notification_settings?.notification_quiet_hours,
                                start: e.target.value
                              }
                            }
                          };
                          saveGlobalConfig(newConfig);
                        }}
                        className="mt-1 block w-full border-secondary-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                      />
                    </div>
                    <div>
                      <label className="block text-xs font-medium text-secondary-700">End</label>
                      <input
                        type="time"
                        value={globalConfig?.notification_settings?.notification_quiet_hours?.end || '06:00'}
                        onChange={(e) => {
                          const newConfig = {
                            ...globalConfig,
                            notification_settings: {
                              ...globalConfig?.notification_settings,
                              notification_quiet_hours: {
                                ...globalConfig?.notification_settings?.notification_quiet_hours,
                                end: e.target.value
                              }
                            }
                          };
                          saveGlobalConfig(newConfig);
                        }}
                        className="mt-1 block w-full border-secondary-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                      />
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Connection Selection */}
      {connections.length > 0 && (
        <div className="bg-white border border-secondary-200 rounded-lg">
          <div className="px-6 py-4 border-b border-secondary-200">
            <h3 className="text-lg font-medium text-secondary-900">Connection-Specific Automation</h3>
            <p className="mt-1 text-sm text-secondary-500">
              Configure automation settings for individual database connections
            </p>
          </div>

          <div className="p-6">
            {/* Connection Selector */}
            <div className="mb-6">
              <label className="block text-sm font-medium text-secondary-700 mb-2">
                Select Connection
              </label>
              <select
                value={selectedConnectionId || ''}
                onChange={(e) => setSelectedConnectionId(e.target.value)}
                className="block w-full border-secondary-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
              >
                <option value="">Select a connection...</option>
                {connections.map(connection => (
                  <option key={connection.id} value={connection.id}>
                    {connection.name} {connection.is_default ? '(Default)' : ''}
                  </option>
                ))}
              </select>
            </div>

            {/* Connection Configuration */}
            {selectedConnectionId && (
              <ConnectionAutomationConfig
                connectionId={selectedConnectionId}
                connection={connections.find(c => c.id === selectedConnectionId)}
                config={connectionConfigs[selectedConnectionId]}
                onSave={(config) => saveConnectionConfig(selectedConnectionId, config)}
                automationStatus={automationStatus}
                globalEnabled={globalConfig?.automation_enabled}
                loading={statusLoading}
              />
            )}
          </div>
        </div>
      )}

      {/* Global automation disabled warning */}
      {!globalConfig?.automation_enabled && (
        <div className="rounded-md bg-warning-50 p-4">
          <div className="flex">
            <div className="flex-shrink-0">
              <ExclamationTriangleIcon className="h-5 w-5 text-warning-400" aria-hidden="true" />
            </div>
            <div className="ml-3">
              <h3 className="text-sm font-medium text-warning-800">
                Global automation is disabled
              </h3>
              <div className="mt-2 text-sm text-warning-700">
                <p>
                  All automated processes are currently paused. Connection-specific settings will take effect once global automation is enabled.
                </p>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

// Component for individual connection automation config
const ConnectionAutomationConfig = ({
  connectionId,
  connection,
  config,
  onSave,
  automationStatus,
  globalEnabled,
  loading
}) => {
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
      notification_threshold: 'failures_only'
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
    if (loading) return <LoadingSpinner size="xs" />;

    const status = automationStatus?.last_runs?.[type];
    if (!status) return null;

    const statusColors = {
      running: 'text-primary-500',
      completed: 'text-accent-500',
      failed: 'text-danger-500',
      scheduled: 'text-secondary-500'
    };

    return (
      <span className={`text-xs ${statusColors[status.status]} ml-2`}>
        {status.status} {status.last_run && `(${new Date(status.last_run).toLocaleDateString()})`}
      </span>
    );
  };

  if (!connection) return null;

  return (
    <div className="space-y-6">
      <div className="bg-secondary-50 border border-secondary-200 rounded-lg p-4">
        <h4 className="font-medium text-secondary-900 mb-2">
          {connection.name} Automation Settings
        </h4>
        <p className="text-sm text-secondary-600">
          Configure automated processes for this database connection
        </p>
      </div>

      <div className="space-y-6">
        {/* Metadata Refresh */}
        <div className="border border-secondary-200 rounded-lg p-4">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center">
              <TableCellsIcon className="h-5 w-5 text-primary-600 mr-2" />
              <div>
                <div className="text-sm font-medium text-secondary-900">
                  Metadata Refresh
                  {getStatusIndicator('metadata_refresh')}
                </div>
                <div className="text-xs text-secondary-500">
                  Automatic refresh of table, column, and statistics metadata
                </div>
              </div>
            </div>
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={localConfig.metadata_refresh.enabled}
                onChange={(e) => updateConfig('metadata_refresh', 'enabled', e.target.checked)}
                className="rounded border-secondary-300 text-primary-600 shadow-sm focus:border-primary-300 focus:ring focus:ring-primary-200 focus:ring-opacity-50"
              />
              <span className="ml-2 text-sm">Enable</span>
            </label>
          </div>

          {localConfig.metadata_refresh.enabled && (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-secondary-700">
                  Refresh Interval
                </label>
                <select
                  value={localConfig.metadata_refresh.interval_hours}
                  onChange={(e) => updateConfig('metadata_refresh', 'interval_hours', parseInt(e.target.value))}
                  className="mt-1 block w-full border-secondary-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                >
                  <option value={1}>Every hour</option>
                  <option value={6}>Every 6 hours</option>
                  <option value={12}>Every 12 hours</option>
                  <option value={24}>Daily</option>
                  <option value={168}>Weekly</option>
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-secondary-700 mb-2">
                  Metadata Types to Refresh
                </label>
                <div className="space-y-2">
                  {['tables', 'columns', 'statistics'].map(type => (
                    <label key={type} className="flex items-center">
                      <input
                        type="checkbox"
                        checked={localConfig.metadata_refresh.types.includes(type)}
                        onChange={(e) => {
                          const types = e.target.checked
                            ? [...localConfig.metadata_refresh.types, type]
                            : localConfig.metadata_refresh.types.filter(t => t !== type);
                          updateConfig('metadata_refresh', 'types', types);
                        }}
                        className="rounded border-secondary-300 text-primary-600 shadow-sm focus:border-primary-300 focus:ring focus:ring-primary-200 focus:ring-opacity-50"
                      />
                      <span className="ml-2 text-sm text-secondary-900 capitalize">
                        {type}
                      </span>
                    </label>
                  ))}
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Schema Change Detection */}
        <div className="border border-secondary-200 rounded-lg p-4">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center">
              <CommandLineIcon className="h-5 w-5 text-primary-600 mr-2" />
              <div>
                <div className="text-sm font-medium text-secondary-900">
                  Schema Change Detection
                  {getStatusIndicator('schema_change_detection')}
                </div>
                <div className="text-xs text-secondary-500">
                  Automatic detection of database schema changes
                </div>
              </div>
            </div>
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={localConfig.schema_change_detection.enabled}
                onChange={(e) => updateConfig('schema_change_detection', 'enabled', e.target.checked)}
                className="rounded border-secondary-300 text-primary-600 shadow-sm focus:border-primary-300 focus:ring focus:ring-primary-200 focus:ring-opacity-50"
              />
              <span className="ml-2 text-sm">Enable</span>
            </label>
          </div>

          {localConfig.schema_change_detection.enabled && (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-secondary-700">
                  Detection Interval
                </label>
                <select
                  value={localConfig.schema_change_detection.interval_hours}
                  onChange={(e) => updateConfig('schema_change_detection', 'interval_hours', parseInt(e.target.value))}
                  className="mt-1 block w-full border-secondary-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                >
                  <option value={1}>Every hour</option>
                  <option value={6}>Every 6 hours</option>
                  <option value={12}>Every 12 hours</option>
                  <option value={24}>Daily</option>
                </select>
              </div>

              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={localConfig.schema_change_detection.auto_acknowledge_safe_changes}
                  onChange={(e) => updateConfig('schema_change_detection', 'auto_acknowledge_safe_changes', e.target.checked)}
                  className="rounded border-secondary-300 text-primary-600 shadow-sm focus:border-primary-300 focus:ring focus:ring-primary-200 focus:ring-opacity-50"
                />
                <span className="ml-2 text-sm text-secondary-900">
                  Auto-acknowledge non-breaking changes
                </span>
              </label>
            </div>
          )}
        </div>

        {/* Validation Automation */}
        <div className="border border-secondary-200 rounded-lg p-4">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center">
              <ClipboardDocumentCheckIcon className="h-5 w-5 text-primary-600 mr-2" />
              <div>
                <div className="text-sm font-medium text-secondary-900">
                  Validation Automation
                  {getStatusIndicator('validation_automation')}
                </div>
                <div className="text-xs text-secondary-500">
                  Automatic validation rule execution and monitoring
                </div>
              </div>
            </div>
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={localConfig.validation_automation.enabled}
                onChange={(e) => updateConfig('validation_automation', 'enabled', e.target.checked)}
                className="rounded border-secondary-300 text-primary-600 shadow-sm focus:border-primary-300 focus:ring focus:ring-primary-200 focus:ring-opacity-50"
              />
              <span className="ml-2 text-sm">Enable</span>
            </label>
          </div>

          {localConfig.validation_automation.enabled && (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-secondary-700">
                  Execution Interval
                </label>
                <select
                  value={localConfig.validation_automation.interval_hours}
                  onChange={(e) => updateConfig('validation_automation', 'interval_hours', parseInt(e.target.value))}
                  className="mt-1 block w-full border-secondary-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                >
                  <option value={6}>Every 6 hours</option>
                  <option value={12}>Every 12 hours</option>
                  <option value={24}>Daily</option>
                  <option value={168}>Weekly</option>
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-secondary-700">
                  Notification Threshold
                </label>
                <select
                  value={localConfig.validation_automation.notification_threshold}
                  onChange={(e) => updateConfig('validation_automation', 'notification_threshold', e.target.value)}
                  className="mt-1 block w-full border-secondary-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                >
                  <option value="all">All validation results</option>
                  <option value="failures_only">Failures only</option>
                  <option value="none">No notifications</option>
                </select>
              </div>

              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={localConfig.validation_automation.auto_generate_for_new_tables}
                  onChange={(e) => updateConfig('validation_automation', 'auto_generate_for_new_tables', e.target.checked)}
                  className="rounded border-secondary-300 text-primary-600 shadow-sm focus:border-primary-300 focus:ring focus:ring-primary-200 focus:ring-opacity-50"
                />
                <span className="ml-2 text-sm text-secondary-900">
                  Auto-generate validations for new tables
                </span>
              </label>
            </div>
          )}
        </div>
      </div>

      <div className="flex justify-end">
        <button
          onClick={handleSave}
          className="px-4 py-2 bg-primary-600 text-white text-sm rounded-md hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
        >
          Save Configuration
        </button>
      </div>
    </div>
  );
};

export default AutomationSettingsPage;