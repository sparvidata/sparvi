import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useAutomationConfig } from '../../hooks/useAutomationConfig';
import { ScheduleConfig } from '../../components/automation';
import { NextRunDisplay } from '../../components/automation';
import {
  ClockIcon,
  BellIcon,
  PlayIcon,
  PauseIcon,
  Cog6ToothIcon,
  ExclamationTriangleIcon,
  CalendarIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../components/common/LoadingSpinner';

const AutomationSettingsPage = () => {
  const [searchParams] = useSearchParams();
  const { connections, activeConnection, setCurrentConnection } = useConnection();
  const { showNotification } = useUI();
  const { globalConfig, loading, updateGlobalConfig } = useAutomationConfig();

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

  const saveGlobalConfig = async (newConfig) => {
    const success = await updateGlobalConfig(newConfig);
    if (success) {
      showNotification('Global automation settings saved', 'success');
    } else {
      showNotification('Failed to save global settings', 'error');
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

  const handleScheduleUpdate = (newSchedule) => {
    showNotification('Automation schedule updated successfully', 'success');
  };

  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <LoadingSpinner size="lg" />
        <span className="ml-3 text-secondary-600">Loading automation settings...</span>
      </div>
    );
  }

  const selectedConnection = connections.find(c => c.id === selectedConnectionId);

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

      {/* Connection Selection and Schedule Configuration */}
      {connections.length > 0 && (
        <div className="bg-white border border-secondary-200 rounded-lg">
          <div className="px-6 py-4 border-b border-secondary-200">
            <h3 className="text-lg font-medium text-secondary-900 flex items-center">
              <CalendarIcon className="h-5 w-5 mr-2 text-primary-600" />
              Connection Automation Schedules
            </h3>
            <p className="mt-1 text-sm text-secondary-500">
              Configure when automated processes run for your database connections
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

            {/* Schedule Configuration */}
            {selectedConnectionId && (
              <div className="space-y-6">
                <ScheduleConfig
                  connectionId={selectedConnectionId}
                  onUpdate={handleScheduleUpdate}
                />

                {/* Next Run Display */}
                <div className="border-t pt-6">
                  <NextRunDisplay
                    connectionId={selectedConnectionId}
                    connectionName={selectedConnection?.name}
                    showManualTriggers={true}
                    compact={false}
                  />
                </div>
              </div>
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
                  All automated processes are currently paused. Connection-specific schedules will take effect once global automation is enabled.
                </p>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* No connections warning */}
      {connections.length === 0 && (
        <div className="rounded-md bg-secondary-50 p-4">
          <div className="flex">
            <div className="flex-shrink-0">
              <ExclamationTriangleIcon className="h-5 w-5 text-secondary-400" aria-hidden="true" />
            </div>
            <div className="ml-3">
              <h3 className="text-sm font-medium text-secondary-800">
                No database connections found
              </h3>
              <div className="mt-2 text-sm text-secondary-700">
                <p>
                  Create a database connection first to configure automation schedules.
                </p>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default AutomationSettingsPage;