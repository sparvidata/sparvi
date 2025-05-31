import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { getSession } from '../../../api/supabase';
import {
  ClockIcon,
  Cog6ToothIcon,
  PlayIcon,
  PauseIcon,
  ExclamationCircleIcon,
  ClipboardDocumentCheckIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

const ValidationAutomationControls = ({
  connectionId,
  tableName,
  automationStatus,
  onToggleValidationAutomation,
  onTriggerAutomatedRun
}) => {
  const [tableConfig, setTableConfig] = useState(null);
  const [loadingTableConfig, setLoadingTableConfig] = useState(false);
  const [savingTableConfig, setSavingTableConfig] = useState(false);

  // Add this helper function at the top of the component
  const isLoadingAutomationStatus = !automationStatus || !automationStatus.connection_config;

  // Load table-specific validation automation config
  useEffect(() => {
    if (!connectionId || !tableName) {
      setTableConfig(null);
      return;
    }

    const loadTableConfig = async () => {
      setLoadingTableConfig(true);
      try {
        const session = await getSession();
        const token = session?.access_token;

        if (!token) return;

        const response = await fetch(`/api/automation/table-configs/${connectionId}/${tableName}`, {
          headers: { 'Authorization': `Bearer ${token}` }
        });

        if (response.ok) {
          const data = await response.json();
          setTableConfig(data.config || data);
        } else {
          // If no config exists, create default
          setTableConfig({
            auto_run_validations: false,
            auto_run_interval_hours: 24,
            validation_notification_threshold: 'failures_only'
          });
        }
      } catch (error) {
        console.error('Error loading table automation config:', error);
      } finally {
        setLoadingTableConfig(false);
      }
    };

    loadTableConfig();
  }, [connectionId, tableName]);

  // Save table-specific config
  const saveTableConfig = async (newConfig) => {
    if (!connectionId || !tableName) return;

    setSavingTableConfig(true);
    try {
      const session = await getSession();
      const token = session?.access_token;

      if (!token) return;

      const response = await fetch(`/api/automation/table-configs/${connectionId}/${tableName}`, {
        method: 'PUT',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(newConfig)
      });

      if (response.ok) {
        const data = await response.json();
        setTableConfig(data.config || data);
      }
    } catch (error) {
      console.error('Error saving table automation config:', error);
    } finally {
      setSavingTableConfig(false);
    }
  };

  // Toggle table-specific validation automation
  const handleToggleTableAutomation = async () => {
    if (!tableConfig) return;

    const newConfig = {
      ...tableConfig,
      auto_run_validations: !tableConfig.auto_run_validations
    };

    await saveTableConfig(newConfig);
  };

  // Update table automation interval
  const handleIntervalChange = async (newInterval) => {
    if (!tableConfig) return;

    const newConfig = {
      ...tableConfig,
      auto_run_interval_hours: parseInt(newInterval)
    };

    await saveTableConfig(newConfig);
  };

  // Update notification threshold
  const handleThresholdChange = async (newThreshold) => {
    if (!tableConfig) return;

    const newConfig = {
      ...tableConfig,
      validation_notification_threshold: newThreshold
    };

    await saveTableConfig(newConfig);
  };

  if (!automationStatus) {
    return (
      <div className="bg-white border border-secondary-200 rounded-lg p-4">
        <div className="animate-pulse h-24 bg-secondary-100 rounded"></div>
      </div>
    );
  }

  const globalValidationEnabled = automationStatus.connection_config?.validation_automation?.enabled;
  const tableValidationEnabled = tableConfig?.auto_run_validations;
  const automationGloballyEnabled = automationStatus.global_enabled;

  return (
    <div className="bg-white border border-secondary-200 rounded-lg">
      <div className="px-4 py-3 border-b border-secondary-200">
        <div className="flex items-center justify-between">
          <h4 className="text-sm font-medium text-secondary-900 flex items-center">
            <ClockIcon className="h-4 w-4 mr-2" />
            Validation Automation
          </h4>
          <Link
            to="/settings/automation"
            className="text-xs text-primary-600 hover:text-primary-700 flex items-center"
          >
            <Cog6ToothIcon className="h-3 w-3 mr-1" />
            Configure
          </Link>
        </div>
      </div>

      <div className="p-4 space-y-4">
        {/* Global validation automation status */}
        <div>
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs font-medium text-secondary-700">Connection-wide Validation Automation</span>

            {isLoadingAutomationStatus ? (
              // Loading state
              <div className="flex items-center px-2 py-1 rounded text-xs font-medium bg-secondary-100">
                <div className="animate-spin h-3 w-3 border border-secondary-400 border-t-transparent rounded-full mr-1"></div>
                <span className="text-secondary-500">Loading...</span>
              </div>
            ) : (
              // Actual toggle
              <button
                onClick={onToggleValidationAutomation}
                disabled={!automationGloballyEnabled}
                className={`flex items-center px-2 py-1 rounded text-xs font-medium transition-all duration-200 ${
                  globalValidationEnabled && automationGloballyEnabled
                    ? 'bg-accent-100 text-accent-800 hover:bg-accent-200'
                    : 'bg-secondary-100 text-secondary-600 hover:bg-secondary-200'
                } ${!automationGloballyEnabled ? 'opacity-50 cursor-not-allowed' : ''}`}
              >
                {globalValidationEnabled ? (
                  <>
                    <PlayIcon className="h-3 w-3 mr-1" />
                    On
                  </>
                ) : (
                  <>
                    <PauseIcon className="h-3 w-3 mr-1" />
                    Off
                  </>
                )}
              </button>
            )}
          </div>

          {/* Only show details when not loading */}
          {!isLoadingAutomationStatus && (
            <>
              {globalValidationEnabled && automationStatus.connection_config?.validation_automation?.interval_hours && (
                <p className="text-xs text-secondary-500">
                  Runs every {automationStatus.connection_config.validation_automation.interval_hours} hours
                </p>
              )}

              {globalValidationEnabled && automationGloballyEnabled && (
                <div className="mt-2">
                  <button
                    onClick={onTriggerAutomatedRun}
                    className="text-xs text-primary-600 hover:text-primary-700 flex items-center"
                    title="Trigger validation automation now"
                  >
                    <PlayIcon className="h-3 w-3 mr-1" />
                    Run Now
                  </button>
                </div>
              )}
            </>
          )}
        </div>

        {/* Table-specific automation */}
        {tableName && (
          <div className="border-t pt-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-secondary-700">
                Auto-run for {tableName}
              </span>
              {loadingTableConfig ? (
                <div className="flex items-center px-2 py-1 rounded text-xs font-medium bg-secondary-100">
                  <div className="animate-spin h-3 w-3 border border-secondary-400 border-t-transparent rounded-full mr-1"></div>
                  <span className="text-secondary-500">Loading...</span>
                </div>
              ) : isLoadingAutomationStatus ? (
                // Show loading for table config too if main status is loading
                <div className="flex items-center px-2 py-1 rounded text-xs font-medium bg-secondary-100">
                  <div className="animate-spin h-3 w-3 border border-secondary-400 border-t-transparent rounded-full mr-1"></div>
                  <span className="text-secondary-500">Loading...</span>
                </div>
              ) : (
                <button
                  onClick={handleToggleTableAutomation}
                  disabled={!globalValidationEnabled || !automationGloballyEnabled || savingTableConfig}
                  className={`flex items-center px-2 py-1 rounded text-xs font-medium transition-all duration-200 ${
                    tableValidationEnabled && globalValidationEnabled && automationGloballyEnabled
                      ? 'bg-primary-100 text-primary-800 hover:bg-primary-200'
                      : 'bg-secondary-100 text-secondary-600 hover:bg-secondary-200'
                  } ${(!globalValidationEnabled || !automationGloballyEnabled) ? 'opacity-50 cursor-not-allowed' : ''}`}
                >
                  {savingTableConfig ? (
                    <>
                      <div className="animate-spin h-3 w-3 border border-current border-t-transparent rounded-full mr-1"></div>
                      Saving...
                    </>
                  ) : tableValidationEnabled ? (
                    <>
                      <PlayIcon className="h-3 w-3 mr-1" />
                      On
                    </>
                  ) : (
                    <>
                      <PauseIcon className="h-3 w-3 mr-1" />
                      Off
                    </>
                  )}
                </button>
              )}
            </div>

            {/* Only show table settings when both configs are loaded */}
            {!isLoadingAutomationStatus && !loadingTableConfig && tableValidationEnabled && globalValidationEnabled && automationGloballyEnabled && (
              <div className="space-y-3 mt-3">
                <div>
                  <label className="block text-xs font-medium text-secondary-700 mb-1">
                    Run Interval
                  </label>
                  <select
                    value={tableConfig?.auto_run_interval_hours || 24}
                    onChange={(e) => handleIntervalChange(e.target.value)}
                    disabled={savingTableConfig}
                    className="block w-full text-xs border-secondary-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500"
                  >
                    <option value={6}>Every 6 hours</option>
                    <option value={12}>Every 12 hours</option>
                    <option value={24}>Daily</option>
                    <option value={168}>Weekly</option>
                  </select>
                </div>

                <div>
                  <label className="block text-xs font-medium text-secondary-700 mb-1">
                    Notify On
                  </label>
                  <select
                    value={tableConfig?.validation_notification_threshold || 'failures_only'}
                    onChange={(e) => handleThresholdChange(e.target.value)}
                    disabled={savingTableConfig}
                    className="block w-full text-xs border-secondary-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500"
                  >
                    <option value="all">All results</option>
                    <option value="failures_only">Failures only</option>
                    <option value="none">No notifications</option>
                  </select>
                </div>
              </div>
            )}

            {/* Status messages - only show when not loading */}
            {!isLoadingAutomationStatus && (
              <>
                {!globalValidationEnabled && (
                  <div className="mt-2 p-2 bg-warning-50 border border-warning-200 rounded text-xs">
                    <div className="flex items-start">
                      <ExclamationCircleIcon className="h-3 w-3 text-warning-400 mr-1 mt-0.5 flex-shrink-0" />
                      <div>
                        <p className="text-warning-800">
                          Enable connection-wide validation automation first.
                        </p>
                      </div>
                    </div>
                  </div>
                )}

                {!automationGloballyEnabled && (
                  <div className="mt-2 p-2 bg-warning-50 border border-warning-200 rounded text-xs">
                    <div className="flex items-start">
                      <ExclamationCircleIcon className="h-3 w-3 text-warning-400 mr-1 mt-0.5 flex-shrink-0" />
                      <div>
                        <p className="text-warning-800">
                          Global automation is disabled.
                        </p>
                        <Link
                          to="/settings/automation"
                          className="text-warning-600 hover:text-warning-700 underline"
                        >
                          Enable in settings →
                        </Link>
                      </div>
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        )}

        {/* Last run information */}
        {!isLoadingAutomationStatus && automationStatus.last_runs?.validation_automation && (
          <div className="border-t pt-4">
            <div className="text-xs text-secondary-500">
              <div className="flex items-center">
                <ClipboardDocumentCheckIcon className="h-3 w-3 mr-1" />
                <span>
                  Last run: {new Date(automationStatus.last_runs.validation_automation.completed_at).toLocaleDateString()}
                </span>
              </div>
              <div className={`mt-1 text-xs ${
                automationStatus.last_runs.validation_automation.status === 'completed' 
                  ? 'text-accent-600' 
                  : 'text-danger-600'
              }`}>
                Status: {automationStatus.last_runs.validation_automation.status}
              </div>
            </div>
          </div>
        )}

        {/* Help text */}
        {!tableName && (
          <div className="border-t pt-4">
            <div className="text-xs text-secondary-500">
              <p>Select a table to configure table-specific automation settings.</p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default ValidationAutomationControls;