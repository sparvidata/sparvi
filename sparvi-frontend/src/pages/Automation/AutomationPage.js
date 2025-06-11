// COMPLETE REPLACEMENT for your AutomationPage.js

import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useAutomationConfig } from '../../hooks/useAutomationConfig';
import { useNextRunTimes } from '../../hooks/useNextRunTimes';
import { automationAPI } from '../../api/enhancedApiService';
import {
  ClockIcon,
  PlayIcon,
  PauseIcon,
  Cog6ToothIcon,
  ExclamationTriangleIcon,
  TableCellsIcon,
  ClipboardDocumentCheckIcon,
  CommandLineIcon,
  ChartBarIcon,
  EyeIcon,
  CalendarIcon,
  CheckCircleIcon,
  ExclamationCircleIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import EmptyState from '../../components/common/EmptyState';

const AutomationPage = () => {
  const { connections, activeConnection } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();
  const { globalConfig, connectionConfigs, loading, updateGlobalConfig, updateConnectionConfig } = useAutomationConfig();
  const { nextRuns: allNextRuns, loading: nextRunsLoading, refresh: refreshNextRuns } = useNextRunTimes(null);
  const [activeTab, setActiveTab] = useState('overview');
  const [automationJobs, setAutomationJobs] = useState([]);
  const [jobsLoading, setJobsLoading] = useState(true);

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Automation', href: '/automation' }
    ]);
  }, [updateBreadcrumbs]);

  // Load recent automation jobs
  useEffect(() => {
    const loadJobs = async () => {
      try {
        setJobsLoading(true);

        const response = await automationAPI.getJobs({
          limit: 20,
          forceFresh: true
        });

        if (response?.jobs) {
          setAutomationJobs(response.jobs);
        } else if (Array.isArray(response)) {
          setAutomationJobs(response);
        } else {
          setAutomationJobs([]);
        }
      } catch (error) {
        console.error('Error loading automation jobs:', error);
        setAutomationJobs([]);
      } finally {
        setJobsLoading(false);
      }
    };

    loadJobs();

    // Refresh jobs every 30 seconds
    const interval = setInterval(loadJobs, 30000);
    return () => clearInterval(interval);
  }, []);

  const toggleGlobalAutomation = async () => {
    if (!globalConfig) return;

    const newEnabled = !globalConfig.automation_enabled;

    try {
      const success = await updateGlobalConfig({
        ...globalConfig,
        automation_enabled: newEnabled
      });

      if (success) {
        showNotification(
          `Global automation ${newEnabled ? 'enabled' : 'disabled'}`,
          'success'
        );
        // Refresh next runs when global automation changes
        refreshNextRuns();
      } else {
        showNotification('Failed to update global automation setting', 'error');
      }
    } catch (error) {
      console.error('Error toggling global automation:', error);
      showNotification('Failed to update global automation setting', 'error');
    }
  };

  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <LoadingSpinner size="lg" />
        <span className="ml-3 text-secondary-600">Loading automation settings...</span>
      </div>
    );
  }

  if (connections.length === 0) {
    return (
      <EmptyState
        icon={ClockIcon}
        title="No connections available"
        description="Create database connections to configure automation"
        actionText="Add Connection"
        actionLink="/connections/new"
      />
    );
  }

  const activeJobs = automationJobs.filter(job => job.status === 'running' || job.status === 'scheduled');
  const recentJobs = automationJobs.slice(0, 10);

  return (
    <div className="py-4">
      <div className="flex justify-between items-center mb-6">
        <div>
          <h1 className="text-2xl font-semibold text-secondary-900">Automation Management</h1>
          <p className="mt-1 text-sm text-secondary-500">
            Configure and monitor automated processes for your database connections
          </p>
        </div>

        {/* Quick actions */}
        <div className="flex items-center space-x-3">
          {/* Global automation toggle */}
          <button
            onClick={toggleGlobalAutomation}
            className={`flex items-center px-4 py-2 rounded-md text-sm font-medium ${
              globalConfig?.automation_enabled
                ? 'bg-accent-100 text-accent-800 hover:bg-accent-200'
                : 'bg-secondary-100 text-secondary-600 hover:bg-secondary-200'
            }`}
          >
            {globalConfig?.automation_enabled ? (
              <>
                <PlayIcon className="h-4 w-4 mr-2" />
                Global Automation On
              </>
            ) : (
              <>
                <PauseIcon className="h-4 w-4 mr-2" />
                Global Automation Off
              </>
            )}
          </button>

          {/* Settings link */}
          <Link
            to="/settings/automation"
            className="flex items-center px-4 py-2 border border-primary-300 rounded-md text-sm font-medium text-primary-700 bg-white hover:bg-primary-50"
          >
            <Cog6ToothIcon className="h-4 w-4 mr-2" />
            Configure Settings
          </Link>
        </div>
      </div>

      {/* Global automation disabled warning */}
      {!globalConfig?.automation_enabled && (
        <div className="mb-6 rounded-md bg-warning-50 p-4">
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
                  All automated processes are currently paused. Enable global automation to resume scheduled tasks.
                </p>
              </div>
              <div className="mt-4">
                <div className="-mx-2 -my-1.5 flex">
                  <button
                    onClick={toggleGlobalAutomation}
                    className="bg-warning-50 px-2 py-1.5 rounded-md text-sm font-medium text-warning-800 hover:bg-warning-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-warning-50 focus:ring-warning-600"
                  >
                    Enable Global Automation
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Tab Navigation */}
      <div className="border-b border-secondary-200 mb-6">
        <nav className="-mb-px flex space-x-8" aria-label="Tabs">
          <button
            className={`
              ${activeTab === 'overview' 
                ? 'border-primary-500 text-primary-600' 
                : 'border-transparent text-secondary-500 hover:border-secondary-300 hover:text-secondary-700'}
              whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center
            `}
            onClick={() => setActiveTab('overview')}
          >
            <TableCellsIcon className="h-4 w-4 mr-2" />
            Configuration Overview
          </button>
          <button
            className={`
              ${activeTab === 'monitoring' 
                ? 'border-primary-500 text-primary-600' 
                : 'border-transparent text-secondary-500 hover:border-secondary-300 hover:text-secondary-700'}
              whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center
            `}
            onClick={() => setActiveTab('monitoring')}
          >
            <ChartBarIcon className="h-4 w-4 mr-2" />
            Monitoring & Schedules
            {activeJobs.length > 0 && (
              <span className="ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-primary-100 text-primary-800">
                {activeJobs.length}
              </span>
            )}
          </button>
        </nav>
      </div>

      {/* Tab Content */}
      {activeTab === 'overview' ? (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Connection Automation Overview */}
          <div className="lg:col-span-2">
            <div className="bg-white shadow rounded-lg">
              <div className="px-6 py-4 border-b border-secondary-200">
                <h3 className="text-lg font-medium text-secondary-900">Connection Automation</h3>
                <p className="mt-1 text-sm text-secondary-500">
                  Automation status and configuration for each database connection
                </p>
              </div>

              <div className="p-6">
                {connections.length === 0 ? (
                  <EmptyState
                    icon={ClockIcon}
                    title="No connections configured"
                    description="Add database connections to set up automation"
                    actionText="Add Connection"
                    actionLink="/connections/new"
                  />
                ) : (
                  <div className="space-y-4">
                    {connections.map(connection => (
                      <ConnectionAutomationSummary
                        key={connection.id}
                        connection={connection}
                        config={connectionConfigs[connection.id]}
                        nextRuns={allNextRuns[connection.id]?.next_runs || {}}
                        nextRunsLoading={nextRunsLoading}
                        onUpdateConfig={(config) => updateConnectionConfig(connection.id, config)}
                        globalEnabled={globalConfig?.automation_enabled}
                      />
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Sidebar with jobs and next runs */}
          <div className="lg:col-span-1 space-y-6">
            {/* Next Upcoming Runs */}
            <NextUpcomingRunsCard
              allNextRuns={allNextRuns}
              connections={connections}
              loading={nextRunsLoading}
            />

            {/* Active Jobs */}
            <div className="bg-white shadow rounded-lg">
              <div className="px-6 py-4 border-b border-secondary-200">
                <h3 className="text-lg font-medium text-secondary-900">
                  Active Jobs
                  {activeJobs.length > 0 && (
                    <span className="ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-primary-100 text-primary-800">
                      {activeJobs.length}
                    </span>
                  )}
                </h3>
              </div>

              <div className="p-6">
                {jobsLoading ? (
                  <div className="flex justify-center py-4">
                    <LoadingSpinner size="sm" />
                  </div>
                ) : activeJobs.length === 0 ? (
                  <div className="text-center py-4">
                    <ClockIcon className="mx-auto h-8 w-8 text-secondary-400" />
                    <p className="mt-2 text-sm text-secondary-500">No active jobs</p>
                  </div>
                ) : (
                  <div className="space-y-3">
                    {activeJobs.map(job => (
                      <AutomationJobSummary key={job.id} job={job} />
                    ))}
                  </div>
                )}
              </div>
            </div>

            {/* Recent Jobs History */}
            <div className="bg-white shadow rounded-lg">
              <div className="px-6 py-4 border-b border-secondary-200">
                <h3 className="text-lg font-medium text-secondary-900">Recent Jobs</h3>
              </div>

              <div className="p-6">
                {jobsLoading ? (
                  <div className="flex justify-center py-4">
                    <LoadingSpinner size="sm" />
                  </div>
                ) : recentJobs.length === 0 ? (
                  <div className="text-center py-4">
                    <ClockIcon className="mx-auto h-8 w-8 text-secondary-400" />
                    <p className="mt-2 text-sm text-secondary-500">No recent jobs</p>
                  </div>
                ) : (
                  <div className="space-y-3">
                    {recentJobs.map(job => (
                      <AutomationJobSummary key={job.id} job={job} compact />
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      ) : (
        // Monitoring Tab Content - placeholder for now
        <div>
          {activeConnection ? (
            <div className="bg-white shadow rounded-lg p-6">
              <h3 className="text-lg font-medium text-secondary-900 mb-4">
                Detailed Monitoring for {activeConnection.name}
              </h3>
              <p className="text-secondary-500">
                Enhanced monitoring dashboard coming soon...
              </p>
            </div>
          ) : (
            <EmptyState
              icon={ChartBarIcon}
              title="No connection selected"
              description="Please select a database connection to view automation monitoring"
              actionText="Manage Connections"
              actionLink="/connections"
            />
          )}
        </div>
      )}
    </div>
  );
};

// Connection automation summary component with next run times
const ConnectionAutomationSummary = ({ connection, config, onUpdateConfig, globalEnabled }) => {
  // Get next run times for this specific connection
  const { nextRuns, loading: nextRunsLoading, error: nextRunsError } = useNextRunTimes(connection.id, {
    enabled: !!connection.id,
    refreshInterval: 60000, // Refresh every minute
    onError: (error) => {
      console.error(`Error loading next runs for connection ${connection.id}:`, error);
    }
  });

  if (!config) {
    return (
      <div className="border border-secondary-200 rounded-lg p-4 bg-secondary-50">
        <div className="flex items-center justify-between">
          <h4 className="font-medium text-secondary-900">{connection.name}</h4>
          <Link
            to={`/settings/automation?connection=${connection.id}`}
            className="text-xs text-primary-600 hover:text-primary-700"
          >
            Configure
          </Link>
        </div>
        <p className="text-sm text-secondary-500 mt-1">No automation configured</p>
      </div>
    );
  }

  const hasEnabledAutomation =
    config.metadata_refresh?.enabled ||
    config.schema_change_detection?.enabled ||
    config.validation_automation?.enabled;

  // Helper to get next run display
  const getNextRunDisplay = (automationType) => {
    if (nextRunsLoading) {
      return <span className="text-xs text-secondary-400">Loading...</span>;
    }

    if (nextRunsError) {
      return <span className="text-xs text-danger-500">Error</span>;
    }

    const nextRun = nextRuns[automationType];
    if (!nextRun) {
      return <span className="text-xs text-secondary-400">Not scheduled</span>;
    }

    if (nextRun.currently_running) {
      return (
        <div className="text-xs flex items-center">
          <LoadingSpinner size="xs" className="mr-1" />
          <span className="text-primary-600">Running</span>
        </div>
      );
    }

    if (nextRun.is_overdue) {
      return <span className="text-xs text-warning-600 font-medium">Overdue</span>;
    }

    return (
      <span className="text-xs text-secondary-500">
        Next: {nextRun.time_until_next || 'Soon'}
      </span>
    );
  };

  // Helper to get status indicator color
  const getStatusColor = (automationType, isEnabled) => {
    if (!isEnabled) return 'text-secondary-400';

    const nextRun = nextRuns[automationType];
    if (nextRun?.currently_running) return 'text-primary-600';
    if (nextRun?.is_overdue) return 'text-warning-600';
    return 'text-primary-600';
  };

  return (
    <div className={`border rounded-lg p-4 ${
      hasEnabledAutomation && globalEnabled 
        ? 'border-primary-200 bg-primary-50' 
        : 'border-secondary-200 bg-white'
    }`}>
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center">
          <h4 className="font-medium text-secondary-900">{connection.name}</h4>
          {connection.is_default && (
            <span className="ml-2 inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-secondary-100 text-secondary-800">
              Default
            </span>
          )}
        </div>
        <div className="flex items-center space-x-2">
          {hasEnabledAutomation && globalEnabled ? (
            <span className="px-2 py-1 text-xs bg-accent-100 text-accent-800 rounded font-medium">
              Active
            </span>
          ) : hasEnabledAutomation ? (
            <span className="px-2 py-1 text-xs bg-warning-100 text-warning-800 rounded font-medium">
              Paused
            </span>
          ) : (
            <span className="px-2 py-1 text-xs bg-secondary-100 text-secondary-600 rounded">
              Inactive
            </span>
          )}
          <Link
            to={`/settings/automation?connection=${connection.id}`}
            className="text-xs text-primary-600 hover:text-primary-700"
          >
            Configure
          </Link>
        </div>
      </div>

      <div className="grid grid-cols-3 gap-4">
        {/* Metadata Refresh */}
        <div className="text-center">
          <div className="flex justify-center mb-2">
            <TableCellsIcon className={`h-6 w-6 ${
              getStatusColor('metadata_refresh', config.metadata_refresh?.enabled)
            }`} />
          </div>
          <div className="text-xs text-secondary-500 mb-1">Metadata</div>
          <div className={`text-sm font-medium ${
            config.metadata_refresh?.enabled 
              ? 'text-primary-700' 
              : 'text-secondary-400'
          }`}>
            {config.metadata_refresh?.enabled
              ? `Every ${config.metadata_refresh.interval_hours}h`
              : 'Off'
            }
          </div>
          {/* Next run time */}
          {config.metadata_refresh?.enabled && globalEnabled && (
            <div className="mt-1">
              {getNextRunDisplay('metadata_refresh')}
            </div>
          )}
        </div>

        {/* Schema Detection */}
        <div className="text-center">
          <div className="flex justify-center mb-2">
            <CommandLineIcon className={`h-6 w-6 ${
              getStatusColor('schema_change_detection', config.schema_change_detection?.enabled)
            }`} />
          </div>
          <div className="text-xs text-secondary-500 mb-1">Schema Detection</div>
          <div className={`text-sm font-medium ${
            config.schema_change_detection?.enabled 
              ? 'text-primary-700' 
              : 'text-secondary-400'
          }`}>
            {config.schema_change_detection?.enabled
              ? `Every ${config.schema_change_detection.interval_hours}h`
              : 'Off'
            }
          </div>
          {/* Next run time */}
          {config.schema_change_detection?.enabled && globalEnabled && (
            <div className="mt-1">
              {getNextRunDisplay('schema_change_detection')}
            </div>
          )}
        </div>

        {/* Validation Automation */}
        <div className="text-center">
          <div className="flex justify-center mb-2">
            <ClipboardDocumentCheckIcon className={`h-6 w-6 ${
              getStatusColor('validation_automation', config.validation_automation?.enabled)
            }`} />
          </div>
          <div className="text-xs text-secondary-500 mb-1">Validations</div>
          <div className={`text-sm font-medium ${
            config.validation_automation?.enabled 
              ? 'text-primary-700' 
              : 'text-secondary-400'
          }`}>
            {config.validation_automation?.enabled
              ? `Every ${config.validation_automation.interval_hours}h`
              : 'Off'
            }
          </div>
          {/* Next run time */}
          {config.validation_automation?.enabled && globalEnabled && (
            <div className="mt-1">
              {getNextRunDisplay('validation_automation')}
            </div>
          )}
        </div>
      </div>

      {/* Show error state if next runs failed to load */}
      {nextRunsError && hasEnabledAutomation && globalEnabled && (
        <div className="mt-3 text-xs text-danger-600 bg-danger-50 border border-danger-200 rounded p-2">
          Unable to load schedule information. Automation may still be running.
        </div>
      )}
    </div>
  );
};

// Next upcoming runs card component
const NextUpcomingRunsCard = ({ allNextRuns, connections, loading }) => {
  // Find all upcoming runs and sort by next run time
  const upcomingRuns = [];

  Object.entries(allNextRuns).forEach(([connectionId, connectionData]) => {
    const connection = connections.find(c => c.id === connectionId);
    if (!connection || !connectionData.next_runs) return;

    Object.entries(connectionData.next_runs).forEach(([automationType, runData]) => {
      if (runData.enabled && !runData.is_overdue && runData.next_run_timestamp) {
        upcomingRuns.push({
          connectionId,
          connectionName: connection.name,
          automationType,
          automationLabel: automationType.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase()),
          nextRunTimestamp: runData.next_run_timestamp,
          timeUntil: runData.time_until_next,
          currentlyRunning: runData.currently_running
        });
      }
    });
  });

  // Sort by next run time
  upcomingRuns.sort((a, b) => a.nextRunTimestamp - b.nextRunTimestamp);

  // Also find overdue runs
  const overdueRuns = [];
  Object.entries(allNextRuns).forEach(([connectionId, connectionData]) => {
    const connection = connections.find(c => c.id === connectionId);
    if (!connection || !connectionData.next_runs) return;

    Object.entries(connectionData.next_runs).forEach(([automationType, runData]) => {
      if (runData.enabled && runData.is_overdue) {
        overdueRuns.push({
          connectionId,
          connectionName: connection.name,
          automationType,
          automationLabel: automationType.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())
        });
      }
    });
  });

  return (
    <div className="bg-white shadow rounded-lg">
      <div className="px-6 py-4 border-b border-secondary-200">
        <h3 className="text-lg font-medium text-secondary-900 flex items-center">
          <CalendarIcon className="h-5 w-5 mr-2" />
          Upcoming Runs
          {overdueRuns.length > 0 && (
            <span className="ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-warning-100 text-warning-800">
              {overdueRuns.length} overdue
            </span>
          )}
        </h3>
      </div>

      <div className="p-6">
        {loading ? (
          <div className="flex justify-center py-4">
            <LoadingSpinner size="sm" />
          </div>
        ) : (
          <>
            {/* Overdue Runs */}
            {overdueRuns.length > 0 && (
              <div className="mb-4">
                <h4 className="text-sm font-medium text-warning-800 mb-2 flex items-center">
                  <ExclamationTriangleIcon className="h-4 w-4 mr-1" />
                  Overdue
                </h4>
                <div className="space-y-2">
                  {overdueRuns.slice(0, 3).map((run, index) => (
                    <div key={index} className="flex items-center justify-between py-2 px-3 bg-warning-50 border border-warning-200 rounded">
                      <div className="flex-1 min-w-0">
                        <div className="text-sm font-medium text-warning-900 truncate">
                          {run.automationLabel}
                        </div>
                        <div className="text-xs text-warning-700">
                          {run.connectionName}
                        </div>
                      </div>
                      <span className="text-xs font-medium text-warning-600">
                        Overdue
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Upcoming Runs */}
            {upcomingRuns.length > 0 ? (
              <div className="space-y-2">
                <h4 className="text-sm font-medium text-secondary-900 mb-2">
                  Next Scheduled
                </h4>
                {upcomingRuns.slice(0, 5).map((run, index) => (
                  <div key={index} className="flex items-center justify-between py-2">
                    <div className="flex-1 min-w-0">
                      <div className="text-sm font-medium text-secondary-900 truncate">
                        {run.automationLabel}
                      </div>
                      <div className="text-xs text-secondary-500">
                        {run.connectionName}
                      </div>
                    </div>
                    <div className="text-right">
                      {run.currentlyRunning ? (
                        <div className="flex items-center text-xs text-primary-600">
                          <LoadingSpinner size="xs" className="mr-1" />
                          Running
                        </div>
                      ) : (
                        <span className="text-xs text-secondary-500">
                          {run.timeUntil}
                        </span>
                      )}
                    </div>
                  </div>
                ))}
                {upcomingRuns.length > 5 && (
                  <div className="text-center text-xs text-secondary-400 py-2">
                    + {upcomingRuns.length - 5} more scheduled
                  </div>
                )}
              </div>
            ) : (
              <div className="text-center py-6">
                <CalendarIcon className="mx-auto h-8 w-8 text-secondary-400" />
                <h3 className="mt-2 text-sm font-medium text-secondary-900">No Upcoming Runs</h3>
                <p className="mt-1 text-sm text-secondary-500">
                  {Object.keys(allNextRuns).length === 0
                    ? 'Enable automation to see scheduled runs here.'
                    : 'All enabled automations are currently overdue.'
                  }
                </p>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
};

// Job summary component (unchanged from your existing implementation)
const AutomationJobSummary = ({ job, compact = false }) => {
  const getStatusColor = (status) => {
    switch (status) {
      case 'running': return 'text-primary-600';
      case 'completed': return 'text-accent-600';
      case 'failed': return 'text-danger-600';
      case 'scheduled': return 'text-secondary-600';
      default: return 'text-secondary-600';
    }
  };

  const getStatusBadge = (status) => {
    switch (status) {
      case 'running': return 'bg-primary-100 text-primary-800';
      case 'completed': return 'bg-accent-100 text-accent-800';
      case 'failed': return 'bg-danger-100 text-danger-800';
      case 'scheduled': return 'bg-secondary-100 text-secondary-600';
      default: return 'bg-secondary-100 text-secondary-600';
    }
  };

  const formatTimeAgo = (timestamp) => {
    if (!timestamp) return 'Unknown';

    const now = new Date();
    const then = new Date(timestamp);
    const diffMs = now - then;
    const diffMins = Math.floor(diffMs / 60000);

    if (diffMins < 60) return `${diffMins}m ago`;
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays}d ago`;
  };

  const formatJobType = (type) => {
    if (!type) return 'Unknown';
    return type.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase());
  };

  if (compact) {
    return (
      <div className="flex items-center justify-between py-2">
        <div className="flex-1 min-w-0">
          <div className="text-sm font-medium text-secondary-900 truncate">
            {formatJobType(job.job_type)}
          </div>
          <div className="text-xs text-secondary-500">
            {formatTimeAgo(job.created_at)}
          </div>
        </div>
        <div className={`text-xs font-medium ${getStatusColor(job.status)}`}>
          {job.status}
        </div>
      </div>
    );
  }

  return (
    <div className="border border-secondary-200 rounded p-3">
      <div className="flex items-center justify-between mb-2">
        <div className="text-sm font-medium text-secondary-900">
          {formatJobType(job.job_type)}
        </div>
        <span className={`px-2 py-1 text-xs font-medium rounded ${getStatusBadge(job.status)}`}>
          {job.status}
        </span>
      </div>

      {job.target_table && (
        <div className="text-xs text-secondary-500 mb-1">
          Table: {job.target_table}
        </div>
      )}

      <div className="text-xs text-secondary-500">
        {job.status === 'running' ? 'Started' : 'Created'}: {formatTimeAgo(job.started_at || job.created_at)}
      </div>

      {job.error_message && (
        <div className="mt-2 text-xs text-danger-600 bg-danger-50 p-2 rounded">
          {job.error_message}
        </div>
      )}
    </div>
  );
};

export default AutomationPage;