import React, { useState, useEffect } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useAutomationJobCount } from '../../hooks/useAutomationJobCount';
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
  CalendarIcon,
  CheckCircleIcon,
  ExclamationCircleIcon,
  CogIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import EmptyState from '../../components/common/EmptyState';
import { ScheduleConfig, NextRunDisplay, ScheduleStatusWidget } from '../../components/automation';
import { useNextRunTimes } from '../../hooks/useNextRunTimes';
import ErrorBoundary from '../../components/ErrorBoundary';

const AutomationPage = () => {
  const { connections, activeConnection } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();
  const { activeJobCount } = useAutomationJobCount();
  const [searchParams, setSearchParams] = useSearchParams();
  const [activeTab, setActiveTab] = useState('overview');
  const [selectedConnectionId, setSelectedConnectionId] = useState(null);
  const [globalConfig, setGlobalConfig] = useState(null);
  const [globalConfigLoading, setGlobalConfigLoading] = useState(true);
  const [automationJobs, setAutomationJobs] = useState([]);
  const [jobsLoading, setJobsLoading] = useState(true);

  // Get all next run times for overview
  const { nextRuns: allNextRuns, loading: nextRunsLoading, refresh: refreshNextRuns } = useNextRunTimes(null, {
    refreshInterval: 120000 // 2 minutes for overview page
  });

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Automation', href: '/automation' }
    ]);
  }, [updateBreadcrumbs]);

  // Handle URL parameters for direct configuration access
  useEffect(() => {
    const connectionParam = searchParams.get('connection');
    const tabParam = searchParams.get('tab');

    if (connectionParam && connections.length > 0) {
      const connection = connections.find(c => c.id === connectionParam);
      if (connection) {
        setSelectedConnectionId(connectionParam);
        setActiveTab('configure');
      }
    }

    if (tabParam && ['overview', 'configure', 'monitoring'].includes(tabParam)) {
      setActiveTab(tabParam);
    }
  }, [searchParams, connections]);

  // Load global automation config
  useEffect(() => {
    const loadGlobalConfig = async () => {
      try {
        const response = await automationAPI.getGlobalConfig();
        setGlobalConfig(response?.config || { automation_enabled: false });
      } catch (error) {
        console.error('Error loading global config:', error);
        setGlobalConfig({ automation_enabled: false });
      } finally {
        setGlobalConfigLoading(false);
      }
    };

    loadGlobalConfig();
  }, []);

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
    if (!globalConfig || globalConfigLoading) return;

    const newEnabled = !globalConfig.automation_enabled;

    try {
      const response = await automationAPI.toggleGlobalAutomation(newEnabled);

      if (response?.success || response?.config) {
        setGlobalConfig(prev => ({
          ...prev,
          automation_enabled: newEnabled
        }));

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

  const handleTabChange = (tab) => {
    setActiveTab(tab);
    // Update URL parameters
    const newParams = new URLSearchParams(searchParams);
    newParams.set('tab', tab);
    if (tab !== 'configure') {
      newParams.delete('connection');
      setSelectedConnectionId(null);
    }
    setSearchParams(newParams);
  };

  const handleConnectionSelect = (connectionId) => {
    setSelectedConnectionId(connectionId);
    setActiveTab('configure');
    // Update URL parameters
    const newParams = new URLSearchParams(searchParams);
    newParams.set('connection', connectionId);
    newParams.set('tab', 'configure');
    setSearchParams(newParams);
  };

  if (globalConfigLoading) {
    return (
      <div className="flex justify-center py-12">
        <LoadingSpinner size="lg" />
        <span className="ml-3 text-gray-600">Loading automation settings...</span>
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
    <ErrorBoundary
      onError={(error, errorInfo) => {
        console.error('Automation page error:', error, errorInfo);
        // Optional: send to error reporting service
      }}
    >
      <div className="py-4">
        <div className="flex justify-between items-center mb-6">
          <div>
            <h1 className="text-2xl font-semibold text-gray-900">Automation Management</h1>
            <p className="mt-1 text-sm text-gray-500">
              Configure and monitor automated processes for your database connections
            </p>
          </div>

          {/* Quick actions */}
          <div className="flex items-center space-x-3">
            {/* Active jobs indicator */}
            {activeJobCount > 0 && (
              <div className="flex items-center px-3 py-2 bg-blue-50 text-blue-700 rounded-md text-sm">
                <LoadingSpinner size="xs" className="mr-2" />
                {activeJobCount} active job{activeJobCount !== 1 ? 's' : ''}
              </div>
            )}

            {/* Global automation toggle */}
            <button
              onClick={toggleGlobalAutomation}
              disabled={globalConfigLoading}
              className={`flex items-center px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                globalConfig?.automation_enabled
                  ? 'bg-green-100 text-green-800 hover:bg-green-200'
                  : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
              } disabled:opacity-50 disabled:cursor-not-allowed`}
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
          </div>
        </div>

        {/* Global automation disabled warning */}
        {!globalConfig?.automation_enabled && (
          <div className="mb-6 rounded-md bg-yellow-50 border border-yellow-200 p-4">
            <div className="flex">
              <div className="flex-shrink-0">
                <ExclamationTriangleIcon className="h-5 w-5 text-yellow-400" aria-hidden="true" />
              </div>
              <div className="ml-3">
                <h3 className="text-sm font-medium text-yellow-800">
                  Global automation is disabled
                </h3>
                <div className="mt-2 text-sm text-yellow-700">
                  <p>
                    All automated processes are currently paused. Enable global automation to resume scheduled tasks.
                  </p>
                </div>
                <div className="mt-4">
                  <div className="-mx-2 -my-1.5 flex">
                    <button
                      onClick={toggleGlobalAutomation}
                      className="bg-yellow-50 px-2 py-1.5 rounded-md text-sm font-medium text-yellow-800 hover:bg-yellow-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-yellow-50 focus:ring-yellow-600"
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
        <div className="border-b border-gray-200 mb-6">
          <nav className="-mb-px flex space-x-8" aria-label="Tabs">
            <button
              className={`
                ${activeTab === 'overview' 
                  ? 'border-blue-500 text-blue-600' 
                  : 'border-transparent text-gray-500 hover:border-gray-300 hover:text-gray-700'}
                whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center
              `}
              onClick={() => handleTabChange('overview')}
            >
              <TableCellsIcon className="h-4 w-4 mr-2" />
              Overview
            </button>
            <button
              className={`
                ${activeTab === 'configure' 
                  ? 'border-blue-500 text-blue-600' 
                  : 'border-transparent text-gray-500 hover:border-gray-300 hover:text-gray-700'}
                whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center
              `}
              onClick={() => handleTabChange('configure')}
            >
              <CogIcon className="h-4 w-4 mr-2" />
              Configure Schedules
            </button>
            <button
              className={`
                ${activeTab === 'monitoring' 
                  ? 'border-blue-500 text-blue-600' 
                  : 'border-transparent text-gray-500 hover:border-gray-300 hover:text-gray-700'}
                whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center
              `}
              onClick={() => handleTabChange('monitoring')}
            >
              <ChartBarIcon className="h-4 w-4 mr-2" />
              Jobs & Monitoring
              {activeJobs.length > 0 && (
                <span className="ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                  {activeJobs.length}
                </span>
              )}
            </button>
          </nav>
        </div>

        {/* Tab Content - Each wrapped in its own ErrorBoundary */}
        {activeTab === 'overview' && (
          <ErrorBoundary fallback={<div className="p-4 text-center text-red-600">Error loading overview. Please refresh the page.</div>}>
            <OverviewTab
              connections={connections}
              allNextRuns={allNextRuns}
              nextRunsLoading={nextRunsLoading}
              globalEnabled={globalConfig?.automation_enabled}
              onConnectionSelect={handleConnectionSelect}
            />
          </ErrorBoundary>
        )}

        {activeTab === 'configure' && (
          <ErrorBoundary fallback={<div className="p-4 text-center text-red-600">Error loading configuration. Please refresh the page.</div>}>
            <ConfigureTab
              connections={connections}
              selectedConnectionId={selectedConnectionId}
              onConnectionSelect={handleConnectionSelect}
            />
          </ErrorBoundary>
        )}

        {activeTab === 'monitoring' && (
          <ErrorBoundary fallback={<div className="p-4 text-center text-red-600">Error loading monitoring. Please refresh the page.</div>}>
            <MonitoringTab
              connections={connections}
              jobs={automationJobs}
              jobsLoading={jobsLoading}
              activeJobs={activeJobs}
              recentJobs={recentJobs}
            />
          </ErrorBoundary>
        )}
      </div>
    </ErrorBoundary>
  );
};

// Overview Tab Component
const OverviewTab = ({ connections, allNextRuns, nextRunsLoading, globalEnabled, onConnectionSelect }) => {
  // Calculate summary statistics
  const totalConnections = connections.length;
  const connectionsWithAutomation = Object.keys(allNextRuns).length;

  // Get upcoming runs across all connections
  const allUpcomingRuns = [];
  Object.entries(allNextRuns).forEach(([connectionId, connectionData]) => {
    const connection = connections.find(c => c.id === connectionId);
    if (connection && connectionData.next_runs) {
      Object.entries(connectionData.next_runs).forEach(([automationType, runData]) => {
        if (runData.enabled && !runData.is_overdue) {
          allUpcomingRuns.push({
            connectionId,
            connectionName: connection.name,
            automationType,
            ...runData
          });
        }
      });
    }
  });

  // Sort by next run time
  allUpcomingRuns.sort((a, b) => (a.next_run_timestamp || Infinity) - (b.next_run_timestamp || Infinity));

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
      {/* Connection Status Overview */}
      <div className="lg:col-span-2">
        <div className="bg-white shadow rounded-lg">
          <div className="px-6 py-4 border-b border-gray-200">
            <h3 className="text-lg font-medium text-gray-900">Connection Automation Status</h3>
            <p className="mt-1 text-sm text-gray-500">
              Schedule status for each database connection
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
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {connections.map(connection => (
                  <div key={connection.id} className="relative">
                    <ScheduleStatusWidget
                      connectionId={connection.id}
                      connectionName={connection.name}
                      className="h-full"
                    />
                    <button
                      onClick={() => onConnectionSelect(connection.id)}
                      className="absolute inset-0 w-full h-full bg-transparent hover:bg-blue-50 hover:bg-opacity-50 rounded-lg transition-colors"
                      title={`Configure automation for ${connection.name}`}
                    />
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Sidebar */}
      <div className="lg:col-span-1 space-y-6">
        {/* Summary Stats */}
        <div className="bg-white shadow rounded-lg p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-4">Summary</h3>
          <div className="space-y-4">
            <div className="flex justify-between">
              <span className="text-sm text-gray-500">Total Connections</span>
              <span className="text-sm font-medium text-gray-900">{totalConnections}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-sm text-gray-500">With Automation</span>
              <span className="text-sm font-medium text-gray-900">{connectionsWithAutomation}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-sm text-gray-500">Upcoming Runs</span>
              <span className="text-sm font-medium text-gray-900">{allUpcomingRuns.length}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-sm text-gray-500">Global Status</span>
              <span className={`text-sm font-medium ${
                globalEnabled ? 'text-green-600' : 'text-gray-500'
              }`}>
                {globalEnabled ? 'Enabled' : 'Disabled'}
              </span>
            </div>
          </div>
        </div>

        {/* Next Upcoming Runs */}
        <div className="bg-white shadow rounded-lg">
          <div className="px-6 py-4 border-b border-gray-200">
            <h3 className="text-lg font-medium text-gray-900 flex items-center">
              <CalendarIcon className="h-5 w-5 mr-2" />
              Next Runs
            </h3>
          </div>

          <div className="p-6">
            {nextRunsLoading ? (
              <div className="flex justify-center py-4">
                <LoadingSpinner size="sm" />
              </div>
            ) : allUpcomingRuns.length === 0 ? (
              <div className="text-center py-4">
                <CalendarIcon className="mx-auto h-8 w-8 text-gray-400" />
                <p className="mt-2 text-sm text-gray-500">
                  {globalEnabled ? 'No runs scheduled' : 'Enable global automation to see scheduled runs'}
                </p>
              </div>
            ) : (
              <div className="space-y-3">
                {allUpcomingRuns.slice(0, 5).map((run, index) => (
                  <div key={index} className="flex justify-between items-center py-2">
                    <div className="flex-1 min-w-0">
                      <div className="text-sm font-medium text-gray-900 truncate">
                        {run.automationType.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}
                      </div>
                      <div className="text-xs text-gray-500 truncate">
                        {run.connectionName}
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-xs text-green-600 font-medium">
                        {run.time_until_next}
                      </div>
                    </div>
                  </div>
                ))}
                {allUpcomingRuns.length > 5 && (
                  <div className="text-center text-xs text-gray-400 py-2">
                    + {allUpcomingRuns.length - 5} more scheduled
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

// Configure Tab Component
const ConfigureTab = ({ connections, selectedConnectionId, onConnectionSelect }) => {
  const selectedConnection = selectedConnectionId
    ? connections.find(c => c.id === selectedConnectionId)
    : null;

  return (
    <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
      {/* Connection Selector */}
      <div className="lg:col-span-1">
        <div className="bg-white shadow rounded-lg">
          <div className="px-4 py-3 border-b border-gray-200">
            <h3 className="text-sm font-medium text-gray-900">Select Connection</h3>
          </div>
          <div className="p-4">
            <div className="space-y-2">
              {connections.map(connection => (
                <button
                  key={connection.id}
                  onClick={() => onConnectionSelect(connection.id)}
                  className={`w-full text-left px-3 py-2 rounded-md text-sm transition-colors ${
                    selectedConnectionId === connection.id
                      ? 'bg-blue-100 text-blue-900 border border-blue-300'
                      : 'hover:bg-gray-100 text-gray-700'
                  }`}
                >
                  <div className="font-medium">{connection.name}</div>
                  {connection.is_default && (
                    <div className="text-xs text-gray-500">Default</div>
                  )}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Schedule Configuration */}
      <div className="lg:col-span-3">
        {selectedConnection ? (
          <div className="bg-white shadow rounded-lg p-6">
            <div className="mb-6">
              <h3 className="text-lg font-medium text-gray-900">
                Configure Automation for {selectedConnection.name}
              </h3>
              <p className="mt-1 text-sm text-gray-500">
                Set up automated schedules for this database connection
              </p>
            </div>

            <ScheduleConfig
              connectionId={selectedConnectionId}
              onUpdate={(updatedSchedule) => {
                console.log('Schedule updated:', updatedSchedule);
              }}
            />
          </div>
        ) : (
          <div className="bg-white shadow rounded-lg p-6">
            <EmptyState
              icon={CogIcon}
              title="Select a connection"
              description="Choose a database connection from the left to configure automation schedules"
            />
          </div>
        )}
      </div>
    </div>
  );
};

// Monitoring Tab Component
const MonitoringTab = ({ connections, jobs, jobsLoading, activeJobs, recentJobs }) => {
  const getJobTypeIcon = (jobType) => {
    switch (jobType) {
      case 'metadata_refresh': return TableCellsIcon;
      case 'schema_change_detection': return CommandLineIcon;
      case 'validation_automation': return ClipboardDocumentCheckIcon;
      default: return ClockIcon;
    }
  };

  const getStatusBadge = (status) => {
    switch (status) {
      case 'running': return 'bg-blue-100 text-blue-800';
      case 'completed': return 'bg-green-100 text-green-800';
      case 'failed': return 'bg-red-100 text-red-800';
      case 'scheduled': return 'bg-gray-100 text-gray-600';
      default: return 'bg-gray-100 text-gray-600';
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

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
      {/* Active Jobs */}
      <div className="lg:col-span-2">
        <div className="bg-white shadow rounded-lg">
          <div className="px-6 py-4 border-b border-gray-200">
            <h3 className="text-lg font-medium text-gray-900">
              Recent Automation Jobs
            </h3>
            <p className="mt-1 text-sm text-gray-500">
              Latest automation executions across all connections
            </p>
          </div>

          <div className="p-6">
            {jobsLoading ? (
              <div className="flex justify-center py-8">
                <LoadingSpinner size="lg" />
              </div>
            ) : jobs.length === 0 ? (
              <EmptyState
                icon={ClockIcon}
                title="No jobs found"
                description="Automation jobs will appear here once they start running"
              />
            ) : (
              <div className="space-y-4">
                {jobs.map(job => {
                  const IconComponent = getJobTypeIcon(job.job_type);
                  const connection = connections.find(c => c.id === job.connection_id);

                  return (
                    <div key={job.id} className="border border-gray-200 rounded-lg p-4">
                      <div className="flex items-center justify-between mb-3">
                        <div className="flex items-center">
                          <IconComponent className="h-5 w-5 text-gray-400 mr-3" />
                          <div>
                            <div className="text-sm font-medium text-gray-900">
                              {job.job_type?.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase()) || 'Unknown Job'}
                            </div>
                            <div className="text-xs text-gray-500">
                              {connection ? connection.name : 'Unknown Connection'}
                              {job.target_table && ` • ${job.target_table}`}
                            </div>
                          </div>
                        </div>

                        <span className={`px-2 py-1 text-xs font-medium rounded ${getStatusBadge(job.status)}`}>
                          {job.status}
                        </span>
                      </div>

                      <div className="text-xs text-gray-500 flex items-center justify-between">
                        <span>
                          {job.status === 'running' ? 'Started' : 'Created'}: {formatTimeAgo(job.started_at || job.created_at)}
                        </span>

                        {job.status === 'running' && (
                          <div className="flex items-center">
                            <LoadingSpinner size="xs" className="mr-1" />
                            <span>In progress...</span>
                          </div>
                        )}
                      </div>

                      {job.error_message && (
                        <div className="mt-3 text-xs text-red-600 bg-red-50 p-2 rounded">
                          {job.error_message}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Sidebar with active jobs and stats */}
      <div className="lg:col-span-1 space-y-6">
        {/* Active Jobs Count */}
        <div className="bg-white shadow rounded-lg p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-4">Active Jobs</h3>
          <div className="text-center">
            <div className="text-3xl font-bold text-blue-600">{activeJobs.length}</div>
            <p className="text-sm text-gray-500">Currently running</p>
          </div>

          {activeJobs.length > 0 && (
            <div className="mt-4 space-y-2">
              {activeJobs.slice(0, 3).map(job => (
                <div key={job.id} className="flex items-center text-sm">
                  <LoadingSpinner size="xs" className="mr-2" />
                  <span className="truncate">
                    {job.job_type?.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}
                  </span>
                </div>
              ))}
              {activeJobs.length > 3 && (
                <div className="text-xs text-gray-500 text-center">
                  +{activeJobs.length - 3} more
                </div>
              )}
            </div>
          )}
        </div>

        {/* Quick Stats */}
        <div className="bg-white shadow rounded-lg p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-4">Job Statistics</h3>
          <div className="space-y-3">
            <div className="flex justify-between">
              <span className="text-sm text-gray-500">Total Jobs (24h)</span>
              <span className="text-sm font-medium text-gray-900">{jobs.length}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-sm text-gray-500">Completed</span>
              <span className="text-sm font-medium text-green-600">
                {jobs.filter(j => j.status === 'completed').length}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-sm text-gray-500">Failed</span>
              <span className="text-sm font-medium text-red-600">
                {jobs.filter(j => j.status === 'failed').length}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-sm text-gray-500">Running</span>
              <span className="text-sm font-medium text-blue-600">
                {jobs.filter(j => j.status === 'running').length}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AutomationPage;