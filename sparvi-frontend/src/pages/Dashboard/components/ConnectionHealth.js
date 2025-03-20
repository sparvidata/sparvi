import React from 'react';
import { Link } from 'react-router-dom';
import { ArrowPathIcon, CheckCircleIcon, ExclamationCircleIcon } from '@heroicons/react/24/outline';
import { useConnection } from '../../../contexts/EnhancedConnectionContext';
import { useUI } from '../../../contexts/UIContext';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { useMetadataStatus, useRefreshMetadata } from '../../../hooks/useMetadataStatus';

const ConnectionHealth = () => {
  const { activeConnection } = useConnection();
  const { showNotification } = useUI();

  // Get connection ID safely
  const connectionId = activeConnection?.id;

  // Use the metadata status hook - this will automatically poll for updates
  const {
    data: status,
    isLoading,
    error,
    refetch: refetchStatus
  } = useMetadataStatus(connectionId, {
    // If there are active tasks, poll more frequently
    refetchInterval: (data) => {
      return (data?.pending_tasks?.length > 0) ? 5000 : 30000;
    }
  });

  // Use the refresh metadata mutation
  const {
    mutate: refreshMetadata,
    isPending: isRefreshing
  } = useRefreshMetadata(connectionId);

  // Handle refresh metadata - use the mutation from our hook
  const handleRefresh = () => {
    if (!connectionId) return;

    refreshMetadata('full', {
      onSuccess: () => {
        showNotification('Metadata refresh initiated', 'success');
        // Force an immediate refetch of status
        refetchStatus();
      },
      onError: (error) => {
        console.error('Error refreshing metadata:', error);
        showNotification('Failed to refresh metadata', 'error');
      }
    });
  };

  // Format the date
  const formatDate = (dateString) => {
    if (!dateString) return 'Never';

    try {
      const date = new Date(dateString);
      // Check if date is valid
      if (isNaN(date.getTime())) return 'Invalid date';
      return date.toLocaleString();
    } catch (error) {
      console.error('Error formatting date:', error);
      return 'Error';
    }
  };

  // Get status badge
  const getStatusBadge = (statusValue) => {
    // Default to 'unknown' if status is undefined
    const currentStatus = statusValue || 'unknown';

    const statusColors = {
      fresh: 'bg-success-100 text-success-800',     // Green for fresh (< 1 hour)
      recent: 'bg-accent-100 text-accent-800',      // Blue for recent (< 1 day)
      stale: 'bg-warning-100 text-warning-800',     // Yellow/orange for stale (> 1 day)
      unknown: 'bg-secondary-100 text-secondary-800',
      error: 'bg-danger-100 text-danger-800'
    };

    const statusLabels = {
      fresh: 'Fresh',       // Less than 1 hour old
      recent: 'Recent',     // Less than 1 day old
      stale: 'Stale',       // More than 1 day old
      unknown: 'Unknown',
      error: 'Error'
    };

    return (
      <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${statusColors[currentStatus] || statusColors.unknown}`}>
        {(currentStatus === 'fresh' || currentStatus === 'recent') ? (
          <CheckCircleIcon className="-ml-0.5 mr-1.5 h-3 w-3 text-current" aria-hidden="true" />
        ) : currentStatus === 'error' ? (
          <ExclamationCircleIcon className="-ml-0.5 mr-1.5 h-3 w-3 text-current" aria-hidden="true" />
        ) : null}
        {statusLabels[currentStatus] || 'Unknown'}
      </span>
    );
  };

  // Get active tasks from status
  const activeTasks = status?.pending_tasks?.filter(
    task => task.status === "pending" || task.status === "running"
  ) || [];

  // Check if we have an active connection
  if (!connectionId) {
    return (
      <div className="card px-4 py-5 sm:p-6">
        <p className="text-sm text-secondary-500">No active connection selected</p>
        <Link
          to="/connections"
          className="mt-2 inline-flex items-center px-3 py-1.5 border border-transparent text-xs font-medium rounded-md text-primary-700 bg-primary-100 hover:bg-primary-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
        >
          Manage connections
        </Link>
      </div>
    );
  }

  // Handle loading or error states
  if (error && !isLoading) {
    return (
      <div className="card px-4 py-5 sm:p-6 bg-danger-50 text-danger-700">
        <p>Error loading connection health data</p>
        <button
          onClick={() => refetchStatus()}
          className="mt-2 inline-flex items-center px-3 py-1.5 border border-transparent text-xs font-medium rounded-md text-white bg-danger-600 hover:bg-danger-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-danger-500"
        >
          Retry
        </button>
      </div>
    );
  }

  return (
    <div className="card overflow-hidden">
      <div className="px-4 py-5 sm:px-6 flex justify-between items-center bg-white border-b border-secondary-200">
        <div>
          <h3 className="text-lg leading-6 font-medium text-secondary-900">Connection Health</h3>
          <p className="mt-1 max-w-2xl text-sm text-secondary-500">
            {activeConnection.name}
            {activeConnection.is_default && (
              <span className="ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-primary-100 text-primary-800">
                Default
              </span>
            )}
          </p>
        </div>
        <button
          onClick={handleRefresh}
          disabled={isRefreshing}
          className="inline-flex items-center px-3 py-1.5 border border-transparent text-xs font-medium rounded-md text-primary-700 bg-primary-100 hover:bg-primary-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
        >
          {isRefreshing ? (
            <>
              <ArrowPathIcon className="animate-spin -ml-0.5 mr-2 h-4 w-4" aria-hidden="true" />
              Refreshing...
            </>
          ) : (
            <>
              <ArrowPathIcon className="-ml-0.5 mr-2 h-4 w-4" aria-hidden="true" />
              Refresh Metadata
            </>
          )}
        </button>
      </div>
      <div className="bg-white px-4 py-5 sm:p-6">
        {isLoading && !status ? (
          <div className="flex justify-center py-4">
            <LoadingSpinner size="md" />
            <span className="ml-2 text-secondary-500">Loading metadata status...</span>
          </div>
        ) : (
          <dl className="grid grid-cols-1 gap-x-4 gap-y-6 sm:grid-cols-2 lg:grid-cols-4">
            <div className="sm:col-span-1">
              <dt className="text-sm font-medium text-secondary-500">Tables Metadata</dt>
              <dd className="mt-1 text-sm text-secondary-900 flex items-center justify-between">
                <span>{getStatusBadge(status?.tables?.freshness?.status)}</span>
                <span className="text-xs text-secondary-500">
                  {formatDate(status?.tables?.last_updated)}
                </span>
              </dd>
            </div>
            <div className="sm:col-span-1">
              <dt className="text-sm font-medium text-secondary-500">Columns Metadata</dt>
              <dd className="mt-1 text-sm text-secondary-900 flex items-center justify-between">
                <span>{getStatusBadge(status?.columns?.freshness?.status)}</span>
                <span className="text-xs text-secondary-500">
                  {formatDate(status?.columns?.last_updated)}
                </span>
              </dd>
            </div>
            <div className="sm:col-span-1">
              <dt className="text-sm font-medium text-secondary-500">Statistics Metadata</dt>
              <dd className="mt-1 text-sm text-secondary-900 flex items-center justify-between">
                <span>{getStatusBadge(status?.statistics?.freshness?.status)}</span>
                <span className="text-xs text-secondary-500">
                  {formatDate(status?.statistics?.last_updated)}
                </span>
              </dd>
            </div>
            <div className="sm:col-span-1">
              <dt className="text-sm font-medium text-secondary-500">Pending Tasks</dt>
              <dd className="mt-1 text-sm text-secondary-900 flex items-center">
                <span>{activeTasks.length || 0} tasks pending</span>
                {activeTasks.length > 0 && <LoadingSpinner size="xs" className="ml-2" />}
              </dd>
            </div>
          </dl>
        )}

        {/* Task status section - show when tasks are present */}
        {activeTasks.length > 0 && (
          <div className="mt-6 border-t border-secondary-200 pt-4">
            <h4 className="text-sm font-medium text-secondary-700 mb-2">Active Tasks</h4>
            <ul className="divide-y divide-secondary-100">
              {activeTasks.map((task, index) => (
                <li key={task.id || index} className="py-2 flex justify-between">
                  <div className="flex items-center">
                    <LoadingSpinner size="xs" className="mr-2" />
                    <span className="text-sm text-secondary-600">
                      {task.task_type || 'Unknown task'}
                      {task.object_name && ` - ${task.object_name}`}
                    </span>
                  </div>
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-primary-100 text-primary-700">
                    {task.status || 'pending'}
                  </span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </div>
  );
};

export default ConnectionHealth;