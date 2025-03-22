import React from 'react';
import { ArrowPathIcon, CheckCircleIcon, ExclamationCircleIcon } from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

const MetadataStatusPanel = ({ metadataStatus, isLoading, onRefresh }) => {
  // Format the date
  const formatDate = (dateString) => {
    if (!dateString) return 'Never';

    try {
      const date = new Date(dateString);
      return date.toLocaleString();
    } catch (error) {
      return 'Invalid date';
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

  // If still loading and no status data, show spinner
  if (isLoading && !metadataStatus) {
    return (
      <div className="px-4 py-5 sm:px-6 flex items-center justify-center">
        <LoadingSpinner size="md" />
        <span className="ml-2 text-secondary-500">Loading metadata status...</span>
      </div>
    );
  }

  return (
    <div className="px-4 py-5 sm:px-6">
      <div className="flex items-center justify-between">
        <h3 className="text-lg leading-6 font-medium text-secondary-900">Metadata Status</h3>
        <button
          onClick={onRefresh}
          className="inline-flex items-center px-3 py-1.5 border border-secondary-300 shadow-sm text-sm font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
        >
          <ArrowPathIcon className="-ml-1 mr-2 h-4 w-4 text-secondary-500" aria-hidden="true" />
          Refresh Status
        </button>
      </div>

      <div className="mt-4 border-t border-b border-secondary-200 py-4">
        <dl className="grid grid-cols-1 gap-x-4 gap-y-6 sm:grid-cols-3">
          <div className="sm:col-span-1">
            <dt className="text-sm font-medium text-secondary-500">Tables Metadata</dt>
            <dd className="mt-1 text-sm text-secondary-900 flex items-center justify-between">
              <span>{getStatusBadge(metadataStatus?.tables?.freshness?.status)}</span>
              <span className="text-xs text-secondary-500">
                {formatDate(metadataStatus?.tables?.last_updated)}
              </span>
            </dd>
          </div>
          <div className="sm:col-span-1">
            <dt className="text-sm font-medium text-secondary-500">Columns Metadata</dt>
            <dd className="mt-1 text-sm text-secondary-900 flex items-center justify-between">
              <span>{getStatusBadge(metadataStatus?.columns?.freshness?.status)}</span>
              <span className="text-xs text-secondary-500">
                {formatDate(metadataStatus?.columns?.last_updated)}
              </span>
            </dd>
          </div>
          <div className="sm:col-span-1">
            <dt className="text-sm font-medium text-secondary-500">Statistics Metadata</dt>
            <dd className="mt-1 text-sm text-secondary-900 flex items-center justify-between">
              <span>{getStatusBadge(metadataStatus?.statistics?.freshness?.status)}</span>
              <span className="text-xs text-secondary-500">
                {formatDate(metadataStatus?.statistics?.last_updated)}
              </span>
            </dd>
          </div>
        </dl>
      </div>

      {/* Recent changes */}
      {metadataStatus?.changes_detected > 0 && (
        <div className="mt-3">
          <div className="flex items-center">
            <span className="px-2.5 py-0.5 rounded-full text-xs font-medium bg-warning-100 text-warning-800">
              {metadataStatus.changes_detected} schema {metadataStatus.changes_detected === 1 ? 'change' : 'changes'} detected
            </span>
          </div>
        </div>
      )}
    </div>
  );
};

export default MetadataStatusPanel;