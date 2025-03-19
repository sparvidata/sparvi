import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { ArrowPathIcon, CheckCircleIcon, ExclamationCircleIcon } from '@heroicons/react/24/outline';
import { metadataAPI } from '../../../api/enhancedApiService';
import { useUI } from '../../../contexts/UIContext';

const ConnectionHealth = ({ connection }) => {
  const [status, setStatus] = useState({
    tables: { status: 'unknown', last_updated: null },
    columns: { status: 'unknown', last_updated: null },
    statistics: { status: 'unknown', last_updated: null },
    pending_tasks: []
  });
  const [loading, setLoading] = useState(true);
  const { showNotification } = useUI();

  useEffect(() => {
    const fetchStatus = async () => {
      if (!connection) return;

      try {
        setLoading(true);
        const response = await metadataAPI.getMetadataStatus(connection.id);
        setStatus(response.data);
      } catch (error) {
        console.error('Error fetching metadata status:', error);

        // Only show notification for non-cancellation errors
        if (!error.cancelled) {
          showNotification('Failed to load connection health data', 'error');
        }
      } finally {
        setLoading(false);
      }
    };

    fetchStatus();
  }, [connection, showNotification]);

  // Handle refresh metadata
  const handleRefresh = async () => {
    if (!connection) return;

    try {
      setLoading(true);
      await metadataAPI.refreshMetadata(connection.id, 'schema');
      showNotification('Metadata refresh initiated', 'success');

      // Fetch updated status after a short delay
      setTimeout(async () => {
        try {
          const response = await metadataAPI.getMetadataStatus(connection.id);
          setStatus(response.data);
        } catch (error) {
          console.error('Error fetching updated metadata status:', error);
        } finally {
          setLoading(false);
        }
      }, 1000);
    } catch (error) {
      console.error('Error refreshing metadata:', error);
      showNotification('Failed to refresh metadata', 'error');
      setLoading(false);
    }
  };

  // Get status badge
  const getStatusBadge = (status) => {
    const statusColors = {
      fresh: 'bg-accent-100 text-accent-800',
      stale: 'bg-warning-100 text-warning-800',
      unknown: 'bg-secondary-100 text-secondary-800',
      error: 'bg-danger-100 text-danger-800'
    };

    const statusLabels = {
      fresh: 'Fresh',
      stale: 'Stale',
      unknown: 'Unknown',
      error: 'Error'
    };

    return (
      <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${statusColors[status] || statusColors.unknown}`}>
        {status === 'fresh' ? (
          <CheckCircleIcon className="-ml-0.5 mr-1.5 h-3 w-3" aria-hidden="true" />
        ) : status === 'error' ? (
          <ExclamationCircleIcon className="-ml-0.5 mr-1.5 h-3 w-3" aria-hidden="true" />
        ) : null}
        {statusLabels[status] || 'Unknown'}
      </span>
    );
  };

  // Format the date
  const formatDate = (dateString) => {
    if (!dateString) return 'Never';

    const date = new Date(dateString);
    return date.toLocaleString();
  };

  // Check if we have an active connection
  if (!connection) {
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

  return (
    <div className="card overflow-hidden">
      <div className="px-4 py-5 sm:px-6 flex justify-between items-center bg-white border-b border-secondary-200">
        <div>
          <h3 className="text-lg leading-6 font-medium text-secondary-900">Connection Health</h3>
          <p className="mt-1 max-w-2xl text-sm text-secondary-500">
            {connection.name}
            {connection.is_default && (
              <span className="ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-primary-100 text-primary-800">
                Default
              </span>
            )}
          </p>
        </div>
        <button
          onClick={handleRefresh}
          disabled={loading}
          className="inline-flex items-center px-3 py-1.5 border border-transparent text-xs font-medium rounded-md text-primary-700 bg-primary-100 hover:bg-primary-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
        >
          {loading ? (
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
        <dl className="grid grid-cols-1 gap-x-4 gap-y-6 sm:grid-cols-2 lg:grid-cols-4">
          <div className="sm:col-span-1">
            <dt className="text-sm font-medium text-secondary-500">Tables Metadata</dt>
            <dd className="mt-1 text-sm text-secondary-900 flex items-center justify-between">
              <span>{getStatusBadge(status.tables?.freshness?.status || 'unknown')}</span>
              <span className="text-xs text-secondary-500">
                {formatDate(status.tables?.last_updated)}
              </span>
            </dd>
          </div>
          <div className="sm:col-span-1">
            <dt className="text-sm font-medium text-secondary-500">Columns Metadata</dt>
            <dd className="mt-1 text-sm text-secondary-900 flex items-center justify-between">
              <span>{getStatusBadge(status.columns?.freshness?.status || 'unknown')}</span>
              <span className="text-xs text-secondary-500">
                {formatDate(status.columns?.last_updated)}
              </span>
            </dd>
          </div>
          <div className="sm:col-span-1">
            <dt className="text-sm font-medium text-secondary-500">Statistics Metadata</dt>
            <dd className="mt-1 text-sm text-secondary-900 flex items-center justify-between">
              <span>{getStatusBadge(status.statistics?.freshness?.status || 'unknown')}</span>
              <span className="text-xs text-secondary-500">
                {formatDate(status.statistics?.last_updated)}
              </span>
            </dd>
          </div>
          <div className="sm:col-span-1">
            <dt className="text-sm font-medium text-secondary-500">Pending Tasks</dt>
            <dd className="mt-1 text-sm text-secondary-900">
              {status.pending_tasks?.length || 0} tasks pending
            </dd>
          </div>
        </dl>
      </div>
    </div>
  );
};

export default ConnectionHealth;