import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { ArrowPathIcon, CheckCircleIcon, ExclamationCircleIcon } from '@heroicons/react/24/outline';
import {metadataAPI, waitForAuth} from '../../../api/enhancedApiService';
import { useUI } from '../../../contexts/UIContext';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import {cancelRequests} from "../../../utils/requestUtils";

const ConnectionHealth = ({ connection }) => {
  const [status, setStatus] = useState({
    tables: { status: 'unknown', last_updated: null, freshness: { status: 'unknown' } },
    columns: { status: 'unknown', last_updated: null, freshness: { status: 'unknown' } },
    statistics: { status: 'unknown', last_updated: null, freshness: { status: 'unknown' } },
    pending_tasks: []
  });
  const [loading, setLoading] = useState(true);
  const { showNotification } = useUI();

  useEffect(() => {
    let isMounted = true;
    const fetchStatus = async () => {
      if (!connection) return;

      try {
        setLoading(true);

        // Wait for authentication to be ready
        await waitForAuth(3000).catch(() => console.log("Auth wait timed out, proceeding anyway"));

        // Add a small delay to ensure other critical components initialize first
        await new Promise(resolve => setTimeout(resolve, 100));

        // Use a unique requestId to better track this request
        const response = await metadataAPI.getMetadataStatus(connection.id, {
          forceFresh: false,
          requestId: `metadata-status-${connection.id}-${Date.now()}`
        });

        // Make sure we have a valid response with data
        if (response && response.data) {
          setStatus(response.data);
        }
      } catch (error) {
        // Only show errors for non-cancelled requests
        if (!error.cancelled) {
          console.error('Error fetching metadata status:', error);
          showNotification('Failed to load connection health data', 'error');
        } else {
          console.log('Metadata status request was cancelled');
        }
      } finally {
        if (isMounted) setLoading(false);
      }
    };

    fetchStatus();

    // Cleanup function
    return () => {
      // Mark component as unmounted
      isMounted = false;

      // Cancel any pending requests for this connection
      if (connection) {
        cancelRequests(`metadata-status-${connection.id}`);
      }
    };
  }, [connection]);

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
          if (response && response.data) {
            setStatus(response.data);
          }
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
  const getStatusBadge = (statusValue) => {
    // Default to 'unknown' if status is undefined
    const currentStatus = statusValue || 'unknown';

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
      <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${statusColors[currentStatus] || statusColors.unknown}`}>
        {currentStatus === 'fresh' ? (
          <CheckCircleIcon className="-ml-0.5 mr-1.5 h-3 w-3" aria-hidden="true" />
        ) : currentStatus === 'error' ? (
          <ExclamationCircleIcon className="-ml-0.5 mr-1.5 h-3 w-3" aria-hidden="true" />
        ) : null}
        {statusLabels[currentStatus] || 'Unknown'}
      </span>
    );
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

  // Format metadata freshness for display
  const getMetadataFreshness = () => {
    // Add safe access to nested properties
    const tablesStatus = status?.tables?.freshness?.status || 'unknown';

    const statusColors = {
      fresh: 'text-accent-500',
      stale: 'text-warning-500',
      unknown: 'text-secondary-400',
      error: 'text-danger-500'
    };

    return (
      <span className={statusColors[tablesStatus] || statusColors.unknown}>
        {tablesStatus === 'fresh' ? 'Up to date' :
         tablesStatus === 'stale' ? 'Needs refresh' :
         tablesStatus === 'error' ? 'Error' : 'Unknown'}
      </span>
    );
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
        {loading ? (
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
              <dd className="mt-1 text-sm text-secondary-900">
                {status?.pending_tasks?.length || 0} tasks pending
              </dd>
            </div>
          </dl>
        )}
      </div>
    </div>
  );
};

export default ConnectionHealth;