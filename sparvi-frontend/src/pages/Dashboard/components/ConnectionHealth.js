import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Link } from 'react-router-dom';
import { ArrowPathIcon, CheckCircleIcon, ExclamationCircleIcon } from '@heroicons/react/24/outline';
import { useConnection } from '../../../contexts/EnhancedConnectionContext';
import { useUI } from '../../../contexts/UIContext';
import { apiRequest } from '../../../utils/apiUtils';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

const ConnectionHealth = () => {
  const { activeConnection } = useConnection();
  const [status, setStatus] = useState({
    tables: { status: 'unknown', last_updated: null, freshness: { status: 'unknown' } },
    columns: { status: 'unknown', last_updated: null, freshness: { status: 'unknown' } },
    statistics: { status: 'unknown', last_updated: null, freshness: { status: 'unknown' } },
    pending_tasks: []
  });
  const [loading, setLoading] = useState(true);
  const { showNotification } = useUI();

  // Use connectionId instead of the entire connection object to prevent re-renders
  const connectionId = activeConnection?.id;

  // Add a last fetch time tracker
  const lastFetchRef = useRef(0);
  // Add a fetch limit to prevent constant re-fetching
  const FETCH_INTERVAL_MS = 30000; // 30 seconds minimum between fetches

  // Add isMounted ref to track component mount state
  const isMountedRef = useRef(true);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      isMountedRef.current = false;
    };
  }, []);

  // Create memoized fetch function to avoid recreation on every render
  const fetchStatus = useCallback(async (force = false) => {
    if (!connectionId) return;

    // Check if we need to fetch again - avoid too frequent refreshes
    const now = Date.now();
    if (!force && now - lastFetchRef.current < FETCH_INTERVAL_MS) {
      console.log("Skipping fetch, too soon since last fetch");
      return;
    }

    try {
      setLoading(true);
      console.log("Fetching metadata status for connection", connectionId);
      lastFetchRef.current = now; // Update last fetch time

      // Make request with longer timeout
      const response = await apiRequest(`connections/${connectionId}/metadata/status`, {
        timeout: 60000, // 60 seconds timeout
        skipThrottle: force // Skip throttling for manual refreshes
      });

      // Make sure response is valid and component is still mounted
      if (response && isMountedRef.current) {
        console.log("Setting metadata status:", response);
        setStatus(response);
      }
    } catch (error) {
      if (error.throttled) {
        console.log("Metadata status request throttled");
        return;
      }

      if (isMountedRef.current) {
        console.error('Error fetching metadata status:', error);
        showNotification('Failed to load connection health data', 'error');
      }
    } finally {
      if (isMountedRef.current) {
        setLoading(false);
      }
    }
  }, [connectionId, showNotification]);

  // Only fetch data once on initial mount or when connection changes
  useEffect(() => {
    if (connectionId) {
      fetchStatus();
    }
  }, [connectionId, fetchStatus]);

  // Handle refresh metadata - use connectionId directly
  const handleRefresh = async () => {
    if (!connectionId) return;

    try {
      setLoading(true);

      // Simplified request using new helper
      await apiRequest(`connections/${connectionId}/metadata/refresh`, {
        method: 'POST',
        data: { metadata_type: 'schema' },
        skipThrottle: true // Skip throttling for manual refreshes
      });

      showNotification('Metadata refresh initiated', 'success');

      // Fetch updated status after a short delay
      setTimeout(async () => {
        if (!isMountedRef.current) return;

        try {
          const response = await apiRequest(`connections/${connectionId}/metadata/status`, {
            skipThrottle: true // Skip throttling for post-refresh update
          });

          if (response && isMountedRef.current) {
            console.log("Setting metadata status after refresh:", response);
            setStatus(response);
          }
        } catch (error) {
          if (isMountedRef.current) {
            console.error('Error fetching updated metadata status:', error);
          }
        } finally {
          if (isMountedRef.current) {
            setLoading(false);
          }
        }
      }, 1000);
    } catch (error) {
      if (isMountedRef.current) {
        console.error('Error refreshing metadata:', error);
        showNotification('Failed to refresh metadata', 'error');
        setLoading(false);
      }
    }
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
      recent: 'bg-accent-100 text-accent-800',
      stale: 'bg-warning-100 text-warning-800',
      unknown: 'bg-secondary-100 text-secondary-800',
      error: 'bg-danger-100 text-danger-800'
    };

    const statusLabels = {
      recent: 'Fresh',
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