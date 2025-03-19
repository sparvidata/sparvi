import React, { useEffect, useState, useCallback, useRef } from 'react';
import { Link } from 'react-router-dom';
import {
  ServerIcon,
  TableCellsIcon,
  ClipboardDocumentCheckIcon,
  ExclamationTriangleIcon,
  ArrowPathIcon,
  PlusCircleIcon
} from '@heroicons/react/24/outline';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { apiRequest } from '../../utils/apiUtils';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import ConnectionHealth from './components/ConnectionHealth';
import OverviewCard from './components/OverviewCard';
import RecentActivity from './components/RecentActivity';
import StatisticCard from './components/StatisticsCard';

const DashboardPage = () => {
  const { connections, activeConnection } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();
  const [refreshing, setRefreshing] = useState(false);

  // State for individual sections
  const [tablesData, setTablesData] = useState(null);
  const [changesData, setChangesData] = useState(null);
  const [tablesLoading, setTablesLoading] = useState(true);
  const [changesLoading, setChangesLoading] = useState(true);

  // Use a ref to track mounted state
  const isMountedRef = useRef(true);
  // Track last fetch time to prevent too frequent refreshes
  const lastFetchRef = useRef({ tables: 0, changes: 0 });
  const FETCH_INTERVAL_MS = 30000; // 30 seconds minimum between fetches

  // Get connectionId to use as a dependency
  const connectionId = activeConnection?.id;

  // Set up cleanup on unmount
  useEffect(() => {
    return () => {
      isMountedRef.current = false;
    };
  }, []);

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Dashboard', href: '/dashboard' }
    ]);
  }, [updateBreadcrumbs]);

  // Load tables data function
  const loadTablesData = useCallback(async (force = false) => {
    if (!connectionId) {
      setTablesLoading(false);
      return;
    }

    // Check if we should skip fetching due to recent fetch
    const now = Date.now();
    if (!force && now - lastFetchRef.current.tables < FETCH_INTERVAL_MS) {
      return;
    }

    try {
      console.log("Loading tables data for connection", connectionId);
      setTablesLoading(true);
      lastFetchRef.current.tables = now;

      const response = await apiRequest(`connections/${connectionId}/tables`, {
        skipThrottle: force // Skip throttling if forcing refresh
      });

      if (isMountedRef.current) {
        setTablesData(response);
        setTablesLoading(false);
      }
    } catch (error) {
      if (isMountedRef.current) {
        console.error('Error loading tables data:', error);
        setTablesLoading(false);
      }
    }
  }, [connectionId]);

  // Load changes data function
  const loadChangesData = useCallback(async (force = false) => {
    if (!connectionId) {
      setChangesLoading(false);
      return;
    }

    // Check if we should skip fetching due to recent fetch
    const now = Date.now();
    if (!force && now - lastFetchRef.current.changes < FETCH_INTERVAL_MS) {
      return;
    }

    try {
      setChangesLoading(true);
      lastFetchRef.current.changes = now;

      const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
      const response = await apiRequest(`connections/${connectionId}/changes`, {
        params: { since },
        skipThrottle: force // Skip throttling if forcing refresh
      });

      if (isMountedRef.current) {
        setChangesData(response);
        setChangesLoading(false);
      }
    } catch (error) {
      if (isMountedRef.current) {
        console.error('Error loading changes data:', error);
        setChangesLoading(false);
      }
    }
  }, [connectionId]);

  // Load dashboard data when connection changes
  useEffect(() => {
    if (connectionId) {
      // Load tables and changes data
      loadTablesData();
      loadChangesData();
    } else {
      // Reset state when connection changes
      setTablesData(null);
      setChangesData(null);
      setTablesLoading(false);
      setChangesLoading(false);
    }
  }, [connectionId, loadTablesData, loadChangesData]);

  // Handle manual refresh
  const handleRefreshData = async () => {
    if (!connectionId || refreshing) return;

    setRefreshing(true);
    try {
      await Promise.all([
        loadTablesData(true), // Pass true to force refresh
        loadChangesData(true) // Pass true to force refresh
      ]);
      showNotification('Dashboard data refreshed', 'success');
    } catch (error) {
      console.error('Error refreshing dashboard data:', error);
      showNotification('Failed to refresh dashboard data', 'error');
    } finally {
      setRefreshing(false);
    }
  };

  // If no connections, show empty state
  if (!connections || connections.length === 0) {
    return (
      <div className="text-center py-12">
        <ServerIcon className="mx-auto h-12 w-12 text-secondary-400" />
        <h3 className="mt-2 text-sm font-medium text-secondary-900">No connections</h3>
        <p className="mt-1 text-sm text-secondary-500">
          Get started by creating a new connection to your data source.
        </p>
        <div className="mt-6">
          <Link
            to="/connections/new"
            className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          >
            <PlusCircleIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
            New Connection
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="py-4">
        <div className="flex justify-between items-center">
          <h1 className="text-2xl font-semibold text-secondary-900">Dashboard</h1>

          {activeConnection && (
            <button
              onClick={handleRefreshData}
              disabled={refreshing}
              className="inline-flex items-center px-3 py-1.5 border border-secondary-300 shadow-sm text-sm font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-secondary-500 disabled:opacity-50"
            >
              {refreshing ? (
                <>
                  <LoadingSpinner size="sm" className="mr-2" />
                  Refreshing...
                </>
              ) : (
                <>
                  <ArrowPathIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
                  Refresh Data
                </>
              )}
            </button>
          )}
        </div>
        <p className="mt-1 text-sm text-secondary-500">
          Overview of your data quality metrics and recent activity
        </p>
      </div>

      {/* Connection health with its own loading logic */}
      <ConnectionHealth />

      {/* Statistics section */}
      <div className="mt-8 grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4">
        {/* Table count - Explicitly handle the loading and value state */}
        <StatisticCard
          title="Tables"
          value={tablesData && tablesData.tables ? tablesData.tables.length : 0}
          icon={TableCellsIcon}
          href="/explorer"
          color="primary"
          loading={tablesLoading && !tablesData}
        />

        {/* Validations count */}
        <StatisticCard
          title="Validations"
          value={0} // No validations data yet
          icon={ClipboardDocumentCheckIcon}
          href="/validations"
          color="accent"
        />

        {/* Schema changes */}
        <StatisticCard
          title="Schema Changes"
          value={changesData && changesData.changes ? changesData.changes.length : 0}
          icon={ArrowPathIcon}
          href="/metadata"
          color="warning"
          loading={changesLoading && !changesData}
        />

        {/* Issues */}
        <StatisticCard
          title="Issues"
          value={0}
          icon={ExclamationTriangleIcon}
          href="/validations"
          color="danger"
        />
      </div>

      {/* Contextual cards */}
      <div className="mt-5 grid grid-cols-1 gap-5 lg:grid-cols-2">
        {/* Recent activity */}
        <RecentActivity
          recentChanges={changesData?.changes || []}
          recentValidations={[]}
        />

        {/* Overview cards with connectionId prop - now with improved handling */}
        <div className="space-y-5">
          {connectionId && (
            <>
              <OverviewCard
                title="Tables"
                type="tables"
                connectionId={connectionId}
              />
              <OverviewCard
                title="Validations"
                type="validations"
                connectionId={connectionId}
              />
            </>
          )}
        </div>
      </div>
    </div>
  );
};

export default DashboardPage;