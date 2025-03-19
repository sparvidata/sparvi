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
  const [tablesLoading, setTablesLoading] = useState(false);
  const [changesLoading, setChangesLoading] = useState(false);

  // Use a ref to track mounted state
  const isMountedRef = useRef(true);
  // Use a ref to track if tables data has been loaded
  const tablesLoadedRef = useRef(false);
  // Track last fetch time to prevent too frequent refreshes
  const lastFetchRef = useRef(0);
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

  // Create memoized load function
  const loadDashboardData = useCallback(async (force = false) => {
    if (!connectionId) return;

    // Check if we need to fetch again - avoid too frequent refreshes
    const now = Date.now();
    if (!force && now - lastFetchRef.current < FETCH_INTERVAL_MS) {
      console.log("Skipping dashboard fetch, too soon since last fetch");
      return;
    }

    try {
      console.log("Loading tables data for connection", connectionId);
      lastFetchRef.current = now;

      // Load tables data
      setTablesLoading(true);
      const tablesResponse = await apiRequest(`connections/${connectionId}/tables`, {
        skipThrottle: force // Skip throttling for manual refreshes
      });

      if (isMountedRef.current) {
        console.log("Setting tables data:", tablesResponse);
        setTablesData(tablesResponse);
        setTablesLoading(false);
        tablesLoadedRef.current = true; // Mark that we've loaded tables data
      }

      // Load changes data
      setChangesLoading(true);
      const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
      const changesResponse = await apiRequest(`connections/${connectionId}/changes`, {
        params: { since },
        skipThrottle: force // Skip throttling for manual refreshes
      });

      if (isMountedRef.current) {
        console.log("Setting changes data:", changesResponse);
        setChangesData(changesResponse);
        setChangesLoading(false);
      }
    } catch (error) {
      if (error.throttled) {
        console.log("Dashboard data request throttled");
        return;
      }

      if (isMountedRef.current) {
        console.error('Error loading dashboard data:', error);
        showNotification('Some dashboard data could not be loaded', 'error');
        // Even on error, set loading to false
        setTablesLoading(false);
        setChangesLoading(false);
      }
    }
  }, [connectionId, showNotification]);

  // Load data only when connectionId changes and only once
  useEffect(() => {
    if (connectionId && !tablesLoadedRef.current) {
      loadDashboardData();
    } else if (!connectionId) {
      // Reset state when connection changes
      setTablesData(null);
      setChangesData(null);
      tablesLoadedRef.current = false;
    }
  }, [connectionId, loadDashboardData]);

  // Handle manual refresh
  const handleRefreshData = async () => {
    if (!connectionId || refreshing) return;

    setRefreshing(true);
    try {
      await loadDashboardData(true); // true = force refresh
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
          Get started by creating a new connection.
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

        {/* Overview cards with connectionId prop */}
        <div className="space-y-5">
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
        </div>
      </div>
    </div>
  );
};

export default DashboardPage;