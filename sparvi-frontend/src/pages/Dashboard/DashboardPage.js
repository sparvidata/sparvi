import React, { useState, useEffect } from 'react';
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
import { schemaAPI, validationsAPI, metadataAPI } from '../../api/enhancedApiService';
import BatchRequest from '../../components/common/BatchRequest';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import OverviewCard from './components/OverviewCard';
import RecentActivity from './components/RecentActivity';
import ConnectionHealth from './components/ConnectionHealth';
import StatisticCard from './components/StatisticsCard';

const DashboardPage = () => {
  const { connections, activeConnection, defaultConnection } = useConnection();
  const { updateBreadcrumbs, showNotification, setLoading } = useUI();

  const [dashboardData, setDashboardData] = useState({
    tableCount: 0,
    recentValidations: [],
    recentChanges: [],
    schemaHealth: { status: 'unknown' },
    loading: true
  });
  const [isRefreshing, setIsRefreshing] = useState(false);

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Dashboard', href: '/dashboard' }
    ]);
  }, [updateBreadcrumbs]);

  // Prepare connection ID for API calls
  const connectionId = activeConnection?.id || defaultConnection?.id;

  // Prepare batch request configuration
  const getBatchRequests = () => {
    if (!connectionId) return [];

    return [
      { id: 'tables', path: `/connections/${connectionId}/tables` },
      { id: 'metadataStatus', path: `/connections/${connectionId}/metadata/status` },
      { id: 'changes', path: `/connections/${connectionId}/changes`,
        params: { since: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString() } }
    ];
  };

  // Handle batch requests completion
  const handleBatchComplete = (results) => {
    // Ensure each property exists before trying to access it
    const tableCount = results.tables?.tables?.length || 0;
    const recentChanges = results.changes?.changes || [];
    const schemaHealth = results.metadataStatus || { status: 'unknown' };

    setDashboardData({
      tableCount,
      recentValidations: [],
      recentChanges,
      schemaHealth,
      loading: false
    });
    setIsRefreshing(false);
  };

  // Handle batch requests error
  const handleBatchError = (error) => {
    console.error('Error loading dashboard data:', error);
    showNotification('Failed to load dashboard data', 'error');
    setDashboardData(prev => ({ ...prev, loading: false }));
    setIsRefreshing(false);
  };

  // Handle dashboard refresh
  const handleRefresh = () => {
    setIsRefreshing(true);
    setDashboardData(prev => ({ ...prev, loading: true }));
  };

  // If no connections, show empty state
  if (!connections.length) {
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

          {connectionId && (
            <button
              onClick={handleRefresh}
              disabled={isRefreshing}
              className="inline-flex items-center px-3 py-1.5 border border-secondary-300 shadow-sm text-sm font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-secondary-500 disabled:opacity-50"
            >
              {isRefreshing ? (
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

      {/* Connection info */}
      <ConnectionHealth connection={activeConnection || defaultConnection} />

      {connectionId ? (
        <BatchRequest
          requests={getBatchRequests()}
          onComplete={handleBatchComplete}
          onError={handleBatchError}
          loadingComponent={
            <div className="mt-8 grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4">
              {/* Skeleton loading states for statistics */}
              {[1, 2, 3, 4].map((i) => (
                <div key={i} className="card px-4 py-5 sm:p-6 animate-pulse">
                  <div className="flex items-center">
                    <div className="flex-shrink-0 rounded-md p-3 bg-secondary-100">
                      <div className="h-6 w-6 bg-secondary-200 rounded"></div>
                    </div>
                    <div className="ml-5 w-0 flex-1">
                      <div className="h-5 bg-secondary-200 rounded w-20 mb-2"></div>
                      <div className="h-7 bg-secondary-200 rounded w-12"></div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          }
        >
          {() => (
            <>
              {/* Stats overview */}
              <div className="mt-8 grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4">
                <StatisticCard
                  title="Tables"
                  value={dashboardData.tableCount}
                  icon={TableCellsIcon}
                  href="/explorer"
                  color="primary"
                />
                <StatisticCard
                  title="Validations"
                  value={dashboardData.recentValidations.length || 0}
                  icon={ClipboardDocumentCheckIcon}
                  href="/validations"
                  color="accent"
                />
                <StatisticCard
                  title="Schema Changes"
                  value={dashboardData.recentChanges.length || 0}
                  icon={ArrowPathIcon}
                  href="/metadata"
                  color="warning"
                />
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
                  recentChanges={dashboardData.recentChanges}
                  recentValidations={dashboardData.recentValidations}
                />

                {/* Overview cards */}
                <div className="space-y-5">
                  <OverviewCard
                    title="Tables"
                    connectionId={connectionId}
                    type="tables"
                  />
                  <OverviewCard
                    title="Validations"
                    connectionId={connectionId}
                    type="validations"
                  />
                </div>
              </div>
            </>
          )}
        </BatchRequest>
      ) : (
        <div className="mt-8 text-center py-8">
          <p className="text-secondary-500">Select a connection to view dashboard data.</p>
        </div>
      )}
    </div>
  );
};

export default DashboardPage;