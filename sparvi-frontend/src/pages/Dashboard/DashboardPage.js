import React, { useEffect } from 'react';
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
import LoadingSpinner from '../../components/common/LoadingSpinner';
import ConnectionHealth from './components/ConnectionHealth';
import OverviewCard from './components/OverviewCard';
import RecentActivity from './components/RecentActivity';
import StatisticCard from './components/StatisticsCard';
import { useTablesData } from '../../hooks/useTablesData';
import { useMetadataStatus } from '../../hooks/useMetadataStatus';
import { useValidationsSummary } from '../../hooks/useValidationsData';
import { queryClient } from '../../api/queryClient';

const DashboardPage = () => {
  const { connections, activeConnection, loading: connectionsLoading } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();

  // Get connectionId safely
  const connectionId = activeConnection?.id;

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Dashboard', href: '/dashboard' }
    ]);
  }, [updateBreadcrumbs]);

  // Use React Query with existing hooks
  const tablesQuery = useTablesData(connectionId, {
    enabled: !!connectionId
  });

  // Use React Query for metadata status
  const metadataQuery = useMetadataStatus(connectionId, {
    enabled: !!connectionId,
    // Only poll if there are pending tasks
    refetchInterval: (data) => {
      return (data?.pending_tasks?.length > 0) ? 5000 : false;
    },
  });

  // Use React Query for validations summary
  const validationsQuery = useValidationsSummary(connectionId, {
    enabled: !!connectionId
  });

  // Extract data safely with fallbacks
  const tablesCount = Array.isArray(tablesQuery.data) ? tablesQuery.data.length : 0;
  const changesCount = metadataQuery.data?.changes_detected || 0;
  const validationsCount = validationsQuery.data?.total_count || 0;

  // Use failing_count for the total failed validations count
  const failedValidations = validationsQuery.data?.failing_count || 0;

  // Handle refresh all data
  const handleRefreshData = async () => {
    if (!connectionId) return;

    showNotification('Refreshing dashboard data...', 'info');

    // Invalidate all queries related to this connection
    queryClient.invalidateQueries(['schema-tables', connectionId]);
    queryClient.invalidateQueries(['metadata-status', connectionId]);
    queryClient.invalidateQueries(['validations-summary', connectionId]);

    showNotification('Dashboard data refreshed', 'success');
  };

  // Show loading indicator while connections are being fetched
  if (connectionsLoading) {
    return (
      <div className="flex justify-center items-center py-12">
        <div className="text-center">
          <LoadingSpinner size="lg" className="mx-auto" />
          <p className="mt-4 text-sm text-secondary-500">Loading connections...</p>
        </div>
      </div>
    );
  }

  // Only show the "No connections" message after loading is complete
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
              disabled={tablesQuery.isFetching || metadataQuery.isFetching || validationsQuery.isFetching}
              className="inline-flex items-center px-3 py-1.5 border border-secondary-300 shadow-sm text-sm font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-secondary-500 disabled:opacity-50"
            >
              {tablesQuery.isFetching || metadataQuery.isFetching || validationsQuery.isFetching ? (
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
        {/* Table count */}
        <StatisticCard
          title="Tables"
          value={tablesCount}
          icon={TableCellsIcon}
          href="/explorer"
          color="primary"
          loading={tablesQuery.isLoading}
          error={tablesQuery.isError}
          isRefetching={tablesQuery.isRefetching && !tablesQuery.isLoading}
        />

        {/* Validations count */}
        <StatisticCard
          title="Validations"
          value={validationsCount}
          icon={ClipboardDocumentCheckIcon}
          href="/validations"
          color="accent"
          loading={validationsQuery.isLoading}
          error={validationsQuery.isError}
          isRefetching={validationsQuery.isRefetching && !validationsQuery.isLoading}
        />

        {/* Schema changes */}
        <StatisticCard
          title="Schema Changes"
          value={changesCount}
          icon={ArrowPathIcon}
          href="/metadata"
          color="warning"
          loading={metadataQuery.isLoading}
          error={metadataQuery.isError}
          isRefetching={metadataQuery.isRefetching && !metadataQuery.isLoading}
        />

        {/* Issues (Failed Validations) */}
        <StatisticCard
          title="Issues"
          value={failedValidations}
          icon={ExclamationTriangleIcon}
          href="/validations"
          color="danger"
          loading={validationsQuery.isLoading}
          error={validationsQuery.isError}
          isRefetching={validationsQuery.isRefetching && !validationsQuery.isLoading}
          healthScore={validationsQuery.data?.overall_health_score}
          freshness={validationsQuery.data?.freshness_status}
        />
      </div>

      {/* Contextual cards */}
      <div className="mt-5 grid grid-cols-1 gap-5 lg:grid-cols-2">
        {/* Recent activity */}
        <RecentActivity
          recentChanges={metadataQuery.data?.changes || []}
          recentValidations={[]}
          isLoading={metadataQuery.isLoading && !metadataQuery.data}
          error={metadataQuery.isError}
          onRetry={() => metadataQuery.refetch()}
        />

        {/* Overview cards */}
        <div className="space-y-5">
          {connectionId && (
            <>
              <OverviewCard
                key={`tables-${connectionId}`}
                title="Tables"
                type="tables"
                connectionId={connectionId}
              />
              <OverviewCard
                key={`validations-${connectionId}`}
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