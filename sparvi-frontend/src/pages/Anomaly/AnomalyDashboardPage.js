// src/pages/Anomaly/AnomalyDashboardPage.js

import React, { useState, useEffect } from 'react';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useParams, useNavigate, Link } from 'react-router-dom';
import {
  ExclamationTriangleIcon,
  CheckCircleIcon,
  ClockIcon,
  ArrowPathIcon,
  AdjustmentsHorizontalIcon,
  ServerIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import BatchRequest from '../../components/common/BatchRequest';
import AnomalyStatusCard from './components/AnomalyStatusCard';
import AnomalySeverityChart from './components/AnomalySeverityChart';
import AnomalyTrendChart from './components/AnomalyTrendChart';
import RecentAnomaliesList from './components/RecentAnomaliesList';
import AnomalyMetricCard from './components/AnomalyMetricCard';
import EmptyState from '../../components/common/EmptyState';

const AnomalyDashboardPage = () => {
  const { activeConnection, loading: connectionLoading } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();
  const { connectionId } = useParams();
  const navigate = useNavigate();
  const [timeRange, setTimeRange] = useState(30); // Default 30 days
  const [refreshing, setRefreshing] = useState(false);
  const [refreshTrigger, setRefreshTrigger] = useState(0);

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Dashboard', href: '/dashboard' },
      { name: 'Anomaly Detection', href: '/anomalies' },
      { name: activeConnection?.name || 'Connection', href: `/anomalies/${connectionId}` }
    ]);
  }, [updateBreadcrumbs, connectionId, activeConnection]);

  // Handle redirect if connection ID is missing but we have activeConnection
  useEffect(() => {
    if (!connectionLoading && !connectionId && activeConnection) {
      console.log("No connectionId in URL, redirecting to active connection:", activeConnection.id);
      navigate(`/anomalies/${activeConnection.id}`, { replace: true });
    }
  }, [connectionId, connectionLoading, activeConnection, navigate]);

  // Function to refresh data
  const handleRefresh = async () => {
    if (refreshing || !connectionId || connectionId === 'undefined') return;

    setRefreshing(true);
    try {
      // Trigger a manual detection run
      const response = await fetch(`/api/connections/${connectionId}/anomalies/detect`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ force: true })
      });

      if (!response.ok) {
        throw new Error('Failed to run anomaly detection');
      }

      const result = await response.json();

      // Show notification
      showNotification({
        type: 'success',
        message: `Anomaly detection completed: ${result.anomalies_detected || 0} anomalies found`
      });

      // Trigger a refresh of the dashboard data
      setRefreshTrigger(prev => prev + 1);

    } catch (error) {
      console.error('Error refreshing anomaly data:', error);
      showNotification({
        type: 'error',
        message: 'Failed to refresh anomaly data'
      });
    } finally {
      setRefreshing(false);
    }
  };

  // Check if we can fetch data safely
  const shouldFetchData = !connectionLoading && connectionId && connectionId !== 'undefined';

  // Define the requests for batch loading - only when we have a valid connectionId
  const requests = shouldFetchData ? [
    { id: 'summary', path: `/connections/${connectionId}/anomalies/summary`, params: { days: timeRange } },
    { id: 'dashboard', path: `/connections/${connectionId}/anomalies/dashboard`, params: { days: timeRange } },
    { id: 'configs', path: `/connections/${connectionId}/anomalies/configs` }
  ] : [];

  // If still loading connection but no active connection yet
  if (connectionLoading) {
    return (
      <div className="flex justify-center py-12">
        <LoadingSpinner size="lg" />
        <span className="ml-3 text-secondary-600">Loading connection information...</span>
      </div>
    );
  }

  // If no active connection is available
  if (!activeConnection) {
    return (
      <EmptyState
        icon={ServerIcon}
        title="No connection selected"
        description="Please select a database connection to view anomaly detection"
        actionText="Manage Connections"
        actionLink="/connections"
      />
    );
  }

  return (
    <div className="flex flex-col space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-bold text-gray-900">Anomaly Detection Dashboard</h1>

        <div className="flex items-center space-x-4">
          {/* Time range selector */}
          <div className="flex items-center space-x-2">
            <span className="text-sm text-gray-500">Time Range:</span>
            <select
              className="rounded-md border-gray-300 py-1 pl-3 pr-10 text-sm focus:outline-none focus:ring-primary-500 focus:border-primary-500"
              value={timeRange}
              onChange={(e) => setTimeRange(parseInt(e.target.value))}
            >
              <option value={7}>Last 7 days</option>
              <option value={30}>Last 30 days</option>
              <option value={90}>Last 90 days</option>
            </select>
          </div>

          {/* Refresh button */}
          <button
            onClick={handleRefresh}
            disabled={refreshing || !shouldFetchData}
            className="inline-flex items-center px-3 py-2 border border-transparent shadow-sm text-sm leading-4 font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
          >
            {refreshing ? (
              <>
                <LoadingSpinner size="xs" className="mr-2" />
                Refreshing...
              </>
            ) : (
              <>
                <ArrowPathIcon className="h-4 w-4 mr-2" />
                Refresh Data
              </>
            )}
          </button>

          {/* Configure button */}
          <Link
            to={shouldFetchData ? `/anomalies/${connectionId}/configs` : "/anomalies"}
            className="inline-flex items-center px-3 py-2 border border-gray-300 shadow-sm text-sm leading-4 font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          >
            <AdjustmentsHorizontalIcon className="h-4 w-4 mr-2" />
            Configure Detection
          </Link>
        </div>
      </div>

      {shouldFetchData ? (
        <BatchRequest
          requests={requests}
          key={`anomaly-dashboard-${refreshTrigger}-${timeRange}-${connectionId}`}
        >
          {(data) => {
            // Extract data from the batch responses
            const summary = data.summary || {};
            const dashboard = data.dashboard || {};
            const configs = data.configs?.configs || [];

            const totalAnomalies = summary.total_anomalies || 0;
            const openAnomalies = summary.open || 0;
            const highSeverity = summary.high_severity || 0;
            const activeConfigs = configs.filter(c => c.is_active).length;

            return (
              <div className="space-y-6">
                {/* Stats cards */}
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                  <AnomalyStatusCard
                    title="Total Anomalies"
                    value={totalAnomalies}
                    subtitle={`In the last ${timeRange} days`}
                    icon={ExclamationTriangleIcon}
                    iconColor="text-yellow-400"
                    bgColor="bg-yellow-50"
                  />

                  <AnomalyStatusCard
                    title="Open Issues"
                    value={openAnomalies}
                    subtitle="Requiring attention"
                    icon={ClockIcon}
                    iconColor="text-red-500"
                    bgColor="bg-red-50"
                  />

                  <AnomalyStatusCard
                    title="High Severity"
                    value={highSeverity}
                    subtitle="Critical issues"
                    icon={ExclamationTriangleIcon}
                    iconColor="text-red-600"
                    bgColor="bg-red-50"
                  />

                  <AnomalyStatusCard
                    title="Active Monitors"
                    value={activeConfigs}
                    subtitle="Detection configurations"
                    icon={CheckCircleIcon}
                    iconColor="text-green-500"
                    bgColor="bg-green-50"
                  />
                </div>

                {/* Charts section */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  {/* Anomaly trend chart */}
                  <div className="bg-white shadow rounded-lg p-6">
                    <h3 className="text-lg font-medium text-gray-900 mb-4">Anomaly Trends</h3>
                    <AnomalyTrendChart
                      data={dashboard.trends || []}
                      timeRange={timeRange}
                    />
                  </div>

                  {/* Anomaly by severity chart */}
                  <div className="bg-white shadow rounded-lg p-6">
                    <h3 className="text-lg font-medium text-gray-900 mb-4">Anomalies by Severity</h3>
                    <AnomalySeverityChart
                      highCount={summary.high_severity || 0}
                      mediumCount={summary.medium_severity || 0}
                      lowCount={summary.low_severity || 0}
                    />
                  </div>
                </div>

                {/* Recent anomalies and metrics */}
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                  {/* Recent anomalies list */}
                  <div className="lg:col-span-2 bg-white shadow rounded-lg p-6">
                    <div className="flex justify-between items-center mb-4">
                      <h3 className="text-lg font-medium text-gray-900">Recent Anomalies</h3>
                      <Link
                        to={`/anomalies/${connectionId}/explorer`}
                        className="text-sm text-primary-600 hover:text-primary-700"
                      >
                        View All
                      </Link>
                    </div>
                    <RecentAnomaliesList
                      anomalies={dashboard.recent_anomalies || []}
                      connectionId={connectionId}
                    />
                  </div>

                  {/* Metrics section */}
                  <div className="bg-white shadow rounded-lg p-6">
                    <h3 className="text-lg font-medium text-gray-900 mb-4">Top Affected Metrics</h3>
                    <div className="space-y-4">
                      {summary.by_table && summary.by_table.length > 0 ? (
                        summary.by_table.slice(0, 5).map((table, index) => (
                          <AnomalyMetricCard
                            key={index}
                            name={table.table_name}
                            count={table.count}
                            type="table"
                            onClick={() => navigate(`/anomalies/${connectionId}/explorer?table=${table.table_name}`)}
                          />
                        ))
                      ) : (
                        <div className="text-center py-6 text-gray-500">
                          No anomalies detected yet
                        </div>
                      )}
                    </div>
                  </div>
                </div>

                {/* Setup guidance */}
                {activeConfigs === 0 && (
                  <div className="bg-yellow-50 border-l-4 border-yellow-400 p-4">
                    <div className="flex">
                      <div className="flex-shrink-0">
                        <ExclamationTriangleIcon className="h-5 w-5 text-yellow-400" aria-hidden="true" />
                      </div>
                      <div className="ml-3">
                        <p className="text-sm text-yellow-700">
                          No active anomaly detection configurations found.
                          <Link
                            to={`/anomalies/${connectionId}/configs/new`}
                            className="font-medium text-yellow-700 underline ml-1"
                          >
                            Set up anomaly detection
                          </Link>
                        </p>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            );
          }}
        </BatchRequest>
      ) : (
        <div className="text-center py-12">
          <div className="mx-auto h-12 w-12 text-gray-400">
            <ExclamationTriangleIcon className="h-12 w-12" />
          </div>
          <h3 className="mt-2 text-sm font-medium text-gray-900">No Connection Selected</h3>
          <p className="mt-1 text-sm text-gray-500">
            Please select a valid database connection to view anomaly detection data.
          </p>
          <div className="mt-6">
            <Link
              to="/connections"
              className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700"
            >
              Manage Connections
            </Link>
          </div>
        </div>
      )}
    </div>
  );
};

export default AnomalyDashboardPage;