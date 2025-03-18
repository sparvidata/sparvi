import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  ServerIcon,
  TableCellsIcon,
  ClipboardDocumentCheckIcon,
  ExclamationTriangleIcon,
  ArrowPathIcon,
  PlusCircleIcon
} from '@heroicons/react/24/outline';
import { useConnection } from '../../contexts/ConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { schemaAPI, validationsAPI, metadataAPI } from '../../api/apiService';
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

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Dashboard', href: '/dashboard' }
    ]);
  }, [updateBreadcrumbs]);

  // Load dashboard data when active connection changes
  useEffect(() => {
    const fetchDashboardData = async () => {
      // If no connections or no active connection, return
      if (!connections.length || (!activeConnection && !defaultConnection)) {
        return;
      }

      const connection = activeConnection || defaultConnection;

      try {
        setLoading('dashboard', true);

        // Fetch tables count
        const tablesResponse = await schemaAPI.getTables(connection.id);
        const tableCount = tablesResponse.data.tables ? tablesResponse.data.tables.length : 0;

        // Fetch metadata status
        const metadataStatusResponse = await metadataAPI.getMetadataStatus(connection.id);

        // Fetch recent schema changes
        const changesResponse = await schemaAPI.getChanges(connection.id,
          new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString());

        // Update dashboard data
        setDashboardData({
          tableCount,
          recentValidations: [], // We'll fetch this later
          recentChanges: changesResponse.data.changes || [],
          schemaHealth: metadataStatusResponse.data || { status: 'unknown' },
          loading: false
        });
      } catch (error) {
        console.error('Error fetching dashboard data:', error);
        showNotification('Failed to load dashboard data', 'error');
      } finally {
        setLoading('dashboard', false);
      }
    };

    fetchDashboardData();
  }, [activeConnection, defaultConnection, connections.length, setLoading, showNotification]);

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
        <h1 className="text-2xl font-semibold text-secondary-900">Dashboard</h1>
        <p className="mt-1 text-sm text-secondary-500">
          Overview of your data quality metrics and recent activity
        </p>
      </div>

      {/* Connection info */}
      <ConnectionHealth connection={activeConnection || defaultConnection} />

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
            connectionId={activeConnection?.id || defaultConnection?.id}
            type="tables"
          />
          <OverviewCard
            title="Validations"
            connectionId={activeConnection?.id || defaultConnection?.id}
            type="validations"
          />
        </div>
      </div>
    </div>
  );
};

export default DashboardPage;