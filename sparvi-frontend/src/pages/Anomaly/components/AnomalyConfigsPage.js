// src/pages/Anomaly/components/AnomalyConfigsPage.js

import React, { useState, useEffect } from 'react';
import { useConnection } from '../../../contexts/EnhancedConnectionContext';
import { useUI } from '../../../contexts/UIContext';
import { useParams, useNavigate, Link } from 'react-router-dom';
import {
  PlusIcon,
  TrashIcon,
  PencilIcon,
  CheckCircleIcon,
  XCircleIcon,
  AdjustmentsHorizontalIcon,
  ServerIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import BatchRequest from '../../../components/common/BatchRequest';
import SearchInput from '../../../components/common/SearchInput';
import EmptyState from '../../../components/common/EmptyState';

const AnomalyConfigsPage = () => {
  const { activeConnection, loading: connectionLoading } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();
  const { connectionId } = useParams();
  const navigate = useNavigate();

  const [searchTerm, setSearchTerm] = useState('');
  const [filterTable, setFilterTable] = useState('');
  const [refreshTrigger, setRefreshTrigger] = useState(0);

  // Handle redirect if connection ID is missing but we have activeConnection
  useEffect(() => {
    if (!connectionLoading && !connectionId && activeConnection) {
      console.log("No connectionId in URL, redirecting to active connection:", activeConnection.id);
      navigate(`/anomalies/${activeConnection.id}/configs`, { replace: true });
    }
  }, [connectionId, connectionLoading, activeConnection, navigate]);

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Dashboard', href: '/dashboard' },
      { name: 'Anomaly Detection', href: '/anomalies' },
      { name: activeConnection?.name || 'Connection', href: `/anomalies/${connectionId}` },
      { name: 'Configurations' }
    ]);
  }, [updateBreadcrumbs, connectionId, activeConnection]);

  // Check if we can fetch data safely
  const shouldFetchData = !connectionLoading && connectionId && connectionId !== 'undefined';

  // Define the requests for batch loading - only when we have a valid connectionId
  const requests = shouldFetchData ? [
    {
      id: 'configs',
      path: `/connections/${connectionId}/anomalies/configs`
    },
    {
      id: 'tables',
      path: `/connections/${connectionId}/tables`
    }
  ] : [];

  // Handle delete confirmation and action
  const handleDeleteConfig = async (configId, e) => {
    e.stopPropagation();

    if (!connectionId || connectionId === 'undefined') {
      console.error("Cannot delete config: connection ID is undefined");
      return;
    }

    // Use window.confirm with proper reference to avoid ESLint issues
    if (window.confirm('Are you sure you want to delete this configuration?')) {
      deleteConfiguration(configId);
    }
  };

  // Function to handle the actual deletion
  const deleteConfiguration = async (configId) => {
    if (!connectionId || connectionId === 'undefined') return;

    try {
      const response = await fetch(`/api/connections/${connectionId}/anomalies/configs/${configId}`, {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error('Failed to delete configuration');
      }

      showNotification({
        type: 'success',
        message: 'Configuration deleted successfully'
      });

      // Trigger refresh
      setRefreshTrigger(prev => prev + 1);
    } catch (error) {
      console.error('Error deleting configuration:', error);
      showNotification({
        type: 'error',
        message: 'Failed to delete configuration'
      });
    }
  };

  // Handle config toggle (active/inactive)
  const handleToggleConfig = async (configId, isActive, e) => {
    e.stopPropagation();

    if (!connectionId || connectionId === 'undefined') {
      console.error("Cannot toggle config: connection ID is undefined");
      return;
    }

    try {
      const response = await fetch(`/api/connections/${connectionId}/anomalies/configs/${configId}`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ is_active: !isActive })
      });

      if (!response.ok) {
        throw new Error('Failed to update configuration');
      }

      showNotification({
        type: 'success',
        message: `Configuration ${!isActive ? 'activated' : 'deactivated'} successfully`
      });

      // Trigger refresh
      setRefreshTrigger(prev => prev + 1);
    } catch (error) {
      console.error('Error updating configuration:', error);
      showNotification({
        type: 'error',
        message: 'Failed to update configuration'
      });
    }
  };

  // Handling loading state and missing connection ID
  if (connectionLoading) {
    return <LoadingSpinner size="lg" className="mx-auto my-12" />;
  }

  // If no active connection is available
  if (!activeConnection) {
    return (
      <EmptyState
        icon={ServerIcon}
        title="No connection selected"
        description="Please select a database connection to view anomaly configurations"
        actionText="Manage Connections"
        actionLink="/connections"
      />
    );
  }

  if (!connectionId || connectionId === 'undefined') {
    return (
      <div className="text-center py-10">
        <p className="text-red-500">No connection ID available. Please select a connection.</p>
        <Link
          to="/anomalies"
          className="mt-4 inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700"
        >
          Go to Anomaly Dashboard
        </Link>
      </div>
    );
  }

  return (
    <div className="flex flex-col space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-bold text-gray-900">Anomaly Detection Configurations</h1>

        <Link
          to={`/anomalies/${connectionId}/configs/new`}
          className="inline-flex items-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
        >
          <PlusIcon className="h-5 w-5 mr-2" />
          New Configuration
        </Link>
      </div>

      {shouldFetchData ? (
        <BatchRequest
          requests={requests}
          key={`configs-${refreshTrigger}-${connectionId}`}
          skipAuthWait={false} // Ensure auth is ready
        >
          {(data) => {
            const configs = data.configs?.configs || [];
            const tables = data.tables?.tables || [];

            // Filter configs based on search term and table filter
            const filteredConfigs = configs.filter(config => {
              const searchMatches =
                !searchTerm ||
                config.table_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
                config.metric_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
                config.detection_method?.toLowerCase().includes(searchTerm.toLowerCase());

              const tableMatches = !filterTable || config.table_name === filterTable;

              return searchMatches && tableMatches;
            });

            return (
              <>
                {/* Filters */}
                <div className="bg-white shadow rounded-md p-4">
                  <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                    {/* Search */}
                    <div>
                      <SearchInput
                        onSearch={setSearchTerm}
                        placeholder="Search tables, metrics..."
                      />
                    </div>

                    {/* Table filter */}
                    <div>
                      <select
                        className="block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                        value={filterTable}
                        onChange={(e) => setFilterTable(e.target.value)}
                      >
                        <option value="">All Tables</option>
                        {tables.map((table, index) => (
                          <option key={index} value={table}>
                            {table}
                          </option>
                        ))}
                      </select>
                    </div>
                  </div>
                </div>

                {/* Configurations list */}
                {filteredConfigs.length > 0 ? (
                  <div className="bg-white shadow overflow-hidden sm:rounded-md">
                    <ul className="divide-y divide-gray-200">
                      {filteredConfigs.map((config) => (
                        <li key={config.id}>
                          <div
                            className="block hover:bg-gray-50 cursor-pointer"
                            onClick={() => navigate(`/anomalies/${connectionId}/configs/${config.id}`)}
                          >
                            <div className="px-4 py-4 sm:px-6">
                              <div className="flex items-center justify-between">
                                <div className="flex items-center">
                                  <p className="text-sm font-medium text-primary-600 truncate">
                                    {config.table_name}
                                    {config.column_name && `.${config.column_name}`}
                                  </p>
                                  <span className={`ml-2 px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${config.is_active ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-800'}`}>
                                    {config.is_active ? 'Active' : 'Inactive'}
                                  </span>
                                </div>
                                <div className="flex items-center space-x-2">
                                  <button
                                    onClick={(e) => handleToggleConfig(config.id, config.is_active, e)}
                                    className={`p-1 rounded-full ${config.is_active ? 'text-green-600 hover:text-green-900' : 'text-gray-400 hover:text-gray-600'}`}
                                  >
                                    {config.is_active ?
                                      <CheckCircleIcon className="h-5 w-5" /> :
                                      <XCircleIcon className="h-5 w-5" />
                                    }
                                  </button>
                                  <button
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      navigate(`/anomalies/${connectionId}/configs/${config.id}`);
                                    }}
                                    className="p-1 rounded-full text-primary-600 hover:text-primary-900"
                                  >
                                    <PencilIcon className="h-5 w-5" />
                                  </button>
                                  <button
                                    onClick={(e) => handleDeleteConfig(config.id, e)}
                                    className="p-1 rounded-full text-red-600 hover:text-red-900"
                                  >
                                    <TrashIcon className="h-5 w-5" />
                                  </button>
                                </div>
                              </div>
                              <div className="mt-2 sm:flex sm:justify-between">
                                <div className="sm:flex">
                                  <p className="flex items-center text-sm text-gray-500">
                                    <AdjustmentsHorizontalIcon className="flex-shrink-0 mr-1.5 h-5 w-5 text-gray-400" />
                                    {config.detection_method} (sensitivity: {config.sensitivity})
                                  </p>
                                  <p className="mt-2 flex items-center text-sm text-gray-500 sm:mt-0 sm:ml-6">
                                    <span className="font-medium">Metric:</span>
                                    <span className="ml-1">{config.metric_name}</span>
                                  </p>
                                </div>
                              </div>
                            </div>
                          </div>
                        </li>
                      ))}
                    </ul>
                  </div>
                ) : (
                  <EmptyState
                    icon={AdjustmentsHorizontalIcon}
                    title="No configurations found"
                    description="Get started by creating your first anomaly detection configuration."
                    actionText="Create Configuration"
                    actionLink={`/anomalies/${connectionId}/configs/new`}
                  />
                )}
              </>
            );
          }}
        </BatchRequest>
      ) : (
        <div className="text-center py-10">
          <LoadingSpinner size="lg" className="mx-auto" />
          <p className="mt-4 text-gray-500">Loading connection data...</p>
        </div>
      )}
    </div>
  );
};

export default AnomalyConfigsPage;