// src/pages/Anomaly/AnomalyExplorerPage.js

import React, { useState, useEffect, useMemo } from 'react';
import { useConnection } from '../../../contexts/EnhancedConnectionContext';
import { useUI } from '../../../contexts/UIContext';
import { useParams, useSearchParams, Link } from 'react-router-dom';
import { useNavigate } from 'react-router-dom';
import {
  ExclamationTriangleIcon,
  FunnelIcon,
  MagnifyingGlassIcon,
  ArrowPathIcon,
  CheckIcon,
  ClockIcon,
  XMarkIcon,
  ExclamationCircleIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import SearchInput from '../../../components/common/SearchInput';
import BatchRequest from '../../../components/common/BatchRequest';
import AnomalyDetailModal from './AnomalyDetailModal';

const AnomalyExplorerPage = () => {
  const { activeConnection, loading: connectionLoading } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();
  const { connectionId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate(); // Add this missing import

  const [searchTerm, setSearchTerm] = useState('');
  const [filterStatus, setFilterStatus] = useState(searchParams.get('status') || 'all');
  const [filterSeverity, setFilterSeverity] = useState(searchParams.get('severity') || 'all');
  const [filterTable, setFilterTable] = useState(searchParams.get('table') || '');
  const [timeRange, setTimeRange] = useState(parseInt(searchParams.get('days') || '30'));

  const [selectedAnomalyId, setSelectedAnomalyId] = useState(null);
  const [refreshTrigger, setRefreshTrigger] = useState(0);

  // Check for valid connection ID and redirect if needed
  useEffect(() => {
    if (!connectionLoading && !connectionId && activeConnection) {
      // Redirect to the page with a valid connection ID
      navigate(`/anomalies/${activeConnection.id}/explorer`, { replace: true });
    }
  }, [connectionId, connectionLoading, activeConnection, navigate]);

  // Update URL with filters
  useEffect(() => {
    const params = new URLSearchParams();
    if (filterStatus !== 'all') params.set('status', filterStatus);
    if (filterSeverity !== 'all') params.set('severity', filterSeverity);
    if (filterTable) params.set('table', filterTable);
    params.set('days', timeRange.toString());

    setSearchParams(params);
  }, [filterStatus, filterSeverity, filterTable, timeRange, setSearchParams]);

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Dashboard', href: '/dashboard' },
      { name: 'Anomaly Detection', href: '/anomalies' },
      { name: activeConnection?.name || 'Connection', href: `/anomalies/${connectionId}` },
      { name: 'Explorer' }
    ]);
  }, [updateBreadcrumbs, connectionId, activeConnection]);

  // Handle refresh
  const handleRefresh = () => {
    setRefreshTrigger(prev => prev + 1);
  };

  // Define the requests for batch loading - only when connection ID is valid
  const shouldFetchData = !connectionLoading && connectionId && connectionId !== 'undefined';
  const requests = useMemo(() => shouldFetchData ? [
    {
      id: 'anomalies',
      path: `/connections/${connectionId}/anomalies`,
      params: {
        days: timeRange,
        status: filterStatus !== 'all' ? filterStatus : undefined,
        table_name: filterTable || undefined,
        limit: 100
      }
    },
    {
      id: 'tables',
      path: `/connections/${connectionId}/tables`
    }
  ] : [], [connectionId, timeRange, filterStatus, filterTable, refreshTrigger, shouldFetchData]);

  // Handle status update
  const handleStatusUpdate = async (anomalyId, newStatus, note) => {
    // Skip if no valid connection ID
    if (!connectionId || connectionId === 'undefined') {
      showNotification({
        type: 'error',
        message: 'Cannot update status: No connection selected'
      });
      return;
    }

    try {
      const response = await fetch(`/api/connections/${connectionId}/anomalies/${anomalyId}/status`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          status: newStatus,
          resolution_note: note
        })
      });

      if (!response.ok) {
        throw new Error('Failed to update status');
      }

      // Show notification
      showNotification({
        type: 'success',
        message: `Anomaly status updated to ${newStatus}`
      });

      // Refresh data
      handleRefresh();

      // Close modal if open
      if (selectedAnomalyId === anomalyId) {
        setSelectedAnomalyId(null);
      }

    } catch (error) {
      console.error('Error updating anomaly status:', error);
      showNotification({
        type: 'error',
        message: 'Failed to update anomaly status'
      });
    }
  };

  // Get severity icon
  const getSeverityIcon = (severity) => {
    switch (severity) {
      case 'high':
        return <ExclamationTriangleIcon className="h-5 w-5 text-red-600" aria-hidden="true" />;
      case 'medium':
        return <ExclamationCircleIcon className="h-5 w-5 text-yellow-500" aria-hidden="true" />;
      case 'low':
        return <ExclamationCircleIcon className="h-5 w-5 text-green-500" aria-hidden="true" />;
      default:
        return <ExclamationCircleIcon className="h-5 w-5 text-gray-400" aria-hidden="true" />;
    }
  };

  // Get status icon
  const getStatusIcon = (status) => {
    switch (status) {
      case 'open':
        return <ClockIcon className="h-5 w-5 text-blue-500" aria-hidden="true" />;
      case 'acknowledged':
        return <ExclamationCircleIcon className="h-5 w-5 text-yellow-500" aria-hidden="true" />;
      case 'resolved':
        return <CheckIcon className="h-5 w-5 text-green-500" aria-hidden="true" />;
      case 'expected':
        return <CheckIcon className="h-5 w-5 text-gray-500" aria-hidden="true" />;
      default:
        return <ClockIcon className="h-5 w-5 text-gray-400" aria-hidden="true" />;
    }
  };

  // Status badge style
  const getStatusBadgeStyle = (status) => {
    switch (status) {
      case 'open':
        return 'bg-blue-100 text-blue-800';
      case 'acknowledged':
        return 'bg-yellow-100 text-yellow-800';
      case 'resolved':
        return 'bg-green-100 text-green-800';
      case 'expected':
        return 'bg-gray-100 text-gray-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  // Helper to format metric display value
  const formatMetricValue = (value) => {
    if (value === null || value === undefined) return 'N/A';

    // Check if it's a number
    if (!isNaN(parseFloat(value))) {
      const num = parseFloat(value);
      if (num === Math.floor(num)) {
        return num.toFixed(0);
      } else {
        return num.toFixed(2);
      }
    }

    return String(value);
  };

  // Showing loading state during connection loading
  if (connectionLoading) {
    return <LoadingSpinner size="lg" className="mx-auto my-12" />;
  }

  // Show error if no connection ID is available
  if (!connectionId || connectionId === 'undefined') {
    return (
      <div className="text-center py-10">
        <ExclamationCircleIcon className="mx-auto h-12 w-12 text-red-500" />
        <p className="mt-2 text-lg font-medium text-gray-900">No connection selected</p>
        <p className="mt-1 text-sm text-gray-500">Please select a connection to view anomalies.</p>
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
        <h1 className="text-2xl font-bold text-gray-900">Anomaly Explorer</h1>

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
            className="inline-flex items-center px-3 py-2 border border-transparent shadow-sm text-sm leading-4 font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          >
            <ArrowPathIcon className="h-4 w-4 mr-2" />
            Refresh
          </button>
        </div>
      </div>

      {shouldFetchData ? (
        <BatchRequest
          requests={requests}
          key={`explorer-${refreshTrigger}`}
        >
          {(data) => {
            const anomalies = data.anomalies?.anomalies || [];
            const tables = data.tables?.tables || [];

            // Filter anomalies based on search term and severity
            const filteredAnomalies = anomalies.filter(anomaly => {
              // Filter by search term
              const searchMatches =
                !searchTerm ||
                anomaly.table_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
                anomaly.column_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
                anomaly.metric_name?.toLowerCase().includes(searchTerm.toLowerCase());

              // Filter by severity if needed
              const severityMatches =
                filterSeverity === 'all' ||
                anomaly.severity === filterSeverity;

              return searchMatches && severityMatches;
            });

            return (
              <>
                {/* Filters */}
                <div className="bg-white shadow overflow-hidden sm:rounded-md p-4">
                  <div className="grid grid-cols-1 gap-6 lg:grid-cols-4">
                    {/* Search */}
                    <div className="col-span-1 lg:col-span-2">
                      <div className="mt-1 flex rounded-md shadow-sm">
                        <div className="relative flex items-stretch flex-grow">
                          <SearchInput
                            onSearch={setSearchTerm}
                            placeholder="Search tables, columns, metrics..."
                            initialValue={searchTerm}
                          />
                        </div>
                      </div>
                    </div>

                    {/* Status filter */}
                    <div>
                      <label htmlFor="status" className="block text-sm font-medium text-gray-700">
                        Status
                      </label>
                      <select
                        id="status"
                        name="status"
                        className="mt-1 block w-full rounded-md border-gray-300 py-2 pl-3 pr-10 text-base focus:border-primary-500 focus:outline-none focus:ring-primary-500 sm:text-sm"
                        value={filterStatus}
                        onChange={(e) => setFilterStatus(e.target.value)}
                      >
                        <option value="all">All Statuses</option>
                        <option value="open">Open</option>
                        <option value="acknowledged">Acknowledged</option>
                        <option value="resolved">Resolved</option>
                        <option value="expected">Expected</option>
                      </select>
                    </div>

                    {/* Table filter */}
                    <div>
                      <label htmlFor="table" className="block text-sm font-medium text-gray-700">
                        Table
                      </label>
                      <select
                        id="table"
                        name="table"
                        className="mt-1 block w-full rounded-md border-gray-300 py-2 pl-3 pr-10 text-base focus:border-primary-500 focus:outline-none focus:ring-primary-500 sm:text-sm"
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

                    {/* Filter info */}
                    <div className="col-span-1 lg:col-span-4 flex justify-between items-center border-t pt-4 mt-2">
                      <div className="text-sm text-gray-700">
                        <span className="font-medium">{filteredAnomalies.length}</span> anomalies found
                        {searchTerm && (
                          <span className="ml-1">
                            matching <span className="font-medium">"{searchTerm}"</span>
                          </span>
                        )}
                      </div>

                      {/* Clear filters */}
                      {(searchTerm || filterStatus !== 'all' || filterSeverity !== 'all' || filterTable) && (
                        <button
                          type="button"
                          className="inline-flex items-center px-2.5 py-1.5 border border-gray-300 shadow-sm text-xs font-medium rounded text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                          onClick={() => {
                            setSearchTerm('');
                            setFilterStatus('all');
                            setFilterSeverity('all');
                            setFilterTable('');
                          }}
                        >
                          <XMarkIcon className="mr-1.5 h-4 w-4 text-gray-500" />
                          Clear filters
                        </button>
                      )}
                    </div>
                  </div>
                </div>

                {/* Results */}
                <div className="bg-white shadow overflow-hidden sm:rounded-lg">
                  {filteredAnomalies.length > 0 ? (
                    <div className="overflow-x-auto">
                      <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-50">
                          <tr>
                            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Severity
                            </th>
                            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Table/Column
                            </th>
                            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Metric
                            </th>
                            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Value
                            </th>
                            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Detected At
                            </th>
                            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Status
                            </th>
                            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Actions
                            </th>
                          </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                          {filteredAnomalies.map((anomaly) => (
                            <tr
                              key={anomaly.id}
                              className="hover:bg-gray-50 cursor-pointer"
                              onClick={() => setSelectedAnomalyId(anomaly.id)}
                            >
                              <td className="px-6 py-4 whitespace-nowrap">
                                <div className="flex items-center">
                                  {getSeverityIcon(anomaly.severity)}
                                  <span className="ml-1 text-sm capitalize">{anomaly.severity}</span>
                                </div>
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap">
                                <div className="text-sm font-medium text-gray-900">
                                  {anomaly.table_name}
                                </div>
                                {anomaly.column_name && (
                                  <div className="text-sm text-gray-500">
                                    {anomaly.column_name}
                                  </div>
                                )}
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap">
                                <div className="text-sm text-gray-900">
                                  {anomaly.metric_name}
                                </div>
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap">
                                <div className="text-sm text-gray-900">
                                  {formatMetricValue(anomaly.metric_value)}
                                </div>
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                {new Date(anomaly.detected_at).toLocaleString()}
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap">
                                <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${getStatusBadgeStyle(anomaly.status)}`}>
                                  {anomaly.status}
                                </span>
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                <div className="flex items-center space-x-2">
                                  <button
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      setSelectedAnomalyId(anomaly.id);
                                    }}
                                    className="text-primary-600 hover:text-primary-900"
                                  >
                                    View
                                  </button>
                                  {anomaly.status === 'open' && (
                                    <button
                                      onClick={(e) => {
                                        e.stopPropagation();
                                        handleStatusUpdate(anomaly.id, 'acknowledged');
                                      }}
                                      className="text-yellow-600 hover:text-yellow-900"
                                    >
                                      Acknowledge
                                    </button>
                                  )}
                                  {(anomaly.status === 'open' || anomaly.status === 'acknowledged') && (
                                    <button
                                      onClick={(e) => {
                                        e.stopPropagation();
                                        handleStatusUpdate(anomaly.id, 'resolved');
                                      }}
                                      className="text-green-600 hover:text-green-900"
                                    >
                                      Resolve
                                    </button>
                                  )}
                                </div>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="text-center py-12">
                      <div className="flex justify-center">
                        <FunnelIcon className="h-12 w-12 text-gray-400" />
                      </div>
                      <h3 className="mt-2 text-sm font-medium text-gray-900">No anomalies found</h3>
                      <p className="mt-1 text-sm text-gray-500">
                        Try adjusting your filters or search criteria.
                      </p>
                      <div className="mt-6">
                        <button
                          type="button"
                          className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                          onClick={() => {
                            setSearchTerm('');
                            setFilterStatus('all');
                            setFilterSeverity('all');
                            setFilterTable('');
                          }}
                        >
                          <MagnifyingGlassIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
                          Clear filters
                        </button>
                      </div>
                    </div>
                  )}
                </div>

                {/* Detail modal */}
                {selectedAnomalyId && (
                  <AnomalyDetailModal
                    connectionId={connectionId}
                    anomalyId={selectedAnomalyId}
                    onClose={() => setSelectedAnomalyId(null)}
                    onStatusUpdate={handleStatusUpdate}
                  />
                )}
              </>
            );
          }}
        </BatchRequest>
      ) : (
        <div className="flex justify-center py-12">
          <LoadingSpinner size="lg" />
          <span className="ml-3 text-gray-600">Loading data...</span>
        </div>
      )}
    </div>
  );
};

export default AnomalyExplorerPage;