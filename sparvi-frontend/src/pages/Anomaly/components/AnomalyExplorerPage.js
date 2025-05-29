import React, { useState, useEffect } from 'react';
import { useConnection } from '../../../contexts/EnhancedConnectionContext';
import { useUI } from '../../../contexts/UIContext';
import { useParams, useSearchParams, Link, useNavigate } from 'react-router-dom';
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
import AnomalyDetailModal from './AnomalyDetailModal';
import anomalyService from '../../../services/anomalyService';
import { schemaAPI } from '../../../api/enhancedApiService';

const AnomalyExplorerPage = () => {
  const { activeConnection, loading: connectionLoading } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();
  const { connectionId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();

  // Filter state
  const [searchTerm, setSearchTerm] = useState('');
  const [filterStatus, setFilterStatus] = useState(searchParams.get('status') || 'all');
  const [filterSeverity, setFilterSeverity] = useState(searchParams.get('severity') || 'all');
  const [filterTable, setFilterTable] = useState(searchParams.get('table') || '');
  const [timeRange, setTimeRange] = useState(parseInt(searchParams.get('days') || '30'));

  // Component state
  const [loading, setLoading] = useState(false);
  const [loadingTables, setLoadingTables] = useState(false);
  const [selectedAnomalyId, setSelectedAnomalyId] = useState(null);
  const [error, setError] = useState(null);

  // Data state
  const [anomalies, setAnomalies] = useState([]);
  const [tables, setTables] = useState([]);

  // Check for valid connection ID and redirect if needed
  useEffect(() => {
    if (!connectionLoading && !connectionId && activeConnection) {
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
      { name: 'Anomaly Explorer' }
    ]);
  }, [updateBreadcrumbs, connectionId, activeConnection]);

  // Load tables - using schemaAPI
  useEffect(() => {
    const loadTables = async () => {
      if (!connectionId || connectionId === 'undefined') return;

      try {
        setLoadingTables(true);
        console.log(`Loading tables for connection ${connectionId}`);

        const response = await schemaAPI.getTables(connectionId);
        console.log('Tables response:', response);

        // Handle different possible response structures
        let tablesData = [];
        if (response?.tables && Array.isArray(response.tables)) {
          tablesData = response.tables;
        } else if (response?.data?.tables && Array.isArray(response.data.tables)) {
          tablesData = response.data.tables;
        } else if (Array.isArray(response)) {
          tablesData = response;
        } else {
          console.warn('Unexpected tables response format:', response);
        }

        console.log(`Found ${tablesData.length} tables for connection ${connectionId}`);
        setTables(tablesData);
      } catch (error) {
        console.error(`Error loading tables for connection ${connectionId}:`, error);
        showNotification(`Failed to load tables: ${error.message}`, 'error');
      } finally {
        setLoadingTables(false);
      }
    };

    loadTables();
  }, [connectionId, showNotification]);

  // Load anomalies - using anomalyService
  useEffect(() => {
    const loadAnomalies = async () => {
      if (!connectionId || connectionId === 'undefined') return;

      try {
        setLoading(true);
        setError(null);
        console.log(`Loading anomalies for connection ${connectionId}`);

        // Build filters object
        const filters = {
          days: timeRange
        };

        if (filterStatus !== 'all') filters.status = filterStatus;
        if (filterSeverity !== 'all') filters.severity = filterSeverity;
        if (filterTable) filters.table_name = filterTable;

        const anomaliesData = await anomalyService.getAnomalies(connectionId, filters);
        console.log(`Found ${anomaliesData.length} anomalies`);
        setAnomalies(anomaliesData);
      } catch (error) {
        console.error(`Error loading anomalies for connection ${connectionId}:`, error);
        const errorMessage = `Failed to load anomalies: ${error.message}`;
        showNotification(errorMessage, 'error');
        setError(errorMessage);
      } finally {
        setLoading(false);
      }
    };

    loadAnomalies();
  }, [connectionId, filterStatus, filterSeverity, filterTable, timeRange, showNotification]);

  // Handle status update from modal
  const handleStatusUpdate = (anomalyId, newStatus, note) => {
    // Update the anomaly in the list
    setAnomalies(prev => prev.map(anomaly =>
      anomaly.id === anomalyId
        ? {
            ...anomaly,
            status: newStatus,
            resolution_note: note,
            resolved_at: newStatus === 'resolved' ? new Date().toISOString() : null
          }
        : anomaly
    ));

    // Close the modal
    setSelectedAnomalyId(null);

    showNotification(`Anomaly ${newStatus} successfully`, 'success');
  };

  // Filter anomalies based on search term
  const filteredAnomalies = anomalies.filter(anomaly => {
    if (!searchTerm) return true;

    const searchLower = searchTerm.toLowerCase();
    return (
      anomaly.table_name?.toLowerCase().includes(searchLower) ||
      anomaly.column_name?.toLowerCase().includes(searchLower) ||
      anomaly.metric_name?.toLowerCase().includes(searchLower)
    );
  });

  // Get severity icon and color
  const getSeverityIcon = (severity) => {
    switch (severity) {
      case 'high':
        return <ExclamationTriangleIcon className="h-5 w-5 text-red-500" />;
      case 'medium':
        return <ExclamationCircleIcon className="h-5 w-5 text-yellow-500" />;
      case 'low':
        return <ExclamationCircleIcon className="h-5 w-5 text-green-500" />;
      default:
        return <ExclamationCircleIcon className="h-5 w-5 text-gray-400" />;
    }
  };

  // Get status icon and color
  const getStatusIcon = (status) => {
    switch (status) {
      case 'resolved':
        return <CheckIcon className="h-5 w-5 text-green-500" />;
      case 'acknowledged':
        return <ClockIcon className="h-5 w-5 text-yellow-500" />;
      case 'open':
        return <XMarkIcon className="h-5 w-5 text-red-500" />;
      default:
        return <ExclamationCircleIcon className="h-5 w-5 text-gray-400" />;
    }
  };

  // Format metric value
  const formatMetricValue = (value) => {
    return anomalyService.formatMetricValue(value);
  };

  // Format date
  const formatDate = (dateString) => {
    return new Date(dateString).toLocaleString();
  };

  // Quick bulk actions
  const handleBulkStatusUpdate = async (selectedIds, newStatus) => {
    if (!connectionId || connectionId === 'undefined') return;

    try {
      await Promise.all(
        selectedIds.map(id => anomalyService.updateAnomalyStatus(connectionId, id, newStatus, ''))
      );

      // Update local state
      setAnomalies(prev => prev.map(anomaly =>
        selectedIds.includes(anomaly.id)
          ? {
              ...anomaly,
              status: newStatus,
              resolved_at: newStatus === 'resolved' ? new Date().toISOString() : null
            }
          : anomaly
      ));

      showNotification(`${selectedIds.length} anomalies updated successfully`, 'success');
    } catch (error) {
      console.error('Error updating anomalies:', error);
      showNotification(`Failed to update anomalies: ${error.message}`, 'error');
    }
  };

  if (connectionLoading) {
    return <LoadingSpinner size="lg" className="mx-auto my-12" />;
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
        <h1 className="text-2xl font-bold text-gray-900">Anomaly Explorer</h1>
        <button
          onClick={() => window.location.reload()}
          className="inline-flex items-center px-3 py-2 border border-gray-300 shadow-sm text-sm leading-4 font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
        >
          <ArrowPathIcon className="h-4 w-4 mr-2" />
          Refresh
        </button>
      </div>

      {error && (
        <div className="rounded-md bg-red-50 p-4">
          <div className="flex">
            <div className="flex-shrink-0">
              <ExclamationTriangleIcon className="h-5 w-5 text-red-400" aria-hidden="true" />
            </div>
            <div className="ml-3">
              <h3 className="text-sm font-medium text-red-800">Error</h3>
              <div className="mt-2 text-sm text-red-700">{error}</div>
            </div>
          </div>
        </div>
      )}

      {/* Filters */}
      <div className="bg-white shadow rounded-md p-4">
        <div className="flex items-center mb-4">
          <FunnelIcon className="h-5 w-5 text-gray-400 mr-2" />
          <h3 className="text-sm font-medium text-gray-900">Filters</h3>
        </div>

        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-5">
          {/* Search */}
          <div>
            <SearchInput
              onSearch={setSearchTerm}
              placeholder="Search tables, metrics..."
            />
          </div>

          {/* Status filter */}
          <div>
            <select
              className="block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
              value={filterStatus}
              onChange={(e) => setFilterStatus(e.target.value)}
            >
              <option value="all">All Statuses</option>
              <option value="open">Open</option>
              <option value="acknowledged">Acknowledged</option>
              <option value="resolved">Resolved</option>
            </select>
          </div>

          {/* Severity filter */}
          <div>
            <select
              className="block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
              value={filterSeverity}
              onChange={(e) => setFilterSeverity(e.target.value)}
            >
              <option value="all">All Severities</option>
              <option value="high">High</option>
              <option value="medium">Medium</option>
              <option value="low">Low</option>
            </select>
          </div>

          {/* Table filter */}
          <div>
            <select
              className="block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
              value={filterTable}
              onChange={(e) => setFilterTable(e.target.value)}
              disabled={loadingTables}
            >
              {loadingTables ? (
                <option>Loading tables...</option>
              ) : (
                <>
                  <option value="">All Tables</option>
                  {tables.map((table, index) => (
                    <option key={index} value={table}>
                      {table}
                    </option>
                  ))}
                </>
              )}
            </select>
          </div>

          {/* Time range */}
          <div>
            <select
              className="block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
              value={timeRange}
              onChange={(e) => setTimeRange(parseInt(e.target.value))}
            >
              <option value={7}>Last 7 days</option>
              <option value={30}>Last 30 days</option>
              <option value={90}>Last 90 days</option>
              <option value={365}>Last year</option>
            </select>
          </div>
        </div>
      </div>

      {/* Results summary */}
      <div className="flex items-center justify-between bg-gray-50 px-4 py-2 rounded-md">
        <p className="text-sm text-gray-600">
          Showing {filteredAnomalies.length} of {anomalies.length} anomalies
        </p>
        <div className="flex items-center space-x-2 text-sm text-gray-500">
          <span className="flex items-center">
            <span className="w-2 h-2 bg-red-500 rounded-full mr-1"></span>
            High: {filteredAnomalies.filter(a => a.severity === 'high').length}
          </span>
          <span className="flex items-center">
            <span className="w-2 h-2 bg-yellow-500 rounded-full mr-1"></span>
            Medium: {filteredAnomalies.filter(a => a.severity === 'medium').length}
          </span>
          <span className="flex items-center">
            <span className="w-2 h-2 bg-green-500 rounded-full mr-1"></span>
            Low: {filteredAnomalies.filter(a => a.severity === 'low').length}
          </span>
        </div>
      </div>

      {/* Anomalies list */}
      {loading ? (
        <div className="flex justify-center py-12">
          <LoadingSpinner size="lg" />
          <span className="ml-3 text-gray-600">Loading anomalies...</span>
        </div>
      ) : filteredAnomalies.length > 0 ? (
        <div className="bg-white shadow overflow-hidden sm:rounded-md">
          <ul className="divide-y divide-gray-200">
            {filteredAnomalies.map((anomaly) => (
              <li key={anomaly.id}>
                <div
                  className="block hover:bg-gray-50 cursor-pointer"
                  onClick={() => setSelectedAnomalyId(anomaly.id)}
                >
                  <div className="px-4 py-4 sm:px-6">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center">
                        {getSeverityIcon(anomaly.severity)}
                        <div className="ml-2">
                          <p className="text-sm font-medium text-primary-600 truncate">
                            {anomaly.table_name}
                            {anomaly.column_name && `.${anomaly.column_name}`}
                          </p>
                          <p className="text-sm text-gray-500">
                            {anomaly.metric_name}: {formatMetricValue(anomaly.metric_value)}
                          </p>
                        </div>
                      </div>
                      <div className="flex items-center space-x-2">
                        {getStatusIcon(anomaly.status)}
                        <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                          anomaly.status === 'resolved' ? 'bg-green-100 text-green-800' :
                          anomaly.status === 'acknowledged' ? 'bg-yellow-100 text-yellow-800' :
                          'bg-red-100 text-red-800'
                        }`}>
                          {anomaly.status}
                        </span>
                      </div>
                    </div>
                    <div className="mt-2 sm:flex sm:justify-between">
                      <div className="sm:flex">
                        <p className="flex items-center text-sm text-gray-500">
                          Detected: {formatDate(anomaly.detected_at)}
                        </p>
                        <p className="mt-2 flex items-center text-sm text-gray-500 sm:mt-0 sm:ml-6">
                          Score: {parseFloat(anomaly.score || 0).toFixed(2)}
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
        <div className="text-center py-12">
          <MagnifyingGlassIcon className="mx-auto h-12 w-12 text-gray-400" />
          <h3 className="mt-2 text-sm font-medium text-gray-900">No anomalies found</h3>
          <p className="mt-1 text-sm text-gray-500">
            Try adjusting your filters or time range to see more results.
          </p>
        </div>
      )}

      {/* Detail Modal */}
      {selectedAnomalyId && (
        <AnomalyDetailModal
          connectionId={connectionId}
          anomalyId={selectedAnomalyId}
          onClose={() => setSelectedAnomalyId(null)}
          onStatusUpdate={handleStatusUpdate}
        />
      )}
    </div>
  );
};

export default AnomalyExplorerPage;