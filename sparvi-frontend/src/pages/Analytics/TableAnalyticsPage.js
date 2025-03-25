import React, { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useHistoricalMetrics } from '../../hooks/useAnalytics';
import { schemaAPI } from '../../api/enhancedApiService';
import {
  ArrowLeftIcon,
  TableCellsIcon,
  ListBulletIcon,
  ChartBarIcon,
  InformationCircleIcon
} from '@heroicons/react/24/outline';
import TrendChart from '../../components/analytics/TrendChart';
import MetricCard from '../../components/analytics/MetricCard';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import { formatDate, formatNumber } from '../../utils/formatting';

const TableAnalyticsPage = () => {
  const { connectionId, tableName } = useParams();
  const { activeConnection, getConnection } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();
  const [tableInfo, setTableInfo] = useState(null);
  const [timeframe, setTimeframe] = useState(30); // Default to 30 days
  const [isLoading, setIsLoading] = useState(false);
  const [columns, setColumns] = useState([]);

  // Fetch historical metrics for row count
  const {
    data: rowCountData,
    isLoading: isRowCountLoading
  } = useHistoricalMetrics(
    connectionId,
    {
      metricName: 'row_count',
      tableName,
      days: timeframe,
      enabled: !!connectionId && !!tableName
    }
  );

  // Fetch historical metrics for validation success rate
  const {
    data: validationData,
    isLoading: isValidationLoading
  } = useHistoricalMetrics(
    connectionId,
    {
      metricName: 'validation_success',
      tableName,
      days: timeframe,
      enabled: !!connectionId && !!tableName
    }
  );

  // Fetch null percentage metrics
  const {
    data: nullPercentageData,
    isLoading: isNullPercentageLoading
  } = useHistoricalMetrics(
    connectionId,
    {
      metricName: 'null_percentage',
      tableName,
      days: timeframe,
      enabled: !!connectionId && !!tableName
    }
  );

  // Load connection and table info
  useEffect(() => {
    // Ensure we have an active connection matching the URL parameter
    const fetchConnectionIfNeeded = async () => {
      if (!activeConnection || activeConnection.id !== connectionId) {
        try {
          await getConnection(connectionId);
        } catch (error) {
          console.error('Error fetching connection:', error);
        }
      }
    };

    // Fetch table information
    const fetchTableInfo = async () => {
      setIsLoading(true);
      try {
        // Get columns first
        const columnsResponse = await schemaAPI.getColumns(connectionId, tableName);
        setColumns(columnsResponse.columns || []);

        // Get table statistics
        const statsResponse = await schemaAPI.getStatistics(connectionId, tableName);

        // Log the response to see what we're getting
        console.log('Table statistics response:', statsResponse);

        // Extract and normalize the statistics
        const stats = statsResponse.statistics || {};

        // Create a default tableInfo object with fallback values
        setTableInfo({
          schema: stats.schema || 'Default',
          row_count: stats.row_count || getLatestRowCount(),
          validation_success_rate: stats.validation_success_rate || getLatestValidationRate(),
          null_percentage: stats.null_percentage || getLatestNullPercentage(),
          last_analyzed: stats.last_analyzed || new Date().toISOString(), // Use current date if not provided
          ...stats // Include any other properties from the statistics
        });
      } catch (error) {
        console.error('Error fetching table information:', error);
        showNotification('Error loading table data', 'error');

        // Set default tableInfo even on error
        setTableInfo({
          schema: 'Default',
          row_count: getLatestRowCount(),
          validation_success_rate: getLatestValidationRate(),
          null_percentage: getLatestNullPercentage(),
          last_analyzed: new Date().toISOString()
        });
      } finally {
        setIsLoading(false);
      }
    };

    fetchConnectionIfNeeded();
    if (connectionId && tableName) {
      fetchTableInfo();
    }
  }, [connectionId, tableName, activeConnection, getConnection, showNotification]);

  // Helper functions to extract the latest metrics from historical data
  const getLatestRowCount = () => {
    if (rowCountData?.metrics && rowCountData.metrics.length > 0) {
      const sorted = [...rowCountData.metrics].sort((a, b) =>
        new Date(b.timestamp) - new Date(a.timestamp)
      );
      return sorted[0].metric_value || 0;
    }
    return 0;
  };

  const getLatestValidationRate = () => {
    if (validationData?.metrics && validationData.metrics.length > 0) {
      const sorted = [...validationData.metrics].sort((a, b) =>
        new Date(b.timestamp) - new Date(a.timestamp)
      );
      return sorted[0].metric_value || 95;
    }
    return 95; // Default fallback
  };

  const getLatestNullPercentage = () => {
    if (nullPercentageData?.metrics && nullPercentageData.metrics.length > 0) {
      const sorted = [...nullPercentageData.metrics].sort((a, b) =>
        new Date(b.timestamp) - new Date(a.timestamp)
      );
      return sorted[0].metric_value || 5;
    }
    return 5; // Default fallback
  };

  // Update breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Analytics', href: '/analytics' },
      { name: tableName, href: `/analytics/table/${connectionId}/${tableName}` },
    ]);
  }, [updateBreadcrumbs, connectionId, tableName]);

  // Handle timeframe change
  const handleTimeframeChange = (days) => {
    setTimeframe(days);
  };

  // If everything is still loading, show a loading spinner
  if (isLoading && !tableInfo) {
    return (
      <div className="flex justify-center items-center h-64">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <div className="flex items-center mb-4">
          <Link to="/analytics" className="mr-4 text-secondary-500 hover:text-secondary-700">
            <ArrowLeftIcon className="h-5 w-5" />
          </Link>
          <h1 className="text-2xl font-bold text-secondary-900">
            Table Analytics: {tableName}
          </h1>
        </div>

        <div className="flex justify-between items-center">
          <p className="text-secondary-500">
            View data quality metrics and trends for this table
          </p>

          {/* Time range selector */}
          <div className="flex items-center space-x-2 bg-white rounded-md shadow-sm p-1">
            <button
              onClick={() => handleTimeframeChange(7)}
              className={`px-3 py-1.5 text-sm font-medium rounded-md ${
                timeframe === 7
                  ? 'bg-primary-100 text-primary-700'
                  : 'text-secondary-500 hover:text-secondary-700'
              }`}
            >
              7 Days
            </button>
            <button
              onClick={() => handleTimeframeChange(30)}
              className={`px-3 py-1.5 text-sm font-medium rounded-md ${
                timeframe === 30
                  ? 'bg-primary-100 text-primary-700'
                  : 'text-secondary-500 hover:text-secondary-700'
              }`}
            >
              30 Days
            </button>
            <button
              onClick={() => handleTimeframeChange(90)}
              className={`px-3 py-1.5 text-sm font-medium rounded-md ${
                timeframe === 90
                  ? 'bg-primary-100 text-primary-700'
                  : 'text-secondary-500 hover:text-secondary-700'
              }`}
            >
              90 Days
            </button>
          </div>
        </div>
      </div>

      {/* Table Info */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex items-center mb-4">
          <TableCellsIcon className="h-6 w-6 text-primary-500 mr-2" />
          <h2 className="text-lg font-medium text-secondary-900">Table Overview</h2>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <div>
            <h3 className="text-sm font-medium text-secondary-500">Schema</h3>
            <p className="mt-1 text-base text-secondary-900">{tableInfo?.schema || 'Default'}</p>
          </div>

          <div>
            <h3 className="text-sm font-medium text-secondary-500">Row Count</h3>
            <p className="mt-1 text-base text-secondary-900">
              {tableInfo?.row_count ? formatNumber(tableInfo.row_count) : formatNumber(getLatestRowCount())}
            </p>
          </div>

          <div>
            <h3 className="text-sm font-medium text-secondary-500">Column Count</h3>
            <p className="mt-1 text-base text-secondary-900">
              {columns.length ? formatNumber(columns.length) : 'Unknown'}
            </p>
          </div>

          <div>
            <h3 className="text-sm font-medium text-secondary-500">Last Analyzed</h3>
            <p className="mt-1 text-base text-secondary-900">
              {tableInfo?.last_analyzed
                ? formatDate(tableInfo.last_analyzed, true)
                : formatDate(new Date(), true)}
            </p>
          </div>
        </div>
      </div>

      {/* Metrics Grid */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <MetricCard
          title="Row Count"
          value={tableInfo?.row_count || getLatestRowCount()}
          format="number"
          icon={ListBulletIcon}
          isLoading={isRowCountLoading}
          size="large"
        />

        <MetricCard
          title="Validation Success"
          value={tableInfo?.validation_success_rate || getLatestValidationRate()}
          format="percentage"
          icon={ChartBarIcon}
          isLoading={isValidationLoading}
          size="large"
        />

        <MetricCard
          title="Null Percentage"
          value={tableInfo?.null_percentage || getLatestNullPercentage()}
          format="percentage"
          icon={InformationCircleIcon}
          isLoading={isNullPercentageLoading}
          inverse={true} // Lower is better for null percentage
          size="large"
        />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <TrendChart
          title="Row Count Trend"
          data={rowCountData?.metrics || []}
          yKey="metric_value"
          type="line"
          color="#6366f1"
          loading={isRowCountLoading}
          height={250}
          emptyMessage="No row count data available for this time period."
        />

        <TrendChart
          title="Validation Success Rate"
          data={validationData?.metrics || []}
          yKey="metric_value"
          type="area"
          color="#10b981"
          valueFormat="percentage"
          loading={isValidationLoading}
          height={250}
          emptyMessage="No validation data available for this time period."
        />
      </div>

      {/* Column Metrics */}
      <div>
        <h2 className="text-lg font-medium text-secondary-900 mb-4">Column Metrics</h2>

        {columns.length === 0 ? (
          <div className="bg-white rounded-lg shadow p-6 text-center">
            <InformationCircleIcon className="h-12 w-12 text-secondary-400 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-secondary-900">No Column Data Available</h3>
            <p className="mt-2 text-secondary-500">
              Column metrics could not be loaded for this table.
            </p>
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div className="flex items-center p-4 border-b border-secondary-200 bg-secondary-50">
              <div className="w-1/4 font-medium text-secondary-700">Column Name</div>
              <div className="w-1/4 font-medium text-secondary-700">Data Type</div>
              <div className="w-1/4 font-medium text-secondary-700">Null Percentage</div>
              <div className="w-1/4 font-medium text-secondary-700">Unique Values</div>
            </div>

            <div className="divide-y divide-secondary-200">
              {columns.map((column, index) => (
                <div key={index} className="flex items-center p-4 hover:bg-secondary-50">
                  <div className="w-1/4 font-medium text-primary-600">
                    {column.name}
                  </div>
                  <div className="w-1/4 text-secondary-500">
                    {column.data_type}
                  </div>
                  <div className="w-1/4">
                    <div className="flex items-center">
                      <div className="h-2 flex-1 bg-secondary-200 rounded-full max-w-xs">
                        <div
                          className="h-2 bg-warning-500 rounded-full"
                          style={{ width: `${Math.min(100, column.null_percentage || 0)}%` }}
                        ></div>
                      </div>
                      <span className="ml-2 text-sm text-secondary-700">
                        {(column.null_percentage || 0).toFixed(1)}%
                      </span>
                    </div>
                  </div>
                  <div className="w-1/4 text-secondary-700">
                    {column.unique_count ? formatNumber(column.unique_count) : 'N/A'}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Null Percentage Trend */}
      <div>
        <h2 className="text-lg font-medium text-secondary-900 mb-4">Null Percentage Trend</h2>
        <TrendChart
          title="Overall Null Percentage"
          data={nullPercentageData?.metrics || []}
          yKey="metric_value"
          type="area"
          color="#f59e0b"
          valueFormat="percentage"
          loading={isNullPercentageLoading}
          height={250}
        />
      </div>
    </div>
  );
};

export default TableAnalyticsPage;