// src/pages/Analytics/AnalyticsPage.js
import React, { useEffect, useState, Fragment } from 'react';
import { Link } from 'react-router-dom';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useAnalyticsDashboard, useHistoricalMetrics, useHighImpactObjects } from '../../hooks/useAnalytics';
import { schemaAPI } from '../../api/enhancedApiService';
import { Transition } from '@headlessui/react';
import {
  ChartBarIcon,
  ExclamationCircleIcon,
  SparklesIcon,
  TableCellsIcon,
  ArrowRightIcon,
  DocumentChartBarIcon,
  ArrowPathIcon,
  PresentationChartLineIcon,
  ChevronDownIcon,
  XMarkIcon,
  MagnifyingGlassIcon,
  FunnelIcon
} from '@heroicons/react/24/outline';
import {
  ClockIcon,
  ArrowTrendingUpIcon,
  ShieldCheckIcon
} from '@heroicons/react/24/solid';

// Import components
import HistoricalMetricsDashboard from '../../components/analytics/HistoricalMetricsDashboard';
import MetricCard from '../../components/analytics/MetricCard';
import TrendChart from '../../components/analytics/TrendChart';
import AnomalyCard from '../../components/analytics/AnomalyCard';
import InsightCard from '../../components/analytics/InsightCard';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import { formatDate } from '../../utils/formatting';

const AnalyticsPage = () => {
  const { activeConnection } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();
  const [timeframe, setTimeframe] = useState(30); // Default to 30 days
  const [showTableSelector, setShowTableSelector] = useState(false);
  const [tableList, setTableList] = useState([]);
  const [tableSearchTerm, setTableSearchTerm] = useState('');
  const [isLoadingTables, setIsLoadingTables] = useState(false);

  // Table filter state
  const [selectedTables, setSelectedTables] = useState([]);
  const [isFiltering, setIsFiltering] = useState(false); // True when filter is active
  const [showFilterDropdown, setShowFilterDropdown] = useState(false);

  // Update breadcrumbs when connection changes
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Analytics', href: '/analytics' },
    ]);
  }, [updateBreadcrumbs]);

  // Fetch tables for the dropdown when it's opened
  useEffect(() => {
    if ((showTableSelector || showFilterDropdown) && activeConnection?.id && tableList.length === 0) {
      const fetchTables = async () => {
        setIsLoadingTables(true);
        try {
          const response = await schemaAPI.getTables(activeConnection.id);
          const tables = Array.isArray(response) ? response :
                        response.tables ? response.tables :
                        response.data?.tables || [];
          setTableList(tables);
        } catch (error) {
          console.error('Error fetching tables:', error);
          showNotification('Failed to load tables', 'error');
        } finally {
          setIsLoadingTables(false);
        }
      };

      fetchTables();
    }
  }, [showTableSelector, showFilterDropdown, activeConnection, tableList.length, showNotification]);

  // Fetch analytics dashboard data
  const { data: dashboardData, isLoading: isDashboardLoading } = useAnalyticsDashboard(
    activeConnection?.id,
    { days: timeframe, enabled: !!activeConnection?.id }
  );

  // Fetch high impact objects
  const { data: highImpactData, isLoading: isHighImpactLoading } = useHighImpactObjects(
    activeConnection?.id,
    { limit: 5, enabled: !!activeConnection?.id }
  );

  // Fetch quality score metrics
  const { data: qualityScoreData, isLoading: isQualityScoreLoading } = useHistoricalMetrics(
    activeConnection?.id,
    {
      metricName: 'quality_score',
      days: timeframe,
      enabled: !!activeConnection?.id
    }
  );

  // Handle timeframe change
  const handleTimeframeChange = (days) => {
    setTimeframe(days);
  };

  // Filter tables based on search term
  const filteredTableList = tableSearchTerm
    ? tableList.filter(table =>
        (table.name || table.table_name || '').toLowerCase().includes(tableSearchTerm.toLowerCase())
      )
    : tableList;

  // Extract unique table names from dashboard data
  const availableTables = React.useMemo(() => {
    if (!dashboardData || !dashboardData.row_count_trends) return [];

    // Get unique table names
    const tableSet = new Set();
    dashboardData.row_count_trends.forEach(item => {
      if (item.table_name) {
        tableSet.add(item.table_name);
      }
    });

    return Array.from(tableSet).sort();
  }, [dashboardData]);

  // Final filtered tables list (combine from both sources)
  const filteredTables = tableSearchTerm
    ? availableTables.filter(tableName =>
        tableName.toLowerCase().includes(tableSearchTerm.toLowerCase())
      )
    : availableTables;

  // Table name accessor function to handle different table object formats
  const getTableName = (table) => {
    return table.name || table.table_name || table.table_id || '';
  };

  // Handle table selection for filtering
  const handleTableSelection = (tableName) => {
    setSelectedTables(prev => {
      // If already selected, remove it
      if (prev.includes(tableName)) {
        return prev.filter(t => t !== tableName);
      }
      // Otherwise add it
      return [...prev, tableName];
    });
  };

  // Toggle filtering mode
  const toggleFiltering = () => {
    if (isFiltering) {
      // Turn off filtering
      setIsFiltering(false);
      setSelectedTables([]);
      setShowFilterDropdown(false);
    } else {
      // Turn on filtering
      setIsFiltering(true);
      setShowFilterDropdown(true);
    }
  };

  // Handle closing the dropdown
  const handleCloseDropdown = () => {
    setShowFilterDropdown(false);
  };

  if (!activeConnection) {
    return (
      <div className="rounded-lg bg-white shadow p-6 text-center">
        <ChartBarIcon className="h-12 w-12 text-secondary-400 mx-auto mb-4" />
        <h3 className="text-lg font-medium text-secondary-900">No Connection Selected</h3>
        <p className="mt-2 text-secondary-500">
          Please select a database connection to view analytics.
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header with Navigation Tabs */}
      <div className="bg-white rounded-lg shadow">
        <div className="px-4 py-5 sm:px-6">
          <h1 className="text-2xl font-bold text-secondary-900">Analytics Dashboard</h1>
          <p className="mt-1 text-secondary-500">
            Data quality insights for {activeConnection.name}
          </p>
        </div>

        {/* Navigation Tabs */}
        <div className="border-t border-secondary-200 px-4 py-3 sm:px-6">
          <div className="flex space-x-4 overflow-x-auto pb-1">
            <Link
              to="/analytics"
              className="px-3 py-2 text-sm font-medium rounded-md bg-primary-100 text-primary-700 whitespace-nowrap"
              aria-current="page"
            >
              <span className="flex items-center">
                <ChartBarIcon className="h-5 w-5 mr-1" />
                Overview
              </span>
            </Link>

            <div className="relative">
              <button
                className="px-3 py-2 text-sm font-medium rounded-md text-secondary-600 hover:text-secondary-800 hover:bg-secondary-50 whitespace-nowrap flex items-center"
                onClick={() => setShowTableSelector(!showTableSelector)}
              >
                <TableCellsIcon className="h-5 w-5 mr-1" />
                Table Analytics
                <ChevronDownIcon className="h-4 w-4 ml-1" />
              </button>

              {/* Table selector dropdown */}
              <Transition
                show={showTableSelector}
                as={Fragment}
                enter="transition ease-out duration-100"
                enterFrom="transform opacity-0 scale-95"
                enterTo="transform opacity-100 scale-100"
                leave="transition ease-in duration-75"
                leaveFrom="transform opacity-100 scale-100"
                leaveTo="transform opacity-0 scale-95"
              >
                <div className="absolute left-0 mt-2 w-72 rounded-md shadow-lg bg-white ring-1 ring-black ring-opacity-5 focus:outline-none z-10">
                  <div className="p-3 border-b border-secondary-200">
                    <div className="flex justify-between items-center mb-2">
                      <h3 className="text-sm font-medium text-secondary-900">Select Table</h3>
                      <button
                        onClick={() => setShowTableSelector(false)}
                        className="text-secondary-400 hover:text-secondary-500"
                      >
                        <XMarkIcon className="h-5 w-5" />
                      </button>
                    </div>
                    <div className="relative">
                      <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                        <MagnifyingGlassIcon className="h-4 w-4 text-secondary-400" />
                      </div>
                      <input
                        type="text"
                        className="block w-full pl-10 pr-3 py-2 border border-secondary-300 rounded-md leading-5 bg-white placeholder-secondary-500 focus:outline-none focus:placeholder-secondary-400 focus:ring-1 focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                        placeholder="Search tables..."
                        value={tableSearchTerm}
                        onChange={(e) => setTableSearchTerm(e.target.value)}
                      />
                    </div>
                  </div>
                  <div className="max-h-60 overflow-y-auto py-1">
                    {isLoadingTables ? (
                      <div className="flex justify-center py-4">
                        <LoadingSpinner size="sm" />
                      </div>
                    ) : filteredTableList.length === 0 ? (
                      <div className="px-4 py-2 text-sm text-secondary-500 text-center">
                        {tableSearchTerm ? 'No matching tables found' : 'No tables available'}
                      </div>
                    ) : (
                      filteredTableList.map((table, index) => (
                        <Link
                          key={index}
                          to={`/analytics/table/${activeConnection.id}/${getTableName(table)}`}
                          className="block px-4 py-2 text-sm text-secondary-700 hover:bg-secondary-100 hover:text-secondary-900"
                          onClick={() => setShowTableSelector(false)}
                        >
                          {getTableName(table)}
                        </Link>
                      ))
                    )}
                  </div>
                </div>
              </Transition>
            </div>

            <Link
              to={`/analytics/schema-changes/${activeConnection.id}`}
              className="px-3 py-2 text-sm font-medium rounded-md text-secondary-600 hover:text-secondary-800 hover:bg-secondary-50 whitespace-nowrap"
            >
              <span className="flex items-center">
                <DocumentChartBarIcon className="h-5 w-5 mr-1" />
                Schema Changes
              </span>
            </Link>

            <Link
              to={`/analytics/business-impact/${activeConnection.id}`}
              className="px-3 py-2 text-sm font-medium rounded-md text-secondary-600 hover:text-secondary-800 hover:bg-secondary-50 whitespace-nowrap"
            >
              <span className="flex items-center">
                <PresentationChartLineIcon className="h-5 w-5 mr-1" />
                Business Impact
              </span>
            </Link>
          </div>
        </div>
      </div>

      {/* Dashboard Controls - Time & Table Filters */}
      <div className="flex justify-between items-center bg-white rounded-lg shadow p-3">
        {/* Table Filter */}
        <div className="relative">
          <div className="flex items-center space-x-2">
            {/* Table filter toggle button */}
            <button
              className={`flex items-center px-3 py-2 border rounded-md text-sm ${
                isFiltering
                  ? 'bg-primary-50 border-primary-300 text-primary-700'
                  : 'border-secondary-300 bg-white text-secondary-700 hover:bg-secondary-50'
              }`}
              onClick={toggleFiltering}
            >
              <FunnelIcon className="h-4 w-4 mr-2" />
              {isFiltering
                ? `Filtering: ${selectedTables.length} Tables`
                : 'Filter Tables'}
            </button>

            {/* Clear selection button (only shown when filtering) */}
            {isFiltering && selectedTables.length > 0 && (
              <button
                className="px-2 py-1 text-xs text-secondary-600 hover:text-secondary-800 hover:underline"
                onClick={() => setSelectedTables([])}
              >
                Clear selection
              </button>
            )}
          </div>

          {/* Table Selection Dropdown */}
          {showFilterDropdown && (
            <div className="absolute left-0 mt-2 w-64 bg-white border border-secondary-200 rounded-md shadow-lg z-10">
              <div className="p-3 border-b border-secondary-200 flex justify-between items-center">
                <h3 className="text-sm font-medium text-secondary-900">Select Tables</h3>
                <button
                  onClick={handleCloseDropdown}
                  className="text-secondary-400 hover:text-secondary-500"
                >
                  <XMarkIcon className="h-5 w-5" />
                </button>
              </div>
              <div className="p-2 border-b border-secondary-200">
                <div className="relative">
                  <input
                    type="text"
                    className="w-full px-3 py-2 text-sm border border-secondary-300 rounded-md"
                    placeholder="Search tables..."
                    value={tableSearchTerm}
                    onChange={(e) => setTableSearchTerm(e.target.value)}
                  />
                </div>
              </div>
              <div className="max-h-60 overflow-y-auto py-1">
                {filteredTables.length === 0 ? (
                  <div className="px-3 py-2 text-sm text-secondary-500">No tables available</div>
                ) : (
                  filteredTables.map((tableName, index) => (
                    <button
                      key={index}
                      className={`w-full text-left px-3 py-2 text-sm ${
                        selectedTables.includes(tableName) 
                          ? 'bg-primary-50 text-primary-700' 
                          : 'text-secondary-700 hover:bg-secondary-50'
                      }`}
                      onClick={() => handleTableSelection(tableName)}
                    >
                      {tableName}
                      {selectedTables.includes(tableName) && (
                        <span className="ml-2">✓</span>
                      )}
                    </button>
                  ))
                )}
              </div>
              <div className="p-2 border-t border-secondary-200 flex justify-end">
                <button
                  className="px-3 py-1.5 text-sm font-medium rounded-md bg-primary-600 text-white hover:bg-primary-700"
                  onClick={handleCloseDropdown}
                >
                  Apply Filters
                </button>
              </div>
            </div>
          )}
        </div>

        {/* Time range selector */}
        <div className="flex items-center space-x-2 bg-white rounded-md p-1">
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

      {/* Historical Metrics Dashboard */}
      <HistoricalMetricsDashboard
        data={dashboardData}
        isLoading={isDashboardLoading}
        timeframe={timeframe}
        selectedTables={selectedTables}
        isFiltering={isFiltering}
      />

      {/* High Impact Objects */}
      <div>
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-lg font-medium text-secondary-900">High Impact Objects</h2>
          <Link
            to={`/analytics/schema-changes/${activeConnection.id}`}
            className="text-sm font-medium text-primary-600 hover:text-primary-500 flex items-center"
          >
            View schema changes <ArrowRightIcon className="ml-1 h-4 w-4" />
          </Link>
        </div>

        {isHighImpactLoading ? (
          <div className="bg-white rounded-lg shadow p-8 flex justify-center">
            <LoadingSpinner size="lg" />
          </div>
        ) : !highImpactData?.objects || highImpactData.objects.length === 0 ? (
          <div className="bg-white rounded-lg shadow p-6 text-center">
            <TableCellsIcon className="h-12 w-12 text-secondary-400 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-secondary-900">No High Impact Objects</h3>
            <p className="mt-2 text-secondary-500">
              No objects with significant changes or impact have been identified.
            </p>
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div className="flex items-center p-4 border-b border-secondary-200 bg-secondary-50">
              <div className="w-1/4 font-medium text-secondary-700">Object Name</div>
              <div className="w-1/4 font-medium text-secondary-700">Type</div>
              <div className="w-1/4 font-medium text-secondary-700">Impact Score</div>
              <div className="w-1/4 font-medium text-secondary-700">Last Change</div>
            </div>

            <div className="divide-y divide-secondary-200">
              {highImpactData.objects.map((object, index) => (
                <div key={index} className="flex items-center p-4 hover:bg-secondary-50">
                  <div className="w-1/4 font-medium text-primary-600">
                    <Link to={`/analytics/table/${activeConnection.id}/${getTableName(object)}`}>
                      {getTableName(object)}
                    </Link>
                  </div>
                  <div className="w-1/4 text-secondary-500 capitalize">
                    {object.object_type || 'table'}
                  </div>
                  <div className="w-1/4">
                    <div className="flex items-center">
                      <div className="h-2 flex-1 bg-secondary-200 rounded-full max-w-xs">
                        <div
                          className="h-2 bg-primary-600 rounded-full"
                          style={{ width: `${Math.min(100, object.impact_score || 75)}%` }}
                        ></div>
                      </div>
                      <span className="ml-2 text-sm text-secondary-700">
                        {object.impact_score || 75}%
                      </span>
                    </div>
                  </div>
                  <div className="w-1/4 text-secondary-500 flex items-center">
                    <ClockIcon className="h-4 w-4 mr-1 text-secondary-400" />
                    {formatDate(object.last_changed_at || new Date(), false)}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Anomaly Detection Section */}
      <div>
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-lg font-medium text-secondary-900">Data Quality Anomalies</h2>
          <button
            onClick={() => console.log('Refresh anomalies')}
            className="text-sm font-medium text-primary-600 hover:text-primary-500 flex items-center"
          >
            <ArrowPathIcon className="mr-1 h-4 w-4" />
            Refresh anomalies
          </button>
        </div>

        {/* Generate sample anomalies based on row_count_trends data */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {isDashboardLoading ? (
            <div className="col-span-2 bg-white rounded-lg shadow p-8 flex justify-center">
              <LoadingSpinner size="lg" />
            </div>
          ) : (dashboardData?.row_count_trends && dashboardData.row_count_trends.length > 0) ? (
            <>
              <AnomalyCard
                title="Row Count Variation Detected"
                description={`Table ${dashboardData.row_count_trends[0].table_name} shows unusual row count patterns compared to historical trends.`}
                severity="medium"
                detectedAt={new Date().toISOString()}
                tableName={dashboardData.row_count_trends[0].table_name}
                actionLink={`/analytics/table/${activeConnection.id}/${dashboardData.row_count_trends[0].table_name}`}
                actionText="Investigate Table"
              />

              <AnomalyCard
                title="Null Percentage Increase"
                description="Detected an increase in NULL values in customer_id column for orders table."
                severity="high"
                detectedAt={new Date().toISOString()}
                tableName="orders"
                columnName="customer_id"
                actionLink={`/analytics/table/${activeConnection.id}/orders`}
                actionText="View Table Details"
              />
            </>
          ) : (
            <div className="col-span-2 bg-white rounded-lg shadow p-6 text-center">
              <ExclamationCircleIcon className="h-12 w-12 text-secondary-400 mx-auto mb-4" />
              <h3 className="text-lg font-medium text-secondary-900">No Anomalies Detected</h3>
              <p className="mt-2 text-secondary-500">
                All monitored metrics are within expected ranges.
              </p>
            </div>
          )}
        </div>
      </div>

      {/* Actionable Insights */}
      <div>
        <h2 className="text-lg font-medium text-secondary-900 mb-4">Actionable Insights</h2>

        {isDashboardLoading ? (
          <div className="bg-white rounded-lg shadow p-8 flex justify-center">
            <LoadingSpinner size="lg" />
          </div>
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {/* Generate sample insights based on available data */}
            {dashboardData?.row_count_trends && dashboardData.row_count_trends.length > 0 ? (
              <>
                <InsightCard
                  title="Set up validation rules"
                  description={`Table ${dashboardData.row_count_trends[0].table_name} has no validation rules defined. Add validation rules to ensure data quality.`}
                  type="suggestion"
                  impact="medium"
                  tableName={dashboardData.row_count_trends[0].table_name}
                  actionText="Create Rules"
                  onAction={() => {
                    window.location.href = `/validations?table=${dashboardData.row_count_trends[0].table_name}`;
                  }}
                />

                <InsightCard
                  title="Optimize database schema"
                  description="Consider adding an index to improve query performance on the most frequently accessed tables."
                  type="optimization"
                  impact="high"
                  tableName="orders"
                  actionText="View Recommendations"
                  onAction={() => {
                    console.log('Opening schema optimization recommendations');
                  }}
                />
              </>
            ) : (
              <div className="col-span-2 bg-white rounded-lg shadow p-6 text-center">
                <SparklesIcon className="h-12 w-12 text-secondary-400 mx-auto mb-4" />
                <h3 className="text-lg font-medium text-secondary-900">No Insights Available</h3>
                <p className="mt-2 text-secondary-500">
                  We don't have any actionable insights at this time. Check back later.
                </p>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Quick Access Links */}
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-lg font-medium text-secondary-900 mb-4">Quick Access</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="border border-secondary-200 rounded-lg p-4 hover:bg-secondary-50">
            <Link to={`/analytics/schema-changes/${activeConnection.id}`} className="flex flex-col items-center">
              <DocumentChartBarIcon className="h-8 w-8 text-primary-500" />
              <span className="mt-2 font-medium text-secondary-900">Schema Changes</span>
              <p className="mt-1 text-xs text-center text-secondary-500">
                Track database schema changes and their impact
              </p>
            </Link>
          </div>

          <div className="border border-secondary-200 rounded-lg p-4 hover:bg-secondary-50">
            <Link to={`/analytics/business-impact/${activeConnection.id}`} className="flex flex-col items-center">
              <PresentationChartLineIcon className="h-8 w-8 text-primary-500" />
              <span className="mt-2 font-medium text-secondary-900">Business Impact</span>
              <p className="mt-1 text-xs text-center text-secondary-500">
                View how data quality affects business outcomes
              </p>
            </Link>
          </div>

          <div className="border border-secondary-200 rounded-lg p-4 hover:bg-secondary-50 relative">
            <button
              onClick={() => setShowTableSelector(true)}
              className="flex flex-col items-center w-full"
            >
              <TableCellsIcon className="h-8 w-8 text-primary-500" />
              <span className="mt-2 font-medium text-secondary-900">Table Analytics</span>
              <p className="mt-1 text-xs text-center text-secondary-500">
                Analyze metrics for individual tables
              </p>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AnalyticsPage;