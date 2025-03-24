import React, { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useHistoricalMetrics } from '../../hooks/useAnalytics';
import { schemaAPI } from '../../api/enhancedApiService';
import {
  ArrowLeftIcon,
  TableCellsIcon,
  PlusCircleIcon,
  MinusCircleIcon,
  PencilSquareIcon,
  CalendarIcon,
  UserIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import TrendChart from '../../components/analytics/TrendChart';
import { formatDate } from '../../utils/formatting';

const SchemaChangesPage = () => {
  const { connectionId } = useParams();
  const { activeConnection, getConnection } = useConnection();
  const { updateBreadcrumbs } = useUI();
  const [timeframe, setTimeframe] = useState(30); // Default to 30 days
  const [changes, setChanges] = useState([]);
  const [isLoading, setIsLoading] = useState(false);

  // Fetch schema change metrics
  const {
    data: schemaChangeData,
    isLoading: isSchemaChangeDataLoading
  } = useHistoricalMetrics(
    connectionId,
    {
      metricName: 'schema_changes',
      days: timeframe,
      enabled: !!connectionId
    }
  );

  // Load connection and schema changes
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

    // Fetch schema changes
    const fetchSchemaChanges = async () => {
      setIsLoading(true);
      try {
        const since = new Date();
        since.setDate(since.getDate() - timeframe);

        const response = await schemaAPI.getChanges(
          connectionId,
          since.toISOString()
        );

        setChanges(response.changes || []);
      } catch (error) {
        console.error('Error fetching schema changes:', error);
      } finally {
        setIsLoading(false);
      }
    };

    fetchConnectionIfNeeded();
    if (connectionId) {
      fetchSchemaChanges();
    }
  }, [connectionId, timeframe, activeConnection, getConnection]);

  // Update breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Analytics', href: '/analytics' },
      { name: 'Schema Changes', href: `/analytics/schema-changes/${connectionId}` },
    ]);
  }, [updateBreadcrumbs, connectionId]);

  // Handle timeframe change
  const handleTimeframeChange = (days) => {
    setTimeframe(days);
  };

  // Get icon for change type
  const getChangeTypeIcon = (changeType) => {
    switch (changeType.toLowerCase()) {
      case 'add':
      case 'added':
        return <PlusCircleIcon className="h-5 w-5 text-accent-500" />;
      case 'drop':
      case 'removed':
        return <MinusCircleIcon className="h-5 w-5 text-danger-500" />;
      case 'alter':
      case 'modified':
        return <PencilSquareIcon className="h-5 w-5 text-warning-500" />;
      default:
        return <TableCellsIcon className="h-5 w-5 text-primary-500" />;
    }
  };

  // Get color for change type
  const getChangeTypeColor = (changeType) => {
    switch (changeType.toLowerCase()) {
      case 'add':
      case 'added':
        return 'bg-accent-100 text-accent-800';
      case 'drop':
      case 'removed':
        return 'bg-danger-100 text-danger-800';
      case 'alter':
      case 'modified':
        return 'bg-warning-100 text-warning-800';
      default:
        return 'bg-secondary-100 text-secondary-800';
    }
  };

  if (isLoading && !activeConnection) {
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
            Schema Change Analysis
          </h1>
        </div>

        <div className="flex justify-between items-center">
          <p className="text-secondary-500">
            View schema changes and their impact for {activeConnection?.name || 'this connection'}
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

      {/* Schema Change Trend Chart */}
      <div>
        <h2 className="text-lg font-medium text-secondary-900 mb-4">Schema Change Frequency</h2>
        <TrendChart
          data={schemaChangeData?.metrics || []}
          yKey="metric_value"
          type="area"
          color="#6366f1"
          loading={isSchemaChangeDataLoading}
          height={250}
        />
      </div>

      {/* Recent Schema Changes */}
      <div>
        <h2 className="text-lg font-medium text-secondary-900 mb-4">Recent Changes</h2>

        {isLoading ? (
          <div className="bg-white rounded-lg shadow p-8 flex justify-center">
            <LoadingSpinner size="lg" />
          </div>
        ) : changes.length === 0 ? (
          <div className="bg-white rounded-lg shadow p-6 text-center">
            <TableCellsIcon className="h-12 w-12 text-secondary-400 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-secondary-900">No Schema Changes</h3>
            <p className="mt-2 text-secondary-500">
              No schema changes have been detected in the last {timeframe} days.
            </p>
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div className="space-y-4 p-4">
              {changes.map((change, index) => (
                <div key={index} className="border border-secondary-200 rounded-lg p-4">
                  <div className="flex justify-between items-start">
                    <div className="flex items-start">
                      <div className="mt-0.5 mr-3">
                        {getChangeTypeIcon(change.change_type)}
                      </div>
                      <div>
                        <div className="flex items-center">
                          <h3 className="text-sm font-medium text-secondary-900 mr-2">
                            {change.object_name}
                          </h3>
                          <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${getChangeTypeColor(change.change_type)}`}>
                            {change.change_type}
                          </span>
                        </div>

                        <p className="mt-1 text-sm text-secondary-700">
                          {change.description ||
                            `${change.change_type} ${change.object_type || 'object'} ${change.object_name}`}
                        </p>

                        <div className="mt-2 flex items-center space-x-4 text-xs text-secondary-500">
                          <div className="flex items-center">
                            <CalendarIcon className="h-4 w-4 mr-1" />
                            {formatDate(change.change_date, true)}
                          </div>

                          {change.changed_by && (
                            <div className="flex items-center">
                              <UserIcon className="h-4 w-4 mr-1" />
                              {change.changed_by}
                            </div>
                          )}
                        </div>
                      </div>
                    </div>

                    {change.table_name && (
                      <Link
                        to={`/explorer/connection/${connectionId}/table/${change.table_name}`}
                        className="text-xs font-medium text-primary-600 hover:text-primary-500"
                      >
                        View Table
                      </Link>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Impact Analysis */}
      <div>
        <h2 className="text-lg font-medium text-secondary-900 mb-4">Impact Analysis</h2>

        <div className="bg-white rounded-lg shadow p-6">
          <p className="text-secondary-700 mb-4">
            These schema changes have potentially affected the following areas:
          </p>

          {isLoading ? (
            <div className="flex justify-center p-4">
              <LoadingSpinner size="md" />
            </div>
          ) : changes.length === 0 ? (
            <p className="text-secondary-500 italic">No impacts to analyze.</p>
          ) : (
            <div className="space-y-4">
              {/* Group impacts by area */}
              <div>
                <h3 className="text-sm font-medium text-secondary-900 mb-2">Validation Rules</h3>
                <div className="pl-4 border-l-2 border-primary-200">
                  {changes.filter(c => c.impacts?.validation_rules).length > 0 ? (
                    changes
                      .filter(c => c.impacts?.validation_rules)
                      .map((change, idx) => (
                        <div key={idx} className="text-sm text-secondary-700 mb-2">
                          {change.impacts.validation_rules} validation rules affected by changes to {change.object_name}
                        </div>
                      ))
                  ) : (
                    <p className="text-sm text-secondary-500 italic">No validation rules affected</p>
                  )}
                </div>
              </div>

              <div>
                <h3 className="text-sm font-medium text-secondary-900 mb-2">Reports and Dashboards</h3>
                <div className="pl-4 border-l-2 border-primary-200">
                  {changes.filter(c => c.impacts?.reports).length > 0 ? (
                    changes
                      .filter(c => c.impacts?.reports)
                      .map((change, idx) => (
                        <div key={idx} className="text-sm text-secondary-700 mb-2">
                          {change.impacts.reports} reports affected by changes to {change.object_name}
                        </div>
                      ))
                  ) : (
                    <p className="text-sm text-secondary-500 italic">No reports affected</p>
                  )}
                </div>
              </div>

              <div>
                <h3 className="text-sm font-medium text-secondary-900 mb-2">Data Pipelines</h3>
                <div className="pl-4 border-l-2 border-primary-200">
                  {changes.filter(c => c.impacts?.pipelines).length > 0 ? (
                    changes
                      .filter(c => c.impacts?.pipelines)
                      .map((change, idx) => (
                        <div key={idx} className="text-sm text-secondary-700 mb-2">
                          {change.impacts.pipelines} pipelines affected by changes to {change.object_name}
                        </div>
                      ))
                  ) : (
                    <p className="text-sm text-secondary-500 italic">No pipelines affected</p>
                  )}
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default SchemaChangesPage;