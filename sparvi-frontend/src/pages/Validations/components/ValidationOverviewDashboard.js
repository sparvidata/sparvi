import React, { useState, useEffect } from 'react';
import {
  ClipboardDocumentCheckIcon,
  ExclamationTriangleIcon,
  CheckCircleIcon,
  XCircleIcon
} from '@heroicons/react/24/outline';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { validationsAPI } from '../../../api/enhancedApiService';

const ValidationOverviewDashboard = ({ connectionId, onTableSelect }) => {
  const [summaryData, setSummaryData] = useState(null);
  const [tableHealthData, setTableHealthData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!connectionId) {
      setLoading(false);
      return;
    }

    const fetchOverviewData = async () => {
      try {
        setLoading(true);
        setError(null);

        console.log('Fetching validation overview for connection:', connectionId);

        // Get validation summary
        const summary = await validationsAPI.getSummary(connectionId, { forceFresh: false });
        console.log('Validation summary:', summary);

        setSummaryData(summary);

        // Process table-level health data
        if (summary.validations_by_table) {
          const tableHealth = Object.entries(summary.validations_by_table).map(([tableName, tableData]) => {
            const total = tableData.total || 0;
            const passing = tableData.passing || 0;
            const failing = tableData.failing || 0;
            const healthScore = total > 0 ? Math.round((passing / total) * 100) : 0;

            return {
              table_name: tableName,
              total_validations: total,
              passing,
              failing,
              health_score: healthScore
            };
          }).sort((a, b) => b.total_validations - a.total_validations); // Sort by total validations

          setTableHealthData(tableHealth);
        }

      } catch (err) {
        console.error('Error fetching validation overview:', err);
        setError(err.message || 'Failed to load validation overview');
      } finally {
        setLoading(false);
      }
    };

    fetchOverviewData();
  }, [connectionId]);

  // Calculate overall health score
  const calculateOverallHealth = () => {
    if (!summaryData || summaryData.total_count === 0) return 0;
    return Math.round((summaryData.passing_count / summaryData.total_count) * 100);
  };

  // Get health status info
  const getHealthStatus = (score) => {
    if (score >= 80) {
      return {
        status: 'Excellent',
        color: 'accent',
        bgColor: 'bg-accent-50',
        textColor: 'text-accent-600',
        borderColor: 'border-accent-200'
      };
    } else if (score >= 60) {
      return {
        status: 'Good',
        color: 'warning',
        bgColor: 'bg-warning-50',
        textColor: 'text-warning-600',
        borderColor: 'border-warning-200'
      };
    } else {
      return {
        status: 'Needs Attention',
        color: 'danger',
        bgColor: 'bg-danger-50',
        textColor: 'text-danger-600',
        borderColor: 'border-danger-200'
      };
    }
  };

  // Custom tooltip for table health chart
  const TableHealthTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white p-3 border border-secondary-200 shadow-lg rounded-md">
          <p className="font-medium text-secondary-900">{label}</p>
          <p className="text-sm text-secondary-600">Health Score: {data.health_score}%</p>
          <p className="text-sm text-accent-600">Passing: {data.passing}</p>
          <p className="text-sm text-danger-600">Failing: {data.failing}</p>
          <p className="text-sm text-secondary-500">Total: {data.total_validations}</p>
        </div>
      );
    }
    return null;
  };

  if (loading) {
    return (
      <div className="flex flex-col justify-center items-center py-12">
        <LoadingSpinner size="lg" />
        <p className="mt-4 text-secondary-500">Loading validation overview...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-center py-12">
        <ExclamationTriangleIcon className="mx-auto h-12 w-12 text-danger-400 mb-4" />
        <p className="text-danger-600 mb-4">{error}</p>
        <button
          onClick={() => window.location.reload()}
          className="px-4 py-2 bg-primary-100 text-primary-700 rounded-md hover:bg-primary-200"
        >
          Try Again
        </button>
      </div>
    );
  }

  if (!summaryData || summaryData.total_count === 0) {
    return (
      <div className="text-center py-12">
        <ClipboardDocumentCheckIcon className="mx-auto h-12 w-12 text-secondary-400 mb-4" />
        <h3 className="text-lg font-medium text-secondary-900 mb-2">No Validation Rules Yet</h3>
        <p className="text-secondary-500 mb-6">
          Get started by selecting a table and creating your first validation rules.
        </p>
      </div>
    );
  }

  const overallHealth = calculateOverallHealth();
  const healthStatus = getHealthStatus(overallHealth);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h2 className="text-xl font-semibold text-secondary-900 mb-2">Validation Overview</h2>
        <p className="text-secondary-600">
          Overall health across {summaryData.tables_with_validations} tables with validation rules
        </p>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {/* Overall Health Score */}
        <div className={`p-4 rounded-lg border ${healthStatus.bgColor} ${healthStatus.borderColor}`}>
          <div className="flex items-center">
            <CheckCircleIcon className={`h-6 w-6 ${healthStatus.textColor} mr-3`} />
            <div>
              <h4 className="text-sm font-medium text-secondary-900">Overall Health</h4>
              <div className={`text-2xl font-bold ${healthStatus.textColor}`}>
                {overallHealth}%
              </div>
              <div className={`text-xs ${healthStatus.textColor}`}>
                {healthStatus.status}
              </div>
            </div>
          </div>
        </div>

        {/* Total Validations */}
        <div className="bg-white p-4 rounded-lg border border-secondary-200">
          <div className="flex items-center">
            <ClipboardDocumentCheckIcon className="h-6 w-6 text-primary-600 mr-3" />
            <div>
              <h4 className="text-sm font-medium text-secondary-900">Total Validations</h4>
              <div className="text-2xl font-bold text-primary-600">
                {summaryData.total_count.toLocaleString()}
              </div>
              <div className="text-xs text-secondary-500">
                across {summaryData.tables_with_validations} tables
              </div>
            </div>
          </div>
        </div>

        {/* Passing Validations */}
        <div className="bg-accent-50 p-4 rounded-lg border border-accent-200">
          <div className="flex items-center">
            <CheckCircleIcon className="h-6 w-6 text-accent-600 mr-3" />
            <div>
              <h4 className="text-sm font-medium text-secondary-900">Passing</h4>
              <div className="text-2xl font-bold text-accent-600">
                {summaryData.passing_count.toLocaleString()}
              </div>
              <div className="text-xs text-accent-600">
                {summaryData.total_count > 0
                  ? Math.round((summaryData.passing_count / summaryData.total_count) * 100)
                  : 0}% of total
              </div>
            </div>
          </div>
        </div>

        {/* Failing Validations */}
        <div className="bg-danger-50 p-4 rounded-lg border border-danger-200">
          <div className="flex items-center">
            <XCircleIcon className="h-6 w-6 text-danger-600 mr-3" />
            <div>
              <h4 className="text-sm font-medium text-secondary-900">Failing</h4>
              <div className="text-2xl font-bold text-danger-600">
                {summaryData.failing_count.toLocaleString()}
              </div>
              <div className="text-xs text-danger-600">
                {summaryData.total_count > 0
                  ? Math.round((summaryData.failing_count / summaryData.total_count) * 100)
                  : 0}% of total
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Table Health Overview */}
      {tableHealthData.length > 0 && (
        <div className="bg-white p-6 rounded-lg shadow border border-secondary-200">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-medium text-secondary-900">Table Health Scores</h3>
            <p className="text-sm text-secondary-500">Click a table to view details</p>
          </div>

          {/* Chart */}
          <div className="h-64 mb-4">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={tableHealthData.slice(0, 10)} margin={{ top: 5, right: 20, left: 5, bottom: 60 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                <XAxis
                  dataKey="table_name"
                  tick={{ fontSize: 12, fill: '#64748b' }}
                  angle={-45}
                  textAnchor="end"
                  height={60}
                />
                <YAxis
                  domain={[0, 100]}
                  tickFormatter={(value) => `${value}%`}
                  tick={{ fontSize: 12, fill: '#64748b' }}
                />
                <Tooltip content={<TableHealthTooltip />} />
                <Bar
                  dataKey="health_score"
                  fill="#6366f1"
                  radius={[4, 4, 0, 0]}
                  cursor="pointer"
                  onClick={(data) => {
                    if (onTableSelect && data.table_name) {
                      onTableSelect(data.table_name);
                    }
                  }}
                />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Table List */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
            {tableHealthData.slice(0, 9).map((table) => {
              const tableHealthStatus = getHealthStatus(table.health_score);

              return (
                <button
                  key={table.table_name}
                  onClick={() => onTableSelect && onTableSelect(table.table_name)}
                  className={`p-3 rounded-lg border text-left hover:shadow-md transition-all ${tableHealthStatus.bgColor} ${tableHealthStatus.borderColor} hover:border-primary-300`}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex-1 min-w-0">
                      <h4 className="text-sm font-medium text-secondary-900 truncate">
                        {table.table_name}
                      </h4>
                      <p className="text-xs text-secondary-500">
                        {table.total_validations} validations
                      </p>
                    </div>
                    <div className={`text-lg font-bold ${tableHealthStatus.textColor} ml-2`}>
                      {table.health_score}%
                    </div>
                  </div>
                  <div className="mt-2 flex items-center text-xs">
                    <span className="text-accent-600">✓ {table.passing}</span>
                    <span className="text-danger-600 ml-3">✗ {table.failing}</span>
                  </div>
                </button>
              );
            })}

            {tableHealthData.length > 9 && (
              <div className="p-3 rounded-lg border border-secondary-200 bg-secondary-50 flex items-center justify-center">
                <div className="text-center">
                  <p className="text-sm font-medium text-secondary-700">
                    +{tableHealthData.length - 9} more tables
                  </p>
                  <p className="text-xs text-secondary-500">
                    Select from the list to view
                  </p>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default ValidationOverviewDashboard;