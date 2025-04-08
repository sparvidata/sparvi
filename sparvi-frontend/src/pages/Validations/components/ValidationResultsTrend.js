import React, { useEffect, useState, useCallback } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { useValidationResults } from '../../../contexts/ValidationResultsContext';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

const ValidationResultsTrend = () => {
  const { isLoading, metrics, trends, selectedTable } = useValidationResults();
  const [chartData, setChartData] = useState([]);

  // Log what we're getting to debug
  console.log("ValidationResultsTrend render with:", {
    metrics,
    isLoading,
    selectedTable,
    trendsLength: trends?.length || 0
  });

  // Use trends if available, or create data from metrics
  useEffect(() => {
    if (trends && trends.length > 0) {
      console.log("Using existing trends data:", trends);
      setChartData(trends);
      return;
    }

    // If we have metrics but no trends, create a single data point
    if (metrics && metrics.counts) {
      console.log("Creating single trend point from metrics:", metrics);

      const today = new Date().toISOString().split('T')[0];
      const dataPoint = {
        date: today,
        total: metrics.total || 0,
        passed: metrics.counts.passed || 0,
        failed: metrics.counts.failed || 0,
        error: metrics.counts.error || 0,
        health_score: Math.round(metrics.health_score) || 0
      };

      setChartData([dataPoint]);
      return;
    }

    // No data available
    setChartData([]);
  }, [trends, metrics]);

  if (isLoading) {
    return (
      <div className="bg-white p-4 rounded-lg shadow flex justify-center items-center h-64">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  // Show a more useful empty state
  if (!chartData || chartData.length === 0) {
    return (
      <div className="bg-white p-4 rounded-lg shadow">
        <div className="flex flex-col justify-center items-center h-40">
          <h3 className="text-lg font-medium text-secondary-900 mb-2">
            Validation Health Trend
          </h3>
          <p className="text-secondary-500 text-center">
            No historical data available yet for {selectedTable || 'this table'}.
            <br />
            Run validations multiple times to see trends.
          </p>
        </div>
      </div>
    );
  }

  // Format date for display
  const formatXAxis = (dateStr) => {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
  };

  // Custom tooltip
  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white p-3 border border-secondary-200 shadow-md rounded-md">
          <p className="font-medium">{formatXAxis(label)}</p>
          <p className="text-accent-600">Passed: {data.passed}</p>
          <p className="text-danger-600">Failed: {data.failed}</p>
          {data.error > 0 && <p className="text-warning-600">Errors: {data.error}</p>}
          <p className="font-semibold mt-1">Health Score: {data.health_score}%</p>
          <p className="text-secondary-500 text-xs">Total: {data.total} validations</p>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="bg-white p-4 rounded-lg shadow">
      <h3 className="text-lg font-medium text-secondary-900 mb-4">
        Validation Health Trend
        <span className="text-sm font-normal text-secondary-500 ml-2">
          {selectedTable}
        </span>
      </h3>

      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={chartData}
            margin={{ top: 5, right: 20, left: 0, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="date"
              tickFormatter={formatXAxis}
              tick={{ fontSize: 12 }}
            />
            <YAxis
              yAxisId="left"
              domain={[0, 100]}
              tickFormatter={(value) => `${value}%`}
              tick={{ fontSize: 12 }}
            />
            <YAxis
              yAxisId="right"
              orientation="right"
              tick={{ fontSize: 12 }}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="health_score"
              name="Health Score"
              stroke="#6366f1"
              strokeWidth={2}
              activeDot={{ r: 8 }}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="passed"
              name="Passed"
              stroke="#10b981"
              strokeWidth={2}
              dot={{ r: 4 }}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="failed"
              name="Failed"
              stroke="#ef4444"
              strokeWidth={2}
              dot={{ r: 4 }}
            />
            {chartData.some(d => d.error > 0) && (
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="error"
                name="Errors"
                stroke="#f59e0b"
                strokeWidth={2}
                dot={{ r: 4 }}
              />
            )}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Run count */}
      <div className="mt-2 text-xs text-secondary-500 text-center">
        Showing data for {chartData.length} day{chartData.length !== 1 ? 's' : ''}
      </div>
    </div>
  );
};

export default ValidationResultsTrend;