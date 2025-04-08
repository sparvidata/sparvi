import React, { useEffect, useState } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { useValidationResults } from '../../../contexts/ValidationResultsContext';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

const ValidationResultsTrend = () => {
  const { 
    isLoading, 
    isLoadingHistory,
    metrics, 
    trends, 
    selectedTable,
    rulesLoaded,
    historyLoaded
  } = useValidationResults();
  
  const [chartData, setChartData] = useState([]);

  // Use trends if available, or create data from metrics
  useEffect(() => {
    if (trends && trends.length > 0) {
      console.log("Using existing trends data:", trends);
      // Make sure we have proper number values in the trends data
      const processedTrends = trends.map(point => ({
        ...point,
        total: Number(point.total || 0),
        passed: Number(point.passed || 0),
        failed: Number(point.failed || 0),
        error: Number(point.error || 0),
        health_score: Number(point.health_score || 0)
      }));
      setChartData(processedTrends);
      return;
    }

    // If we have metrics but no trends, create a single data point
    if (metrics && metrics.counts) {
      console.log("Creating single trend point from metrics:", metrics);

      const today = new Date().toISOString().split('T')[0];
      const dataPoint = {
        date: today,
        total: Number(metrics.total || 0),
        passed: Number(metrics.counts.passed || 0),
        failed: Number(metrics.counts.failed || 0),
        error: Number(metrics.counts.error || 0),
        health_score: Number(Math.round(metrics.health_score) || 0)
      };

      setChartData([dataPoint]);
      return;
    }

    // No data available
    setChartData([]);
  }, [trends, metrics]);

  // Show loading state specifically when we're loading history
  if (isLoading || isLoadingHistory) {
    return (
      <div className="bg-white p-4 rounded-lg shadow">
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-lg font-medium text-secondary-900">
            Validation Health Trend
            {selectedTable && (
              <span className="text-sm font-normal text-secondary-500 ml-2">
                {selectedTable}
              </span>
            )}
          </h3>
        </div>
        <div className="flex justify-center items-center h-64">
          <LoadingSpinner size="lg" />
        </div>
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
            {rulesLoaded && !historyLoaded
              ? `Loading history data for ${selectedTable || 'this table'}...`
              : rulesLoaded
                ? `No historical data available yet for ${selectedTable || 'this table'}.`
                : 'Select a table and run validations to see trends.'}
            <br />
            Run validations multiple times to see trends over time.
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
        {chartData.length > 1
          ? `Showing data for ${chartData.length} days`
          : `Showing data for 1 day`}
      </div>
    </div>
  );
};

export default ValidationResultsTrend;