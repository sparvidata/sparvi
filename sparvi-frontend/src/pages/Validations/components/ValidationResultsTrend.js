import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { validationsAPI } from '../../../api/enhancedApiService';

const ValidationResultsTrend = ({
  connectionId,
  tableName,
  days = 30
}) => {
  const [trendData, setTrendData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [message, setMessage] = useState(null);
  const [initialLoad, setInitialLoad] = useState(true);

  useEffect(() => {
    // Only fetch if we have both connectionId and tableName
    if (!connectionId || !tableName) {
      setIsLoading(false);
      return;
    }

    const fetchTrendData = async () => {
      try {
        setIsLoading(true);
        setError(null);
        setMessage(null);

        const response = await validationsAPI.getValidationTrends(connectionId, tableName, { days });

        // Check if we got a response with trends data
        if (response && Array.isArray(response.trends)) {
          setTrendData(response.trends);

          // If there's a message from the API, display it
          if (response.message) {
            setMessage(response.message);
          }
        } else {
          // Handle empty or invalid response
          setTrendData([]);
          setMessage("No trend data available");
        }
      } catch (err) {
        console.error('Error fetching validation trends:', err);
        setError(err.message || 'Failed to load trend data');
      } finally {
        setIsLoading(false);
        setInitialLoad(false);
      }
    };

    fetchTrendData();
  }, [connectionId, tableName, days]);

  // Show loading state during initial load and when changing tables
  if (isLoading) {
    return (
      <div className="bg-white p-4 rounded-lg shadow">
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-lg font-medium text-secondary-900">
            Validation Health Trend
            {tableName && (
              <span className="text-sm font-normal text-secondary-500 ml-2">
                {tableName}
              </span>
            )}
          </h3>
        </div>
        <div className="flex flex-col justify-center items-center h-64">
          <LoadingSpinner size="lg" />
          <p className="mt-2 text-secondary-500">
            Loading validation trends for {tableName || 'this table'}...
          </p>
        </div>
      </div>
    );
  }

  // Show error state
  if (error) {
    return (
      <div className="bg-white p-4 rounded-lg shadow">
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-lg font-medium text-secondary-900">
            Validation Health Trend
          </h3>
        </div>
        <div className="flex flex-col justify-center items-center h-40">
          <p className="text-danger-500 text-center">
            {error}
          </p>
          <button
            onClick={() => window.location.reload()}
            className="mt-4 px-4 py-2 bg-primary-100 text-primary-700 rounded-md"
          >
            Try Again
          </button>
        </div>
      </div>
    );
  }

  // Show a more useful empty state
  if (!trendData || trendData.length === 0) {
    return (
      <div className="bg-white p-4 rounded-lg shadow">
        <div className="flex flex-col justify-center items-center h-40">
          <h3 className="text-lg font-medium text-secondary-900 mb-2">
            Validation Health Trend
          </h3>
          <p className="text-secondary-500 text-center">
            {message || `No historical data available yet for ${tableName || 'this table'}.`}
            <br />
            Run validations multiple times to see trends over time.
          </p>
        </div>
      </div>
    );
  }

  // Filter out days with zero validations to avoid empty dots on the chart
  const filteredData = trendData.filter(day => day.total_validations > 0);

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
          {(data.not_run > 0) && <p className="text-secondary-600">Not Run: {data.not_run}</p>}
          <p className="font-semibold mt-1">Health Score: {data.health_score}%</p>
          <p className="text-secondary-500 text-xs">Total: {data.total_validations} validations</p>
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
          {tableName}
        </span>
      </h3>

      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={filteredData}
            margin={{ top: 5, right: 20, left: 0, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="day"
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
              connectNulls={true}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="passed"
              name="Passed"
              stroke="#10b981"
              strokeWidth={2}
              dot={{ r: 4 }}
              connectNulls={true}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="failed"
              name="Failed"
              stroke="#ef4444"
              strokeWidth={2}
              dot={{ r: 4 }}
              connectNulls={true}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="not_run"
              name="Not Run"
              stroke="#94a3b8"
              strokeWidth={2}
              dot={{ r: 4 }}
              connectNulls={true}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Display count of days with data */}
      <div className="mt-2 text-xs text-secondary-500 text-center">
        {filteredData.length > 0
          ? `Showing data for ${filteredData.length} day${filteredData.length > 1 ? 's' : ''} with validation activity`
          : `No days with validation activity`}
      </div>
    </div>
  );
};

export default ValidationResultsTrend;