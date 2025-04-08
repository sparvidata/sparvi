// src/pages/Validations/components/ValidationResultsTrend.js - Fixed dependencies
import React, { useEffect, useState, useCallback } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { useValidationResults } from '../../../contexts/ValidationResultsContext';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

const ValidationResultsTrend = () => {
  const { isLoading, current, history, selectedTable, lastFetched } = useValidationResults();
  const [chartData, setChartData] = useState([]);
  const [processingKey, setProcessingKey] = useState('');

  // Create a single data point from current validation results
  const createSingleDataPoint = useCallback((validationResults) => {
    try {
      const today = new Date().toISOString().split('T')[0];

      // Count results by category
      let passed = 0, failed = 0, error = 0;

      validationResults.forEach(result => {
        if (result.error) {
          error++;
        } else if (result.is_valid === true || result.last_result === true) {
          passed++;
        } else if (result.is_valid === false || result.last_result === false) {
          failed++;
        }
      });

      // Calculate health score
      const total = validationResults.length;
      const validResults = passed + failed;
      const health_score = validResults > 0 ? Math.round((passed / validResults) * 100) : 0;

      // Create data point
      const dataPoint = {
        date: today,
        total,
        passed,
        failed,
        error,
        health_score
      };

      console.log("Created single data point:", dataPoint);
      setChartData([dataPoint]);
    } catch (error) {
      console.error("Error creating data point:", error);
      setChartData([]);
    }
  }, []); // No dependencies needed

  // Process history data into chart format - use useCallback to memoize
  const processHistoryData = useCallback((historyData) => {
    try {
      // Group validation results by date
      const resultsByDate = {};

      // Process each history item
      historyData.forEach(item => {
        // Extract date from run_at field
        const runDate = item.run_at || item.timestamp || new Date().toISOString();
        const dateStr = new Date(runDate).toISOString().split('T')[0];

        // Initialize date entry if it doesn't exist
        if (!resultsByDate[dateStr]) {
          resultsByDate[dateStr] = {
            date: dateStr,
            total: 0,
            passed: 0,
            failed: 0,
            error: 0,
            health_score: 0
          };
        }

        // Count this result
        resultsByDate[dateStr].total++;

        // Categorize result
        if (item.error) {
          resultsByDate[dateStr].error++;
        } else if (item.is_valid === true) {
          resultsByDate[dateStr].passed++;
        } else if (item.is_valid === false) {
          resultsByDate[dateStr].failed++;
        }
      });

      // Calculate health scores for each date
      Object.keys(resultsByDate).forEach(dateStr => {
        const dateData = resultsByDate[dateStr];
        const validResults = dateData.passed + dateData.failed;

        if (validResults > 0) {
          dateData.health_score = Math.round((dateData.passed / validResults) * 100);
        }
      });

      // Convert to array and sort by date
      const aggregatedData = Object.values(resultsByDate)
        .sort((a, b) => a.date.localeCompare(b.date));

      console.log("Aggregated history data:", aggregatedData);

      // If we have data, update the chart
      if (aggregatedData.length > 0) {
        setChartData(aggregatedData);
      } else {
        // Fallback to current data if available
        if (current && current.length > 0) {
          createSingleDataPoint(current);
        } else {
          setChartData([]);
        }
      }
    } catch (error) {
      console.error("Error processing history data:", error);
      // Create fallback with current data if available
      if (current && current.length > 0) {
        createSingleDataPoint(current);
      } else {
        setChartData([]);
      }
    }
  }, [current, createSingleDataPoint]); // Add createSingleDataPoint as dependency

  // Generate a processing key when table or data changes to force re-processing
  useEffect(() => {
    const newKey = `${selectedTable}-${lastFetched ? lastFetched.getTime() : 'initial'}`;
    setProcessingKey(newKey);
  }, [selectedTable, lastFetched]);

  // Process data when processing key changes
  useEffect(() => {
    console.log(`Processing data for ${processingKey}`);
    console.log("ValidationResultsTrend - Received data:", {
      selectedTable,
      historyLength: history?.length || 0,
      currentResults: current?.length || 0,
      lastFetched: lastFetched ? lastFetched.toISOString() : null
    });

    // Clear chart data when table changes
    if (processingKey.includes('initial') || !processingKey) {
      setChartData([]);
      return;
    }

    // Transform history data into chart-friendly format
    if (history && history.length > 0) {
      processHistoryData(history);
    } else if (current && current.length > 0) {
      // If we only have current data, create a single point
      createSingleDataPoint(current);
    } else {
      // Clear chart data if we have no data
      setChartData([]);
    }
  }, [processingKey, history, current, selectedTable, lastFetched, processHistoryData, createSingleDataPoint]);

  if (isLoading) {
    return (
      <div className="bg-white p-4 rounded-lg shadow flex justify-center items-center h-64">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (!chartData || chartData.length === 0) {
    return (
      <div className="bg-white p-4 rounded-lg shadow">
        <div className="text-secondary-500 text-center p-8">
          No historical data available yet for {selectedTable}.
          <br />
          Run validations multiple times to see trends.
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

      {/* Debug information (only in development) */}
      {process.env.NODE_ENV === 'development' && (
        <div className="mt-2 border-t border-secondary-200 pt-2 text-xs text-secondary-500">
          <details>
            <summary>Debug Data</summary>
            <pre className="mt-2 bg-secondary-50 p-2 rounded overflow-auto max-h-40">
              {JSON.stringify({
                processingKey,
                selectedTable,
                chartDataPoints: chartData.length,
                historyLength: history?.length || 0
              }, null, 2)}
            </pre>
          </details>
        </div>
      )}
    </div>
  );
};

export default ValidationResultsTrend;