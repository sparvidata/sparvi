import React, { useState, useEffect, useCallback } from 'react';
import { ChevronDownIcon, ChevronUpIcon, ExclamationTriangleIcon, ArrowPathIcon } from '@heroicons/react/24/outline';
import HealthScoreChart from './HealthScoreChart';
import ValidationVolumeChart from './ValidationVolumeChart';
import ValidationMetricCards from './ValidationMetricCards';
import ValidationDetailCharts from './ValidationDetailCharts';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { validationsAPI } from '../../../api/enhancedApiService';

const ValidationHealthDashboard = ({
  connectionId,
  tableName,
  days = 30
}) => {
  const [trendData, setTrendData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showDetails, setShowDetails] = useState(false);
  const [summaryMetrics, setSummaryMetrics] = useState(null);
  const [retryCount, setRetryCount] = useState(0);
  const [lastFetchTime, setLastFetchTime] = useState(null);

  // Enhanced fetch function with retry logic
  const fetchTrendData = useCallback(async (isRetry = false) => {
    // Only show loading on initial load or manual retry
    if (!isRetry) {
      setIsLoading(true);
    }

    setError(null);

    try {
      console.log(`[ValidationHealthDashboard] Fetching trends for ${tableName}, attempt ${retryCount + 1}`);

      const response = await validationsAPI.getValidationTrends(
        connectionId,
        tableName,
        {
          days,
          forceFresh: isRetry // Force fresh data on retry
        }
      );

      console.log(`[ValidationHealthDashboard] API Response:`, response);

      // Validate response structure
      if (!response) {
        throw new Error('No response received from server');
      }

      // Handle different response formats
      let processedData = [];
      if (Array.isArray(response.trends)) {
        processedData = response.trends;
      } else if (Array.isArray(response)) {
        processedData = response;
      } else {
        console.warn('[ValidationHealthDashboard] Unexpected response format:', response);
        processedData = [];
      }

      setTrendData(processedData);
      setLastFetchTime(new Date());

      // Calculate summary metrics if we have data
      if (processedData.length > 0) {
        const metrics = calculateSummaryMetrics(processedData);
        setSummaryMetrics(metrics);
        console.log(`[ValidationHealthDashboard] Calculated metrics:`, metrics);
      } else {
        setSummaryMetrics(null);
        console.log(`[ValidationHealthDashboard] No data available for metrics calculation`);
      }

      // Reset retry count on success
      setRetryCount(0);

    } catch (err) {
      console.error(`[ValidationHealthDashboard] Error fetching validation trends:`, err);

      // Enhanced error handling with categorization
      let errorMessage = 'Failed to load validation trends';
      let errorType = 'unknown';

      if (err.response?.status === 404) {
        errorMessage = 'Table or connection not found';
        errorType = 'not_found';
      } else if (err.response?.status === 403) {
        errorMessage = 'Access denied - check your permissions';
        errorType = 'permission';
      } else if (err.response?.status >= 500) {
        errorMessage = 'Server error - please try again';
        errorType = 'server';
      } else if (err.message?.includes('timeout')) {
        errorMessage = 'Request timed out - please try again';
        errorType = 'timeout';
      } else if (err.message) {
        errorMessage = err.message;
      }

      setError({
        message: errorMessage,
        type: errorType,
        canRetry: errorType !== 'not_found' && errorType !== 'permission'
      });

    } finally {
      setIsLoading(false);
    }
  }, [connectionId, tableName, days, retryCount]);

  // Initial data fetch
  useEffect(() => {
    if (!connectionId || !tableName) {
      setIsLoading(false);
      return;
    }

    fetchTrendData();
  }, [connectionId, tableName, days, fetchTrendData]);

  // Enhanced retry function with exponential backoff
  const handleRetry = useCallback(async () => {
    const newRetryCount = retryCount + 1;
    setRetryCount(newRetryCount);

    // Add a small delay for better UX
    if (newRetryCount > 1) {
      const delay = Math.min(1000 * Math.pow(2, newRetryCount - 2), 5000); // Cap at 5 seconds
      console.log(`[ValidationHealthDashboard] Retrying in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }

    await fetchTrendData(true);
  }, [fetchTrendData, retryCount]);

  // Enhanced summary metrics calculation with error handling
  const calculateSummaryMetrics = useCallback((data) => {
    try {
      if (!data || data.length === 0) return null;

      // Filter out days with zero validations for meaningful stats
      const validDays = data.filter(day => day.total_validations > 0);

      if (validDays.length === 0) {
        console.log('[ValidationHealthDashboard] No days with validation activity found');
        return null;
      }

      const latestDay = validDays[validDays.length - 1];
      const previousDay = validDays.length > 1 ? validDays[validDays.length - 2] : null;

      // Calculate trends with null checks
      const healthScoreTrend = previousDay
        ? (latestDay.health_score || 0) - (previousDay.health_score || 0)
        : 0;

      // Calculate averages with safety checks
      const avgHealthScore = validDays.reduce((sum, day) => sum + (day.health_score || 0), 0) / validDays.length;
      const avgDailyFailures = validDays.reduce((sum, day) => sum + (day.failed || 0), 0) / validDays.length;
      const totalValidationsRun = validDays.reduce((sum, day) => sum + (day.total_validations || 0), 0);

      // Find days since last failure
      let daysSinceLastFailure = 0;
      for (let i = validDays.length - 1; i >= 0; i--) {
        if ((validDays[i].failed || 0) > 0) {
          break;
        }
        daysSinceLastFailure++;
      }

      const metrics = {
        currentHealthScore: latestDay.health_score || 0,
        healthScoreTrend,
        avgHealthScore: Math.round(avgHealthScore),
        totalValidationsRun,
        avgDailyFailures: Math.round(avgDailyFailures * 10) / 10,
        daysSinceLastFailure,
        activeDays: validDays.length,
        latestTotalValidations: latestDay.total_validations || 0,
        latestPassed: latestDay.passed || 0,
        latestFailed: latestDay.failed || 0,
        latestNotRun: latestDay.not_run || 0
      };

      console.log('[ValidationHealthDashboard] Metrics calculated successfully:', metrics);
      return metrics;

    } catch (error) {
      console.error('[ValidationHealthDashboard] Error calculating metrics:', error);
      return null;
    }
  }, []);

  // Enhanced loading state with progress indication
  if (isLoading) {
    return (
      <div className="bg-white p-6 rounded-lg shadow">
        <div className="flex flex-col justify-center items-center h-64">
          <LoadingSpinner size="lg" />
          <p className="mt-4 text-secondary-500 text-center">
            Loading validation trends for {tableName}...
            {retryCount > 0 && (
              <span className="block text-sm text-secondary-400 mt-1">
                Attempt {retryCount + 1}
              </span>
            )}
          </p>
          {lastFetchTime && (
            <p className="mt-2 text-xs text-secondary-400">
              Last updated: {lastFetchTime.toLocaleTimeString()}
            </p>
          )}
        </div>
      </div>
    );
  }

  // Enhanced error state with actionable options
  if (error) {
    return (
      <div className="bg-white p-6 rounded-lg shadow border border-danger-200">
        <div className="flex flex-col justify-center items-center h-64">
          <ExclamationTriangleIcon className="h-12 w-12 text-danger-400 mb-4" />

          <h3 className="text-lg font-medium text-secondary-900 mb-2">
            Unable to Load Validation Trends
          </h3>

          <p className="text-danger-600 text-center mb-4 max-w-md">
            {error.message}
          </p>

          <div className="flex flex-col sm:flex-row gap-3">
            {error.canRetry && (
              <button
                onClick={handleRetry}
                disabled={retryCount >= 3}
                className="flex items-center px-4 py-2 bg-primary-600 text-white rounded-md hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <ArrowPathIcon className="h-4 w-4 mr-2" />
                {retryCount >= 3 ? 'Max Retries Reached' : 'Try Again'}
              </button>
            )}

            <button
              onClick={() => window.location.reload()}
              className="px-4 py-2 bg-secondary-100 text-secondary-700 rounded-md hover:bg-secondary-200"
            >
              Refresh Page
            </button>
          </div>

          {retryCount > 0 && (
            <p className="mt-4 text-xs text-secondary-500">
              Retry attempts: {retryCount}/3
            </p>
          )}

          {lastFetchTime && (
            <p className="mt-2 text-xs text-secondary-400">
              Last successful fetch: {lastFetchTime.toLocaleString()}
            </p>
          )}
        </div>
      </div>
    );
  }

  // Enhanced empty state with helpful guidance
  if (!trendData || trendData.length === 0 || !summaryMetrics) {
    return (
      <div className="bg-white p-6 rounded-lg shadow">
        <div className="flex flex-col justify-center items-center h-64">
          <h3 className="text-lg font-medium text-secondary-900 mb-2">
            Validation Health Trends
          </h3>
          <p className="text-secondary-500 text-center mb-4">
            No historical data available yet for {tableName}.
            <br />
            Run validations multiple times to see trends over time.
          </p>

          <div className="bg-secondary-50 p-4 rounded-lg max-w-md">
            <h4 className="text-sm font-medium text-secondary-900 mb-2">
              To see trends, you need to:
            </h4>
            <ul className="text-sm text-secondary-600 space-y-1">
              <li>• Create validation rules for this table</li>
              <li>• Run validations multiple times</li>
              <li>• Wait for historical data to accumulate</li>
            </ul>
          </div>

          {lastFetchTime && (
            <p className="mt-4 text-xs text-secondary-400">
              Last checked: {lastFetchTime.toLocaleString()}
            </p>
          )}
        </div>
      </div>
    );
  }

  // Main dashboard render
  return (
    <div className="space-y-6">
      {/* Header with refresh option */}
      <div className="bg-white p-4 rounded-lg shadow">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-lg font-medium text-secondary-900 mb-1">
              Validation Health Trends
            </h3>
            <p className="text-sm text-secondary-500">
              {tableName} • Last {days} days • {summaryMetrics.activeDays} days with validation activity
            </p>
          </div>

          <div className="flex items-center space-x-2">
            {lastFetchTime && (
              <span className="text-xs text-secondary-400">
                Updated: {lastFetchTime.toLocaleTimeString()}
              </span>
            )}

            <button
              onClick={() => handleRetry()}
              className="text-secondary-500 hover:text-secondary-700 p-1"
              title="Refresh data"
            >
              <ArrowPathIcon className="h-4 w-4" />
            </button>
          </div>
        </div>
      </div>

      {/* Summary Metrics Cards */}
      <ValidationMetricCards metrics={summaryMetrics} />

      {/* Primary Health Score Chart */}
      <HealthScoreChart
        data={trendData}
        title="Health Score Over Time"
        currentScore={summaryMetrics.currentHealthScore}
        trend={summaryMetrics.healthScoreTrend}
      />

      {/* Validation Volume Chart */}
      <ValidationVolumeChart
        data={trendData}
        title="Validation Results Volume"
      />

      {/* Detailed Charts Toggle */}
      <div className="bg-white rounded-lg shadow">
        <button
          onClick={() => setShowDetails(!showDetails)}
          className="w-full px-4 py-3 flex items-center justify-between text-left hover:bg-secondary-50 rounded-lg transition-colors"
        >
          <span className="text-sm font-medium text-secondary-700">
            Detailed Breakdown
          </span>
          {showDetails ? (
            <ChevronUpIcon className="h-5 w-5 text-secondary-400" />
          ) : (
            <ChevronDownIcon className="h-5 w-5 text-secondary-400" />
          )}
        </button>

        {showDetails && (
          <div className="border-t border-secondary-200 p-4">
            <ValidationDetailCharts data={trendData} />
          </div>
        )}
      </div>

      {/* Debug information in development */}
      {process.env.NODE_ENV === 'development' && (
        <div className="bg-secondary-50 p-4 rounded-lg">
          <details className="text-xs">
            <summary className="cursor-pointer font-medium text-secondary-700 mb-2">
              Debug Information
            </summary>
            <div className="space-y-2 text-secondary-600">
              <div>Connection ID: {connectionId}</div>
              <div>Table Name: {tableName}</div>
              <div>Days Requested: {days}</div>
              <div>Trend Data Points: {trendData.length}</div>
              <div>Active Days: {summaryMetrics?.activeDays || 0}</div>
              <div>Retry Count: {retryCount}</div>
              <div>Last Fetch: {lastFetchTime?.toISOString() || 'Never'}</div>
            </div>
          </details>
        </div>
      )}
    </div>
  );
};

export default ValidationHealthDashboard;