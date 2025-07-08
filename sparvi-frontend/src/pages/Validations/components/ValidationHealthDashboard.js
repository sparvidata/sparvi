import React, { useState, useEffect } from 'react';
import { ChevronDownIcon, ChevronUpIcon } from '@heroicons/react/24/outline';
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

        console.log(`Loading validation trends for ${tableName}...`);

        const response = await validationsAPI.getValidationTrends(connectionId, tableName, { days });

        // Check if we got a response with trends data
        if (response && Array.isArray(response.trends)) {
          const processedData = response.trends;
          setTrendData(processedData);

          // Calculate summary metrics
          const metrics = calculateSummaryMetrics(processedData);
          setSummaryMetrics(metrics);
        } else {
          // Handle empty or invalid response
          setTrendData([]);
          setSummaryMetrics(null);
        }
      } catch (err) {
        console.error('Error fetching validation trends:', err);
        setError(err.message || 'Failed to load trend data');
      } finally {
        setIsLoading(false);
      }
    };

    fetchTrendData();
  }, [connectionId, tableName, days]);

  // Calculate summary metrics from trend data
  const calculateSummaryMetrics = (data) => {
    if (!data || data.length === 0) return null;

    // Filter out days with zero validations for meaningful stats
    const validDays = data.filter(day => day.total_validations > 0);

    if (validDays.length === 0) return null;

    const latestDay = validDays[validDays.length - 1];
    const previousDay = validDays.length > 1 ? validDays[validDays.length - 2] : null;

    // Calculate trends
    const healthScoreTrend = previousDay
      ? latestDay.health_score - previousDay.health_score
      : 0;

    // Calculate averages
    const avgHealthScore = validDays.reduce((sum, day) => sum + day.health_score, 0) / validDays.length;
    const avgDailyFailures = validDays.reduce((sum, day) => sum + (day.failed || 0), 0) / validDays.length;
    const totalValidationsRun = validDays.reduce((sum, day) => sum + day.total_validations, 0);

    // Find days since last failure
    let daysSinceLastFailure = 0;
    for (let i = validDays.length - 1; i >= 0; i--) {
      if (validDays[i].failed > 0) {
        break;
      }
      daysSinceLastFailure++;
    }

    return {
      currentHealthScore: latestDay.health_score,
      healthScoreTrend,
      avgHealthScore: Math.round(avgHealthScore),
      totalValidationsRun,
      avgDailyFailures: Math.round(avgDailyFailures * 10) / 10,
      daysSinceLastFailure,
      activeDays: validDays.length,
      latestTotalValidations: latestDay.total_validations,
      latestPassed: latestDay.passed || 0,
      latestFailed: latestDay.failed || 0,
      latestNotRun: latestDay.not_run || 0
    };
  };

  // Show loading state
  if (isLoading) {
    return (
      <div className="bg-white p-6 rounded-lg shadow">
        <div className="flex flex-col justify-center items-center h-64">
          <LoadingSpinner size="lg" />
          <p className="mt-4 text-secondary-500">
            Loading validation trends for {tableName}...
          </p>
        </div>
      </div>
    );
  }

  // Show error state
  if (error) {
    return (
      <div className="bg-white p-6 rounded-lg shadow">
        <div className="flex flex-col justify-center items-center h-40">
          <p className="text-danger-500 text-center mb-4">{error}</p>
          <button
            onClick={() => window.location.reload()}
            className="px-4 py-2 bg-primary-100 text-primary-700 rounded-md hover:bg-primary-200"
          >
            Try Again
          </button>
        </div>
      </div>
    );
  }

  // Show empty state
  if (!trendData || trendData.length === 0 || !summaryMetrics) {
    return (
      <div className="bg-white p-6 rounded-lg shadow">
        <div className="flex flex-col justify-center items-center h-40">
          <h3 className="text-lg font-medium text-secondary-900 mb-2">
            Validation Health Trends
          </h3>
          <p className="text-secondary-500 text-center">
            No historical data available yet for {tableName}.
            <br />
            Run validations multiple times to see trends over time.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-white p-4 rounded-lg shadow">
        <h3 className="text-lg font-medium text-secondary-900 mb-1">
          Validation Health Trends
        </h3>
        <p className="text-sm text-secondary-500">
          {tableName} • Last {days} days • {summaryMetrics.activeDays} days with validation activity
        </p>
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
    </div>
  );
};

export default ValidationHealthDashboard;