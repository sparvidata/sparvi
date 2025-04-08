import React from 'react';
import {
  CheckCircleIcon,
  XCircleIcon,
  ExclamationTriangleIcon,
  ClockIcon,
  ArrowUpIcon,
  ArrowDownIcon,
  ArrowPathIcon
} from '@heroicons/react/24/outline';
import { useValidationResults } from '../../../contexts/ValidationResultsContext';
import { formatDate } from '../../../utils/formatting';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

const ValidationResultsSummary = ({ onRunAll, isRunning }) => {
  const {
    metrics,
    lastFetched,
    isLoading,
    isLoadingResults,
    error,
    trends,
    selectedTable,
    rulesLoaded,
    resultsLoaded
  } = useValidationResults();

  // Show loading state when we're loading the results specifically
  if (isLoading || isLoadingResults) {
    return (
      <div className="bg-white px-4 py-5 border-b border-secondary-200">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-secondary-900">Validation Health</h2>
            <p className="text-sm text-secondary-500">
              Loading validation results...
            </p>
          </div>
          <div className="flex items-center">
            <LoadingSpinner size="md" />
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white px-4 py-5 border-b border-secondary-200">
        <div className="flex items-center text-danger-600">
          <ExclamationTriangleIcon className="h-5 w-5 mr-2" />
          <span>Error loading validation results: {error}</span>
        </div>

        {/* Add a retry button */}
        <div className="mt-2">
          <button
            onClick={onRunAll}
            className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          >
            <ArrowPathIcon className="h-4 w-4 mr-2" />
            Run Validations
          </button>
        </div>
      </div>
    );
  }

  // Check if we have meaningful metrics (explicitly check counts properties)
  const hasMetrics = metrics &&
    (metrics.total > 0 ||
     (metrics.counts && (
       metrics.counts.passed > 0 ||
       metrics.counts.failed > 0 ||
       metrics.counts.error > 0
     ))
    );

  // Handle cases where we have rules but no metrics yet
  if (rulesLoaded && !resultsLoaded) {
    return (
      <div className="bg-white px-4 py-5 border-b border-secondary-200">
        <div className="flex justify-between items-center">
          <div>
            <h2 className="text-lg font-semibold text-secondary-900">Validation Health</h2>
            <p className="text-sm text-secondary-500">
              Loading results for {selectedTable || 'this table'}...
            </p>
          </div>
          <div>
            <LoadingSpinner size="md" />
          </div>
        </div>
      </div>
    );
  }

  if (!hasMetrics) {
    return (
      <div className="bg-white px-4 py-5 border-b border-secondary-200">
        <div className="text-secondary-700 mb-4 flex items-center">
          <ExclamationTriangleIcon className="h-5 w-5 mr-2 text-secondary-400" />
          <span>
            {rulesLoaded
              ? `No validation results available for ${selectedTable || 'this table'}.`
              : `Select a table to view validation results.`}
          </span>
        </div>

        <button
          type="button"
          onClick={onRunAll}
          disabled={isRunning || !selectedTable}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
        >
          {isRunning ? (
            <>
              <LoadingSpinner size="sm" className="mr-2" />
              Running...
            </>
          ) : (
            <>
              Run Validations
            </>
          )}
        </button>
      </div>
    );
  }

  // Calculate health color based on health score
  const getHealthColor = (score) => {
    if (score >= 90) return 'accent';
    if (score >= 70) return 'warning';
    return 'danger';
  };

  // Ensure health_score is a number and round it
  const healthScore = metrics.health_score ? Math.round(Number(metrics.health_score)) : 0;
  const healthColor = getHealthColor(healthScore);

  // Calculate trend (if we have enough data)
  let trend = null;
  if (trends && trends.length >= 2) {
    const latest = trends[trends.length - 1];
    const previous = trends[trends.length - 2];

    const diff = latest.health_score - previous.health_score;
    if (Math.abs(diff) >= 1) {
      trend = {
        value: diff,
        improved: diff > 0
      };
    }
  }

  return (
    <div className="bg-white px-4 py-5 border-b border-secondary-200">
      <div className="flex flex-col md:flex-row md:items-center md:justify-between">
        <div className="mb-4 md:mb-0">
          <h2 className="text-lg font-semibold text-secondary-900">Validation Health</h2>
          <div className="mt-1 text-sm text-secondary-500">
            Last updated: {lastFetched ? formatDate(lastFetched, true) : 'Never'}
          </div>
        </div>

        <div className="flex items-center space-x-4">
          {/* Health Score */}
          <div className="flex flex-col items-center">
            <div className={`text-2xl font-bold text-${healthColor}-600`}>
              {healthScore}%
            </div>
            <div className="text-xs text-secondary-500">Health Score</div>

            {/* Trend indicator */}
            {trend && (
              <div className={`flex items-center text-xs mt-1 ${
                trend.improved ? 'text-accent-600' : 'text-danger-600'
              }`}>
                {trend.improved ? (
                  <ArrowUpIcon className="h-3 w-3 mr-1" />
                ) : (
                  <ArrowDownIcon className="h-3 w-3 mr-1" />
                )}
                <span>{Math.abs(trend.value)}%</span>
              </div>
            )}
          </div>

          {/* Passed Count */}
          <div className="flex flex-col items-center">
            <div className="flex items-center text-xl font-semibold text-accent-600">
              <CheckCircleIcon className="h-5 w-5 mr-1" />
              {metrics.counts?.passed || 0}
            </div>
            <div className="text-xs text-secondary-500">Passed</div>
          </div>

          {/* Failed Count */}
          <div className="flex flex-col items-center">
            <div className="flex items-center text-xl font-semibold text-danger-600">
              <XCircleIcon className="h-5 w-5 mr-1" />
              {metrics.counts?.failed || 0}
            </div>
            <div className="text-xs text-secondary-500">Failed</div>
          </div>

          {/* Error Count */}
          <div className="flex flex-col items-center">
            <div className="flex items-center text-xl font-semibold text-warning-600">
              <ExclamationTriangleIcon className="h-5 w-5 mr-1" />
              {metrics.counts?.error || 0}
            </div>
            <div className="text-xs text-secondary-500">Errors</div>
          </div>

          {/* Performance */}
          {metrics.avg_execution_time > 0 && (
            <div className="flex flex-col items-center">
              <div className="flex items-center text-xl font-semibold text-secondary-600">
                <ClockIcon className="h-5 w-5 mr-1" />
                {Math.round(metrics.avg_execution_time)}ms
              </div>
              <div className="text-xs text-secondary-500">Avg. Time</div>
            </div>
          )}
        </div>
      </div>

      {/* Action Button */}
      <div className="mt-4 flex justify-end">
        <button
          type="button"
          onClick={onRunAll}
          disabled={isRunning}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
        >
          {isRunning ? (
            <>
              <LoadingSpinner size="sm" className="mr-2" />
              Running...
            </>
          ) : (
            <>
              Run All Validations
            </>
          )}
        </button>
      </div>
    </div>
  );
};

export default ValidationResultsSummary;