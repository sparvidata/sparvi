import React, { useMemo } from 'react';
import { useNextRunTimes } from '../../hooks/useNextRunTimes';
import {
  ClockIcon,
  PlayIcon,
  ExclamationTriangleIcon,
  CalendarIcon,
  ArrowPathIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../common/LoadingSpinner';
import { formatAutomationType, formatNextRunTime, getNextRunStatusColor } from '../../utils/scheduleUtils';

const NextRunDisplay = ({
  connectionId,
  connectionName,
  className = '',
  showManualTriggers = true,
  compact = false
}) => {
  // Memoize the options to prevent unnecessary re-renders
  const hookOptions = useMemo(() => ({
    refreshInterval: 60000, // Update every minute
    enabled: true,
    onError: (error) => {
      console.warn(`NextRunDisplay error for connection ${connectionId}:`, error);
    }
  }), [connectionId]);

  const {
    nextRuns,
    loading,
    error,
    refresh,
    triggerManualRun,
    circuitBreakerOpen,
    consecutiveErrors
  } = useNextRunTimes(connectionId, hookOptions);

  const handleManualTrigger = async (automationType) => {
    const success = await triggerManualRun(automationType);
    if (success) {
      console.log(`${formatAutomationType(automationType)} triggered successfully`);
    } else {
      console.error(`Failed to trigger ${formatAutomationType(automationType)}`);
    }
  };

  const handleRefresh = () => {
    console.log('Manual refresh requested');
    refresh();
  };

  // Memoize the filtered and sorted runs to prevent unnecessary recalculations
  const { sortedRuns, hasEnabledRuns } = useMemo(() => {
    const entries = Object.entries(nextRuns).filter(([_, runData]) => runData?.enabled);
    const sorted = entries.sort((a, b) => {
      const aTime = a[1]?.next_run_timestamp || Infinity;
      const bTime = b[1]?.next_run_timestamp || Infinity;
      return aTime - bTime;
    });

    return {
      sortedRuns: sorted,
      hasEnabledRuns: entries.length > 0
    };
  }, [nextRuns]);

  // Show loading state
  if (loading && !hasEnabledRuns) {
    return (
      <div className={`flex justify-center py-4 ${className}`}>
        <LoadingSpinner size="sm" />
        <span className="ml-2 text-sm text-gray-500">Loading next run times...</span>
      </div>
    );
  }

  // Show error state with retry option
  if (error && !hasEnabledRuns) {
    return (
      <div className={`rounded-md bg-yellow-50 p-3 ${className}`}>
        <div className="flex items-center justify-between">
          <div className="flex">
            <ExclamationTriangleIcon className="h-5 w-5 text-yellow-400" />
            <div className="ml-3">
              <p className="text-sm text-yellow-700">
                {circuitBreakerOpen
                  ? 'Too many errors - schedule loading paused'
                  : 'Unable to load schedule information'
                }
              </p>
              {consecutiveErrors > 0 && (
                <p className="text-xs text-yellow-600 mt-1">
                  {consecutiveErrors} consecutive error{consecutiveErrors !== 1 ? 's' : ''}
                </p>
              )}
            </div>
          </div>
          {!circuitBreakerOpen && (
            <button
              onClick={handleRefresh}
              className="flex items-center px-2 py-1 text-xs bg-yellow-100 text-yellow-800 rounded hover:bg-yellow-200 transition-colors"
              title="Retry loading schedules"
            >
              <ArrowPathIcon className="h-3 w-3 mr-1" />
              Retry
            </button>
          )}
        </div>
      </div>
    );
  }

  // Show empty state
  if (!hasEnabledRuns) {
    return (
      <div className={`text-center py-6 ${className}`}>
        <CalendarIcon className="mx-auto h-8 w-8 text-gray-400" />
        <h3 className="mt-2 text-sm font-medium text-gray-900">No Scheduled Runs</h3>
        <p className="mt-1 text-sm text-gray-500">
          {connectionName ? `No automation scheduled for ${connectionName}` : 'No automation scheduled'}
        </p>
        {error && (
          <button
            onClick={handleRefresh}
            className="mt-3 text-sm text-blue-600 hover:text-blue-700"
          >
            Try loading again
          </button>
        )}
      </div>
    );
  }

  // Compact view for widgets
  if (compact) {
    const nextRun = sortedRuns[0];

    return (
      <div className={`flex items-center justify-between p-2 ${className}`}>
        <div className="flex items-center">
          <ClockIcon className="h-4 w-4 text-gray-400 mr-2" />
          <div>
            <div className="text-sm font-medium text-gray-900">
              {nextRun ? formatAutomationType(nextRun[0]) : 'No runs scheduled'}
            </div>
            {nextRun && (
              <div className="text-xs text-gray-500">
                {connectionName}
              </div>
            )}
          </div>
        </div>

        {nextRun && (
          <div className={`text-sm ${getNextRunStatusColor(nextRun[1])}`}>
            {formatNextRunTime(nextRun[1])}
          </div>
        )}

        {loading && (
          <LoadingSpinner size="xs" />
        )}
      </div>
    );
  }

  // Full view
  return (
    <div className={`space-y-3 ${className}`}>
      {/* Header with refresh button */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center">
          <ClockIcon className="h-5 w-5 text-gray-400 mr-2" />
          <h4 className="text-sm font-medium text-gray-900">
            Next Scheduled Runs{connectionName ? ` - ${connectionName}` : ''}
          </h4>
        </div>

        <div className="flex items-center space-x-2">
          {loading && <LoadingSpinner size="xs" />}
          {error && consecutiveErrors > 0 && (
            <span className="text-xs text-yellow-600">
              {consecutiveErrors} error{consecutiveErrors !== 1 ? 's' : ''}
            </span>
          )}
          <button
            onClick={handleRefresh}
            disabled={loading || circuitBreakerOpen}
            className="p-1 text-gray-400 hover:text-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
            title="Refresh schedules"
          >
            <ArrowPathIcon className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Runs list */}
      {sortedRuns.map(([automationType, runData]) => {
        const statusColor = getNextRunStatusColor(runData);
        const nextRunText = formatNextRunTime(runData);

        return (
          <div
            key={automationType}
            className="flex items-center justify-between p-3 bg-gray-50 rounded-lg border"
          >
            <div className="flex-1 min-w-0">
              <div className="flex items-center">
                <div className="text-sm font-medium text-gray-900 truncate">
                  {formatAutomationType(automationType)}
                </div>
                {runData.currently_running && (
                  <LoadingSpinner size="xs" className="ml-2" />
                )}
              </div>

              <div className="mt-1 flex items-center text-xs text-gray-500">
                <span>
                  {runData.schedule_type === 'daily' ? 'Daily' : 'Weekly'} at {runData.scheduled_time}
                </span>
                {runData.timezone && (
                  <span className="ml-2">({runData.timezone})</span>
                )}
                {runData.schedule_type === 'weekly' && runData.days && (
                  <span className="ml-2">
                    on {runData.days.map(d => d.charAt(0).toUpperCase() + d.slice(1)).join(', ')}
                  </span>
                )}
              </div>
            </div>

            <div className="flex items-center space-x-3">
              <div className="text-right">
                <div className={`text-sm font-medium ${statusColor}`}>
                  {nextRunText}
                </div>
                {runData.next_run_iso && !runData.currently_running && !runData.is_overdue && (
                  <div className="text-xs text-gray-500">
                    {new Date(runData.next_run_iso).toLocaleString(undefined, {
                      month: 'short',
                      day: 'numeric',
                      hour: 'numeric',
                      minute: '2-digit'
                    })}
                  </div>
                )}
              </div>

              {showManualTriggers && !runData.currently_running && (
                <button
                  onClick={() => handleManualTrigger(automationType)}
                  className="flex items-center px-2 py-1 text-xs bg-blue-100 text-blue-800 rounded hover:bg-blue-200 transition-colors"
                  title={`Run ${formatAutomationType(automationType)} now`}
                >
                  <PlayIcon className="h-3 w-3 mr-1" />
                  Run Now
                </button>
              )}
            </div>
          </div>
        );
      })}

      {/* Error indicator at bottom if there are runs but also errors */}
      {error && hasEnabledRuns && (
        <div className="text-center">
          <p className="text-xs text-yellow-600">
            {circuitBreakerOpen
              ? 'Schedule updates paused due to errors'
              : 'Some schedule data may be outdated'
            }
          </p>
        </div>
      )}
    </div>
  );
};

export default NextRunDisplay;