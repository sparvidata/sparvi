import React from 'react';
import { useNextRunTimes } from '../../hooks/useNextRunTimes';
import {
  ClockIcon,
  PlayIcon,
  ExclamationTriangleIcon,
  CalendarIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../common/LoadingSpinner';
import { formatAutomationType, formatNextRunTime, getNextRunStatusColor } from '../../utils/scheduleUtils';

const NextRunDisplay = ({ connectionId, connectionName, className = '', showManualTriggers = true }) => {
  const { nextRuns, loading, error, triggerManualRun } = useNextRunTimes(connectionId, {
    refreshInterval: 60000 // Update every minute
  });

  const handleManualTrigger = async (automationType) => {
    const success = await triggerManualRun(automationType);
    if (success) {
      // Show success feedback (you could use a toast notification here)
      console.log(`${formatAutomationType(automationType)} triggered successfully`);
    } else {
      console.error(`Failed to trigger ${formatAutomationType(automationType)}`);
    }
  };

  if (loading) {
    return (
      <div className={`flex justify-center py-4 ${className}`}>
        <LoadingSpinner size="sm" />
        <span className="ml-2 text-sm text-gray-500">Loading next run times...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className={`rounded-md bg-yellow-50 p-3 ${className}`}>
        <div className="flex">
          <ExclamationTriangleIcon className="h-5 w-5 text-yellow-400" />
          <div className="ml-3">
            <p className="text-sm text-yellow-700">
              Unable to load schedule information
            </p>
          </div>
        </div>
      </div>
    );
  }

  const nextRunEntries = Object.entries(nextRuns).filter(([_, runData]) => runData.enabled);

  if (nextRunEntries.length === 0) {
    return (
      <div className={`text-center py-6 ${className}`}>
        <CalendarIcon className="mx-auto h-8 w-8 text-gray-400" />
        <h3 className="mt-2 text-sm font-medium text-gray-900">No Scheduled Runs</h3>
        <p className="mt-1 text-sm text-gray-500">
          {connectionName ? `No automation scheduled for ${connectionName}` : 'No automation scheduled'}
        </p>
      </div>
    );
  }

  // Sort by next run time (soonest first)
  const sortedRuns = nextRunEntries.sort((a, b) => {
    const aTime = a[1].next_run_timestamp || Infinity;
    const bTime = b[1].next_run_timestamp || Infinity;
    return aTime - bTime;
  });

  return (
    <div className={`space-y-3 ${className}`}>
      {connectionName && (
        <div className="flex items-center mb-4">
          <ClockIcon className="h-5 w-5 text-gray-400 mr-2" />
          <h4 className="text-sm font-medium text-gray-900">
            Next Scheduled Runs - {connectionName}
          </h4>
        </div>
      )}

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
    </div>
  );
};

export default NextRunDisplay;