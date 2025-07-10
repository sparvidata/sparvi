import React from 'react';
import { Link } from 'react-router-dom';
import { useScheduleConfig } from '../../hooks/useScheduleConfig';
import { useNextRunTimes } from '../../hooks/useNextRunTimes';
import {
  ClockIcon,
  Cog6ToothIcon,
  PlayIcon,
  PauseIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../common/LoadingSpinner';
import { hasEnabledAutomation, formatNextRunTime, getNextRunStatusColor } from '../../utils/scheduleUtils';

const ScheduleStatusWidget = ({ connectionId, connectionName, className = '', compact = false }) => {
  const { schedule, loading: scheduleLoading } = useScheduleConfig(connectionId);
  const { nextRuns, loading: nextRunsLoading, error: nextRunsError } = useNextRunTimes(connectionId, {
    refreshInterval: 120000, // Update every 2 minutes for widgets
    onError: (error) => {
      console.warn(`Widget error for connection ${connectionId}:`, error);
    }
  });

  if (scheduleLoading) {
    return (
      <div className={`flex items-center justify-center p-4 ${className}`}>
        <LoadingSpinner size="sm" />
        <span className="ml-2 text-sm text-gray-500">Loading...</span>
      </div>
    );
  }

  if (!schedule) {
    return (
      <div className={`p-4 bg-gray-50 rounded-lg ${className}`}>
        <div className="text-center">
          <PauseIcon className="mx-auto h-6 w-6 text-gray-400" />
          <p className="mt-1 text-sm text-gray-500">No automation configured</p>
          <Link
            to={`/settings/automation?connection=${connectionId}`}
            className="mt-2 text-xs text-blue-600 hover:text-blue-700"
          >
            Configure Automation
          </Link>
        </div>
      </div>
    );
  }

  const isEnabled = hasEnabledAutomation(schedule);
  const enabledCount = Object.values(schedule).filter(config => config.enabled).length;
  const totalCount = Object.keys(schedule).length;

  // Get the next upcoming run
  const upcomingRuns = Object.entries(nextRuns)
    .filter(([_, runData]) => runData.enabled && !runData.is_overdue)
    .sort((a, b) => (a[1].next_run_timestamp || Infinity) - (b[1].next_run_timestamp || Infinity));

  const nextRun = upcomingRuns[0];
  const overdueCount = Object.values(nextRuns).filter(runData => runData.is_overdue).length;

  if (compact) {
    return (
      <div className={`flex items-center justify-between p-3 bg-white border rounded-lg ${className}`}>
        <div className="flex items-center">
          <div className={`w-2 h-2 rounded-full mr-3 ${
            isEnabled ? 'bg-green-500' : 'bg-gray-300'
          }`} />
          <div>
            <div className="text-sm font-medium text-gray-900">Automation</div>
            <div className="text-xs text-gray-500">
              {enabledCount} of {totalCount} enabled
            </div>
          </div>
        </div>

        <div className="text-right">
          {isEnabled && nextRun && !nextRunsLoading ? (
            <div className="text-xs text-gray-600">
              Next: {formatNextRunTime(nextRun[1])}
            </div>
          ) : nextRunsLoading ? (
            <LoadingSpinner size="xs" />
          ) : (
            <div className="text-xs text-gray-400">Inactive</div>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className={`bg-white border rounded-lg ${className}`}>
      <div className="p-4">
        {/* Header */}
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center">
            <ClockIcon className="h-5 w-5 text-gray-400 mr-2" />
            <h4 className="text-sm font-medium text-gray-900">Automation Status</h4>
          </div>

          <div className={`flex items-center px-2 py-1 rounded text-xs font-medium ${
            isEnabled 
              ? 'bg-green-100 text-green-800' 
              : 'bg-gray-100 text-gray-600'
          }`}>
            {isEnabled ? (
              <>
                <PlayIcon className="h-3 w-3 mr-1" />
                Active
              </>
            ) : (
              <>
                <PauseIcon className="h-3 w-3 mr-1" />
                Inactive
              </>
            )}
          </div>
        </div>

        {/* Status */}
        <div className="text-sm text-gray-600 mb-3">
          {enabledCount} of {totalCount} automation types enabled
          {connectionName && ` for ${connectionName}`}
        </div>

        {/* Overdue warning */}
        {overdueCount > 0 && (
          <div className="mb-3 p-2 bg-yellow-50 border border-yellow-200 rounded">
            <div className="flex items-center">
              <ExclamationTriangleIcon className="h-4 w-4 text-yellow-600 mr-2" />
              <span className="text-sm text-yellow-800">
                {overdueCount} overdue run{overdueCount !== 1 ? 's' : ''}
              </span>
            </div>
          </div>
        )}

        {/* Next run */}
        {isEnabled && (
          <div className="pt-3 border-t">
            {nextRunsLoading ? (
              <div className="flex items-center text-xs text-gray-500">
                <LoadingSpinner size="xs" className="mr-2" />
                Loading next run...
              </div>
            ) : nextRunsError ? (
              <div className="text-xs text-gray-400">
                Schedule information unavailable
              </div>
            ) : nextRun ? (
              <div>
                <div className="text-xs text-gray-500 mb-1">Next run:</div>
                <div className="flex items-center justify-between">
                  <div className="text-sm font-medium text-gray-900">
                    {nextRun[0].replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}
                  </div>
                  <div className={`text-sm ${getNextRunStatusColor(nextRun[1])}`}>
                    {formatNextRunTime(nextRun[1])}
                  </div>
                </div>
                {nextRun[1].next_run_iso && (
                  <div className="text-xs text-gray-500 mt-1">
                    {new Date(nextRun[1].next_run_iso).toLocaleString()}
                  </div>
                )}
              </div>
            ) : (
              <div className="text-xs text-gray-500">
                No upcoming runs scheduled
              </div>
            )}
          </div>
        )}

        {/* Configure link */}
        <div className="pt-3 border-t mt-3">
          <Link
            to={`/settings/automation?connection=${connectionId}`}
            className="flex items-center text-xs text-blue-600 hover:text-blue-700"
          >
            <Cog6ToothIcon className="h-3 w-3 mr-1" />
            Configure Automation
          </Link>
        </div>
      </div>
    </div>
  );
};

export default ScheduleStatusWidget;