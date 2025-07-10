import React from 'react';
import { Link } from 'react-router-dom';
import { useScheduleConfig } from '../../hooks/useScheduleConfig';
import { useNextRunTimes } from '../../hooks/useNextRunTimes';
import { ClockIcon, Cog6ToothIcon } from '@heroicons/react/24/outline';
import { hasEnabledAutomation, formatNextRunTime } from '../../utils/scheduleUtils';

const ConnectionAutomationBadge = ({ connectionId, compact = true }) => {
  const { schedule, loading: scheduleLoading } = useScheduleConfig(connectionId);
  const { nextRuns, loading: nextRunsLoading } = useNextRunTimes(connectionId, {
    refreshInterval: 180000, // 3 minutes
    enabled: hasEnabledAutomation(schedule)
  });

  if (scheduleLoading) {
    return (
      <div className="flex items-center text-xs text-gray-400">
        <ClockIcon className="h-3 w-3 mr-1" />
        Loading...
      </div>
    );
  }

  const isEnabled = hasEnabledAutomation(schedule);

  if (!isEnabled) {
    return (
      <Link
        to={`/automation?connection=${connectionId}&tab=configure`}
        className="flex items-center text-xs text-gray-400 hover:text-blue-600 transition-colors"
        title="Configure automation"
      >
        <Cog6ToothIcon className="h-3 w-3 mr-1" />
        Configure
      </Link>
    );
  }

  // Get next upcoming run
  const upcomingRuns = Object.entries(nextRuns)
    .filter(([_, runData]) => runData.enabled && !runData.is_overdue)
    .sort((a, b) => (a[1].next_run_timestamp || Infinity) - (b[1].next_run_timestamp || Infinity));

  const nextRun = upcomingRuns[0];

  if (compact) {
    return (
      <div className="flex items-center text-xs">
        <div className="w-2 h-2 bg-green-500 rounded-full mr-2" />
        <span className="text-gray-600">
          {nextRunsLoading ? 'Loading...' : nextRun ? formatNextRunTime(nextRun[1]) : 'Active'}
        </span>
        <Link
          to={`/automation?connection=${connectionId}&tab=configure`}
          className="ml-2 text-blue-600 hover:text-blue-700"
          title="Configure automation"
        >
          <Cog6ToothIcon className="h-3 w-3" />
        </Link>
      </div>
    );
  }

  return (
    <div className="flex items-center justify-between py-2 px-3 bg-green-50 border border-green-200 rounded">
      <div className="flex items-center">
        <ClockIcon className="h-4 w-4 text-green-600 mr-2" />
        <div>
          <div className="text-sm font-medium text-green-900">Automation Active</div>
          {nextRun && !nextRunsLoading && (
            <div className="text-xs text-green-700">
              Next: {formatNextRunTime(nextRun[1])}
            </div>
          )}
        </div>
      </div>
      <Link
        to={`/automation?connection=${connectionId}&tab=configure`}
        className="text-green-600 hover:text-green-700"
        title="Configure automation"
      >
        <Cog6ToothIcon className="h-4 w-4" />
      </Link>
    </div>
  );
};

export default ConnectionAutomationBadge;