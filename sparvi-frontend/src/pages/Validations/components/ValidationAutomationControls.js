import React from 'react';
import { Link } from 'react-router-dom';
import { useScheduleConfig } from '../../../hooks/useScheduleConfig';
import { useNextRunTimes } from '../../../hooks/useNextRunTimes';
import {
  ClockIcon,
  Cog6ToothIcon,
  PlayIcon,
  PauseIcon,
  ExclamationCircleIcon,
  ClipboardDocumentCheckIcon,
  CalendarIcon,
  CheckCircleIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { formatAutomationType, formatNextRunTime, getNextRunStatusColor, hasEnabledAutomation } from '../../../utils/scheduleUtils';

const ValidationAutomationControls = ({
  connectionId,
  tableName
}) => {
  const { schedule, loading: scheduleLoading, updateSchedule } = useScheduleConfig(connectionId);

  const {
    nextRuns,
    loading: nextRunsLoading,
    error: nextRunsError,
    refresh: refreshNextRuns,
    triggerManualRun,
    circuitBreakerOpen,
    consecutiveErrors
  } = useNextRunTimes(connectionId, {
    refreshInterval: 60000, // Update every minute
    enabled: !!connectionId,
    onError: (error) => {
      console.warn(`Automation error for connection ${connectionId}:`, error);
    }
  });

  // Check if validation automation is enabled
  const validationAutomationEnabled = schedule?.validation_automation?.enabled || false;
  const validationNextRun = nextRuns?.validation_automation;

  // Handle toggling validation automation
  const handleToggleValidationAutomation = async () => {
    if (!schedule) return;

    const updatedSchedule = {
      ...schedule,
      validation_automation: {
        ...schedule.validation_automation,
        enabled: !validationAutomationEnabled
      }
    };

    const success = await updateSchedule(updatedSchedule);
    if (success) {
      // Refresh next runs after a short delay
      setTimeout(() => refreshNextRuns(), 1000);
    }
  };

  // Handle manual trigger
  const handleTriggerManualRun = async () => {
    const success = await triggerManualRun('validation_automation');
    if (success) {
      // Refresh next runs after triggering
      setTimeout(() => refreshNextRuns(), 2000);
    }
  };

  // Show loading state
  if (scheduleLoading) {
    return (
      <div className="bg-white border border-secondary-200 rounded-lg p-4">
        <div className="animate-pulse h-24 bg-secondary-100 rounded"></div>
      </div>
    );
  }

  // Check if any automation is enabled globally
  const hasAnyAutomation = hasEnabledAutomation(schedule);

  return (
    <div className="bg-white border border-secondary-200 rounded-lg">
      <div className="px-4 py-3 border-b border-secondary-200">
        <div className="flex items-center justify-between">
          <h4 className="text-sm font-medium text-secondary-900 flex items-center">
            <ClockIcon className="h-4 w-4 mr-2" />
            Validation Automation
          </h4>
          <Link
            to={`/settings/automation?connection=${connectionId}`}
            className="text-xs text-primary-600 hover:text-primary-700 flex items-center"
          >
            <Cog6ToothIcon className="h-3 w-3 mr-1" />
            Configure
          </Link>
        </div>
      </div>

      <div className="p-4 space-y-4">
        {/* Validation Automation Status */}
        <div>
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs font-medium text-secondary-700">
              Automated Validation Runs
            </span>

            <button
              onClick={handleToggleValidationAutomation}
              disabled={scheduleLoading}
              className={`flex items-center px-2 py-1 rounded text-xs font-medium transition-all duration-200 ${
                validationAutomationEnabled
                  ? 'bg-accent-100 text-accent-800 hover:bg-accent-200'
                  : 'bg-secondary-100 text-secondary-600 hover:bg-secondary-200'
              } ${scheduleLoading ? 'opacity-50 cursor-not-allowed' : ''}`}
            >
              {scheduleLoading ? (
                <>
                  <div className="animate-spin h-3 w-3 border border-secondary-400 border-t-transparent rounded-full mr-1"></div>
                  Loading...
                </>
              ) : validationAutomationEnabled ? (
                <>
                  <PlayIcon className="h-3 w-3 mr-1" />
                  On
                </>
              ) : (
                <>
                  <PauseIcon className="h-3 w-3 mr-1" />
                  Off
                </>
              )}
            </button>
          </div>

          {/* Schedule details */}
          {validationAutomationEnabled && schedule?.validation_automation && (
            <div className="text-xs text-secondary-500 mb-2">
              <div className="flex items-center justify-between">
                <span>
                  Runs {schedule.validation_automation.schedule_type === 'daily' ? 'daily' : 'weekly'} at {schedule.validation_automation.time}
                  {schedule.validation_automation.timezone && ` (${schedule.validation_automation.timezone})`}
                </span>
              </div>
              {schedule.validation_automation.schedule_type === 'weekly' && schedule.validation_automation.days && (
                <div className="mt-1">
                  On {schedule.validation_automation.days.map(d => d.charAt(0).toUpperCase() + d.slice(1)).join(', ')}
                </div>
              )}
            </div>
          )}

          {/* Next run information */}
          {validationAutomationEnabled && validationNextRun && !nextRunsLoading && (
            <div className={`mt-2 p-2 rounded text-xs ${
              validationNextRun.is_overdue 
                ? 'bg-danger-50 border border-danger-200' 
                : validationNextRun.currently_running
                ? 'bg-primary-50 border border-primary-200'
                : 'bg-accent-50 border border-accent-200'
            }`}>
              <div className="flex items-center justify-between">
                <div className="flex items-center">
                  {validationNextRun.currently_running ? (
                    <LoadingSpinner size="xs" className="mr-2" />
                  ) : validationNextRun.is_overdue ? (
                    <ExclamationTriangleIcon className="h-3 w-3 mr-2 text-danger-600" />
                  ) : (
                    <CalendarIcon className="h-3 w-3 mr-2 text-accent-600" />
                  )}
                  <div>
                    <div className={`font-medium ${getNextRunStatusColor(validationNextRun)}`}>
                      {validationNextRun.currently_running
                        ? 'Validation automation is running'
                        : validationNextRun.is_overdue
                        ? 'Validation automation is overdue'
                        : 'Next validation run'
                      }
                    </div>
                    {validationNextRun.next_run_iso && !validationNextRun.currently_running && (
                      <div className="text-secondary-600">
                        {formatNextRunTime(validationNextRun)}
                      </div>
                    )}
                  </div>
                </div>
                {!validationNextRun.currently_running && (
                  <button
                    onClick={handleTriggerManualRun}
                    className={`text-xs hover:underline ${getNextRunStatusColor(validationNextRun)}`}
                    title="Trigger validation automation now"
                  >
                    Run Now
                  </button>
                )}
              </div>
            </div>
          )}

          {/* Manual trigger when no schedule or loading */}
          {validationAutomationEnabled && !validationNextRun && !nextRunsLoading && (
            <div className="mt-2">
              <button
                onClick={handleTriggerManualRun}
                className="text-xs text-primary-600 hover:text-primary-700 flex items-center"
                title="Trigger validation automation now"
              >
                <PlayIcon className="h-3 w-3 mr-1" />
                Run Now
              </button>
            </div>
          )}

          {/* Loading state for next runs */}
          {validationAutomationEnabled && nextRunsLoading && (
            <div className="mt-2 flex items-center text-xs text-secondary-500">
              <LoadingSpinner size="xs" className="mr-2" />
              Loading schedule...
            </div>
          )}

          {/* Error state */}
          {nextRunsError && !circuitBreakerOpen && (
            <div className="mt-2 p-2 bg-warning-50 border border-warning-200 rounded text-xs">
              <div className="flex items-center">
                <ExclamationTriangleIcon className="h-3 w-3 text-warning-600 mr-1" />
                <span className="text-warning-800">Unable to load schedule information</span>
              </div>
              {consecutiveErrors > 0 && (
                <div className="text-warning-600 mt-1">
                  {consecutiveErrors} consecutive error{consecutiveErrors !== 1 ? 's' : ''}
                </div>
              )}
            </div>
          )}

          {/* Circuit breaker message */}
          {circuitBreakerOpen && (
            <div className="mt-2 p-2 bg-danger-50 border border-danger-200 rounded text-xs">
              <div className="flex items-center">
                <ExclamationTriangleIcon className="h-3 w-3 text-danger-600 mr-1" />
                <span className="text-danger-800">Schedule loading paused due to errors</span>
              </div>
            </div>
          )}
        </div>

        {/* Show message if no automation is enabled */}
        {!hasAnyAutomation && (
          <div className="mt-2 p-2 bg-secondary-50 border border-secondary-200 rounded text-xs">
            <div className="flex items-start">
              <ExclamationCircleIcon className="h-3 w-3 text-secondary-400 mr-1 mt-0.5 flex-shrink-0" />
              <div>
                <p className="text-secondary-800">
                  No automation is configured for this connection.
                </p>
                <Link
                  to={`/settings/automation?connection=${connectionId}`}
                  className="text-secondary-600 hover:text-secondary-700 underline"
                >
                  Configure automation schedules →
                </Link>
              </div>
            </div>
          </div>
        )}

        {/* Last run information */}
        {validationNextRun?.last_run && (
          <div className="border-t pt-4">
            <div className="text-xs text-secondary-500">
              <div className="flex items-center justify-between">
                <div className="flex items-center">
                  <ClipboardDocumentCheckIcon className="h-3 w-3 mr-1" />
                  <span>
                    Last run: {new Date(validationNextRun.last_run).toLocaleDateString()}
                  </span>
                </div>
                <div className={`flex items-center text-xs ${
                  validationNextRun.last_run_status === 'completed'
                    ? 'text-accent-600' 
                    : 'text-danger-600'
                }`}>
                  {validationNextRun.last_run_status === 'completed' ? (
                    <CheckCircleIcon className="h-3 w-3 mr-1" />
                  ) : (
                    <ExclamationCircleIcon className="h-3 w-3 mr-1" />
                  )}
                  <span>
                    {validationNextRun.last_run_status || 'Unknown'}
                  </span>
                </div>
              </div>

              {/* Show execution time if available */}
              {validationNextRun.avg_duration_seconds && (
                <div className="mt-1 text-xs text-secondary-400">
                  Avg duration: {Math.round(validationNextRun.avg_duration_seconds)}s
                </div>
              )}
            </div>
          </div>
        )}

        {/* Help text */}
        {!tableName && (
          <div className="border-t pt-4">
            <div className="text-xs text-secondary-500">
              <p>Select a table to view table-specific automation settings.</p>
            </div>
          </div>
        )}

        {/* Debug info in development */}
        {process.env.NODE_ENV === 'development' && validationNextRun && (
          <div className="border-t pt-4">
            <details className="text-xs text-secondary-400">
              <summary className="cursor-pointer">Debug: Schedule Data</summary>
              <pre className="mt-2 p-2 bg-secondary-50 rounded overflow-auto">
                {JSON.stringify({ schedule: schedule?.validation_automation, nextRun: validationNextRun }, null, 2)}
              </pre>
            </details>
          </div>
        )}
      </div>
    </div>
  );
};

export default ValidationAutomationControls;