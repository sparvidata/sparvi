// src/pages/Validations/components/ValidationHistorySummary.js
// This component displays a summary of the latest validation run results
import React from 'react';
import {
  ClipboardDocumentCheckIcon,
  CalendarIcon,
  CheckCircleIcon,
  XCircleIcon,
  ExclamationTriangleIcon,
  ArrowPathIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

/**
 * Component to display the latest validation history summary
 */
const ValidationHistorySummary = ({
  validations = [],
  lastRunTimestamp,
  onRunAll,
  isRunning = false,
  connectionId,
  tableName
}) => {
  // Calculate summary statistics
  const totalValidations = validations.length;
  const passedValidations = validations.filter(v => v.last_result === true).length;
  const failedValidations = validations.filter(v => v.last_result === false).length;
  const notRunValidations = validations.filter(v => v.last_result === undefined || v.last_result === null).length;

  // Determine if there are any errors
  const hasErrors = validations.some(v => v.error);
  const errorCount = validations.filter(v => v.error).length;

  // Format timestamp
  const formatTimestamp = (timestamp) => {
    if (!timestamp) return 'Never';

    try {
      const date = new Date(timestamp);
      return date.toLocaleString(undefined, {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
      });
    } catch (e) {
      console.error('Error formatting timestamp:', e);
      return 'Invalid date';
    }
  };

  // Get health status color
  const getHealthStatusColor = () => {
    if (totalValidations === 0) return 'secondary';
    if (notRunValidations === totalValidations) return 'secondary';
    if (hasErrors) return 'warning';
    if (failedValidations > 0) return 'danger';
    return 'accent';
  };

  // Get health status message
  const getHealthStatusMessage = () => {
    if (totalValidations === 0) return 'No validations defined';
    if (notRunValidations === totalValidations) return 'Validations not run yet';
    if (hasErrors) return `${errorCount} validation ${errorCount === 1 ? 'error' : 'errors'} detected`;
    if (failedValidations > 0) return `${failedValidations} of ${totalValidations} validations failing`;
    return 'All validations passing';
  };

  const healthStatus = getHealthStatusColor();

  return (
    <div className="bg-white shadow rounded-lg mb-6">
      <div className="px-4 py-5 sm:p-6">
        <div className="flex items-start flex-wrap sm:flex-nowrap">
          {/* Health Status Indicator */}
          <div className={`flex-shrink-0 rounded-full p-3 bg-${healthStatus}-100 mr-4 mb-4 sm:mb-0`}>
            {healthStatus === 'accent' && <CheckCircleIcon className={`h-6 w-6 text-${healthStatus}-600`} />}
            {healthStatus === 'danger' && <XCircleIcon className={`h-6 w-6 text-${healthStatus}-600`} />}
            {healthStatus === 'warning' && <ExclamationTriangleIcon className={`h-6 w-6 text-${healthStatus}-600`} />}
            {healthStatus === 'secondary' && <ClipboardDocumentCheckIcon className={`h-6 w-6 text-${healthStatus}-600`} />}
          </div>

          {/* Summary Information */}
          <div className="flex-grow">
            <h3 className="text-lg font-medium text-secondary-900">Validation Summary</h3>
            <p className={`mt-1 text-sm text-${healthStatus}-600 font-medium`}>
              {getHealthStatusMessage()}
            </p>

            <div className="mt-3 flex flex-wrap items-center text-sm text-secondary-600">
              <div className="mr-6 mb-2">
                <span className="font-medium text-secondary-900">{totalValidations}</span> total validations
              </div>

              <div className="mr-6 mb-2">
                <span className="font-medium text-accent-600">{passedValidations}</span> passing
              </div>

              <div className="mr-6 mb-2">
                <span className="font-medium text-danger-600">{failedValidations}</span> failing
              </div>

              <div className="mr-6 mb-2">
                <span className="font-medium text-secondary-700">{notRunValidations}</span> not run
              </div>

              {hasErrors && (
                <div className="mr-6 mb-2">
                  <span className="font-medium text-warning-600">{errorCount}</span> with errors
                </div>
              )}

              <div className="flex items-center mb-2">
                <CalendarIcon className="mr-1 h-4 w-4 text-secondary-500" />
                <span>Last run: {formatTimestamp(lastRunTimestamp)}</span>
              </div>
            </div>
          </div>

          {/* Action Button */}
          <div className="flex-shrink-0 self-center mt-4 w-full sm:w-auto sm:mt-0 sm:ml-4">
            <button
              type="button"
              onClick={onRunAll}
              disabled={isRunning || !connectionId || !tableName}
              className="w-full flex justify-center items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
            >
              {isRunning ? (
                <>
                  <LoadingSpinner size="sm" className="mr-2" />
                  Running Validations...
                </>
              ) : (
                <>
                  <ArrowPathIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
                  Run All Validations
                </>
              )}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ValidationHistorySummary;