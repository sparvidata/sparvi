import React, { useState } from 'react';
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
 * with mutually exclusive categories for statistics
 */
const ValidationHistorySummary = ({
  validations = [],
  lastRunTimestamp,
  onRunAll,
  isRunning = false,
  connectionId,
  tableName
}) => {
  const [isExpanded, setIsExpanded] = useState(false);

  // Calculate summary statistics with mutually exclusive categories
  const totalValidations = validations.length;

  // First, count validations with errors
  const errorsValidations = validations.filter(v => v.error).length;

  // Then count passed, failed, and not run (excluding those with errors)
  const passedValidations = validations.filter(v => v.last_result === true && !v.error).length;
  const failedValidations = validations.filter(v => v.last_result === false && !v.error).length;
  const notRunValidations = validations.filter(v =>
    (v.last_result === undefined || v.last_result === null) && !v.error
  ).length;

  // Sanity check - these should now add up to the total
  const calculatedTotal = passedValidations + failedValidations + notRunValidations + errorsValidations;
  if (calculatedTotal !== totalValidations) {
    console.warn(
      `Validation count mismatch: total=${totalValidations}, ` +
      `calculated=${calculatedTotal} (passed=${passedValidations}, ` +
      `failed=${failedValidations}, not run=${notRunValidations}, ` +
      `errors=${errorsValidations})`
    );
  }

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
    if (errorsValidations > 0) return 'warning';
    if (failedValidations > 0) return 'danger';
    return 'accent';
  };

  // Get health status message
  const getHealthStatusMessage = () => {
    if (totalValidations === 0) return 'No validations defined';
    if (notRunValidations + errorsValidations === totalValidations) return 'Validations not run yet';
    if (errorsValidations > 0) return `${errorsValidations} validation ${errorsValidations === 1 ? 'error' : 'errors'} detected`;
    if (failedValidations > 0) return `${failedValidations} of ${totalValidations} validations failing`;
    return 'All validations passing';
  };

  const healthStatus = getHealthStatusColor();

  return (
    <div className="bg-white px-4 py-5 border-b border-secondary-200">
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

            {errorsValidations > 0 && (
              <div className="mr-6 mb-2">
                <span className="font-medium text-warning-600">{errorsValidations}</span> with errors
              </div>
            )}

            <div className="flex items-center mb-2">
              <CalendarIcon className="mr-1 h-4 w-4 text-secondary-500" />
              <span>Last run: {formatTimestamp(lastRunTimestamp)}</span>
            </div>
          </div>
        </div>

        {/* Action Buttons */}
        <div className="flex-shrink-0 self-start mt-4 w-full sm:w-auto sm:mt-0 sm:ml-4 space-y-2">
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

          <button
            type="button"
            onClick={() => setIsExpanded(!isExpanded)}
            className="w-full flex justify-center items-center px-4 py-2 border border-secondary-300 text-sm font-medium rounded-md shadow-sm text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-secondary-500"
          >
            {isExpanded ? 'Hide Results' : 'Show Results'}
          </button>
        </div>
      </div>

      {/* Results table moved outside the flex container for full width */}
      {isExpanded && validations.length > 0 && (
        <div className="mt-4 space-y-2 pt-3 border-t border-secondary-200 w-full">
          <h4 className="text-sm font-medium text-secondary-900 mb-2">Latest Results</h4>
          <div className="max-h-64 overflow-y-auto w-full">
            <table className="w-full table-fixed divide-y divide-secondary-200">
              <thead className="bg-secondary-50">
                <tr>
                  <th scope="col" className="px-3 py-2 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider w-1/3">
                    Rule
                  </th>
                  <th scope="col" className="px-3 py-2 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider w-1/6">
                    Status
                  </th>
                  <th scope="col" className="px-3 py-2 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider w-1/4">
                    Value
                  </th>
                  <th scope="col" className="px-3 py-2 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider w-1/4">
                    Time
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-secondary-200">
                {validations.map((validation, index) => (
                  <tr key={validation.id || index} className={index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
                    <td className="px-3 py-2 whitespace-nowrap text-sm font-medium text-secondary-900 truncate">
                      {validation.rule_name}
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap text-sm">
                      {validation.error ? (
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-warning-100 text-warning-800">
                          <ExclamationTriangleIcon className="-ml-0.5 mr-1 h-3 w-3" />
                          Error
                        </span>
                      ) : validation.last_result === true ? (
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-accent-100 text-accent-800">
                          <CheckCircleIcon className="-ml-0.5 mr-1 h-3 w-3" />
                          Passed
                        </span>
                      ) : validation.last_result === false ? (
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-danger-100 text-danger-800">
                          <XCircleIcon className="-ml-0.5 mr-1 h-3 w-3" />
                          Failed
                        </span>
                      ) : (
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-secondary-100 text-secondary-800">
                          Not run
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap text-sm text-secondary-500">
                      {validation.actual_value !== undefined && validation.actual_value !== null ?
                          <span className={validation.last_result === true ? 'text-accent-600' : 'text-danger-600'}>
                          {validation.actual_value}
                        </span>
                       : '—'}
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap text-xs text-secondary-500">
                      {validation.last_run_at ?
                        formatTimestamp(validation.last_run_at) :
                        '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
};

export default ValidationHistorySummary;