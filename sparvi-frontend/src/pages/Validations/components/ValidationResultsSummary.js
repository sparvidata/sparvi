import React from 'react';
import {
  ClipboardDocumentCheckIcon,
  CheckCircleIcon,
  XCircleIcon,
  ExclamationTriangleIcon,
  ArrowPathIcon,
  CalendarIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { formatDate } from '../../../utils/formatting';

const ValidationResultsSummary = ({
  metrics,
  onRunAll,
  isRunning = false,
  connectionId,
  tableName
}) => {
  if (!metrics) {
    return (
      <div className="bg-white p-4 rounded-lg shadow flex justify-center items-center h-24">
        <LoadingSpinner size="md" />
      </div>
    );
  }

  // Get health status color
  const getHealthStatusColor = () => {
    if (metrics.total === 0) return 'secondary';
    if (metrics.notRun === metrics.total) return 'secondary';
    if (metrics.errored > 0) return 'warning';
    if (metrics.failed > 0) return 'danger';
    return 'accent';
  };

  // Get health status message
  const getHealthStatusMessage = () => {
    if (metrics.total === 0) return 'No validations defined';
    if (metrics.notRun + metrics.errored === metrics.total) return 'Validations not run yet';
    if (metrics.errored > 0) return `${metrics.errored} validation ${metrics.errored === 1 ? 'error' : 'errors'} detected`;
    if (metrics.failed > 0) return `${metrics.failed} of ${metrics.total} validations failing`;
    return 'All validations passing';
  };

  const healthStatus = getHealthStatusColor();

  return (
    <div className="bg-white px-4 py-5 border-b border-secondary-200 rounded-lg shadow">
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
              <span className="font-medium text-secondary-900">{metrics.total}</span> total validations
            </div>

            <div className="mr-6 mb-2">
              <span className="font-medium text-accent-600">{metrics.passed}</span> passing
            </div>

            <div className="mr-6 mb-2">
              <span className="font-medium text-danger-600">{metrics.failed}</span> failing
            </div>

            <div className="mr-6 mb-2">
              <span className="font-medium text-secondary-700">{metrics.notRun}</span> not run
            </div>

            {metrics.errored > 0 && (
              <div className="mr-6 mb-2">
                <span className="font-medium text-warning-600">{metrics.errored}</span> with errors
              </div>
            )}

            <div className="flex items-center mb-2">
              <CalendarIcon className="mr-1 h-4 w-4 text-secondary-500" />
              <span>Last run: {metrics.lastRunAt ? formatDate(metrics.lastRunAt, true) : 'Never'}</span>
            </div>
          </div>
        </div>

        {/* Action Button */}
        <div className="flex-shrink-0 self-start mt-4 w-full sm:w-auto sm:mt-0 sm:ml-4">
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
  );
};

export default ValidationResultsSummary;