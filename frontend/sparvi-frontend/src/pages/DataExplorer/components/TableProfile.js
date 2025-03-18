import React from 'react';
import {
  InformationCircleIcon,
  ExclamationCircleIcon,
  CheckCircleIcon,
  XCircleIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { formatNumber, formatPercentage } from '../../../utils/formatting';

const TableProfile = ({ profile, isLoading, tableName }) => {
  // If no profile data and not loading, show empty state
  if (!profile && !isLoading) {
    return (
      <div className="text-center py-10">
        <ExclamationCircleIcon className="mx-auto h-10 w-10 text-secondary-400" />
        <h3 className="mt-2 text-sm font-medium text-secondary-900">No profile data available</h3>
        <p className="mt-1 text-sm text-secondary-500">
          This table hasn't been profiled yet. Click the "Refresh Profile" button to generate profile data.
        </p>
      </div>
    );
  }

  // Show loading state when refreshing profile
  if (isLoading && !profile) {
    return (
      <div className="text-center py-16">
        <LoadingSpinner size="lg" className="mx-auto" />
        <p className="mt-4 text-sm text-secondary-500">Generating profile for {tableName}...</p>
      </div>
    );
  }

  // Extract relevant profile data
  const rowCount = profile?.row_count;
  const columnCount = profile?.column_count;
  const nullFractionByColumn = profile?.null_fractions || {};
  const distinctCountByColumn = profile?.distinct_counts || {};
  const summaryStatistics = profile?.summary_statistics || {};
  const distributionData = profile?.distribution_data || {};
  const validationResults = profile?.validation_results || [];

  // Get overall validation status
  const passedValidations = validationResults.filter(r => r.is_valid).length;
  const failedValidations = validationResults.filter(r => !r.is_valid).length;
  const validationStatus = failedValidations > 0 ? 'error' :
                          passedValidations > 0 ? 'success' : 'neutral';

  return (
    <div>
      {/* Show a refreshing indicator if we're updating the profile */}
      {isLoading && profile && (
        <div className="bg-primary-50 p-4 rounded-md mb-6 flex items-center">
          <LoadingSpinner size="sm" className="mr-3" />
          <p className="text-sm text-primary-700">
            Refreshing profile data...
          </p>
        </div>
      )}

      {/* Profile summary */}
      <div className="mb-6">
        <h3 className="text-lg font-medium text-secondary-900 mb-4">Profile Summary</h3>

        <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4">
          {/* Basic stats */}
          <div className="bg-white overflow-hidden shadow rounded-lg">
            <div className="px-4 py-5 sm:p-6">
              <div className="flex items-center">
                <div className="flex-shrink-0 bg-primary-100 rounded-md p-3">
                  <InformationCircleIcon className="h-6 w-6 text-primary-600" aria-hidden="true" />
                </div>
                <div className="ml-5 w-0 flex-1">
                  <dl>
                    <dt className="text-sm font-medium text-secondary-500 truncate">Row Count</dt>
                    <dd>
                      <div className="text-lg font-medium text-secondary-900">{formatNumber(rowCount)}</div>
                    </dd>
                  </dl>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-white overflow-hidden shadow rounded-lg">
            <div className="px-4 py-5 sm:p-6">
              <div className="flex items-center">
                <div className="flex-shrink-0 bg-primary-100 rounded-md p-3">
                  <InformationCircleIcon className="h-6 w-6 text-primary-600" aria-hidden="true" />
                </div>
                <div className="ml-5 w-0 flex-1">
                  <dl>
                    <dt className="text-sm font-medium text-secondary-500 truncate">Column Count</dt>
                    <dd>
                      <div className="text-lg font-medium text-secondary-900">{formatNumber(columnCount)}</div>
                    </dd>
                  </dl>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-white overflow-hidden shadow rounded-lg">
            <div className="px-4 py-5 sm:p-6">
              <div className="flex items-center">
                <div className="flex-shrink-0 bg-primary-100 rounded-md p-3">
                  <InformationCircleIcon className="h-6 w-6 text-primary-600" aria-hidden="true" />
                </div>
                <div className="ml-5 w-0 flex-1">
                  <dl>
                    <dt className="text-sm font-medium text-secondary-500 truncate">Profile Timestamp</dt>
                    <dd>
                      <div className="text-sm font-medium text-secondary-900">
                        {profile?.timestamp ? new Date(profile.timestamp).toLocaleString() : 'Unknown'}
                      </div>
                    </dd>
                  </dl>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-white overflow-hidden shadow rounded-lg">
            <div className="px-4 py-5 sm:p-6">
              <div className="flex items-center">
                <div className={`flex-shrink-0 rounded-md p-3 ${
                  validationStatus === 'error' ? 'bg-danger-100' : 
                  validationStatus === 'success' ? 'bg-accent-100' : 'bg-secondary-100'
                }`}>
                  {validationStatus === 'error' ? (
                    <XCircleIcon className="h-6 w-6 text-danger-600" aria-hidden="true" />
                  ) : validationStatus === 'success' ? (
                    <CheckCircleIcon className="h-6 w-6 text-accent-600" aria-hidden="true" />
                  ) : (
                    <InformationCircleIcon className="h-6 w-6 text-secondary-600" aria-hidden="true" />
                  )}
                </div>
                <div className="ml-5 w-0 flex-1">
                  <dl>
                    <dt className="text-sm font-medium text-secondary-500 truncate">Validations</dt>
                    <dd>
                      <div className="text-lg font-medium text-secondary-900">
                        {validationResults.length > 0 ? (
                          <span>
                            <span className="text-accent-600">{passedValidations}</span>
                            {' / '}
                            <span className="text-danger-600">{failedValidations}</span>
                          </span>
                        ) : (
                          'No validations'
                        )}
                      </div>
                    </dd>
                  </dl>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Validation results */}
      {validationResults.length > 0 && (
        <div className="mb-6">
          <h3 className="text-lg font-medium text-secondary-900 mb-4">Validation Results</h3>
          <div className="bg-white shadow overflow-hidden sm:rounded-md">
            <ul className="divide-y divide-secondary-200">
              {validationResults.map((validation, index) => (
                <li key={index}>
                  <div className="px-4 py-4 sm:px-6">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center">
                        {validation.is_valid ? (
                          <CheckCircleIcon className="h-5 w-5 text-accent-500" />
                        ) : (
                          <XCircleIcon className="h-5 w-5 text-danger-500" />
                        )}
                        <p className="ml-2 text-sm font-medium text-secondary-900">
                          {validation.name}
                        </p>
                      </div>
                      <div className="ml-2 flex-shrink-0 flex">
                        <p className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                          validation.is_valid 
                            ? 'bg-accent-100 text-accent-800' 
                            : 'bg-danger-100 text-danger-800'
                        }`}>
                          {validation.is_valid ? 'Passed' : 'Failed'}
                        </p>
                      </div>
                    </div>
                    <div className="mt-2 sm:flex sm:justify-between">
                      <div className="sm:flex">
                        <p className="flex items-center text-sm text-secondary-500">
                          {validation.operator} {validation.expected_value}
                        </p>
                      </div>
                      {validation.actual_value !== undefined && (
                        <div className="mt-2 flex items-center text-sm text-secondary-500 sm:mt-0">
                          <p>
                            Actual: <span className="font-medium">{validation.actual_value}</span>
                          </p>
                        </div>
                      )}
                    </div>
                  </div>
                </li>
              ))}
            </ul>
          </div>
        </div>
      )}

      {/* Column statistics */}
      <div>
        <h3 className="text-lg font-medium text-secondary-900 mb-4">Column Statistics</h3>
        <div className="shadow overflow-hidden border-b border-secondary-200 sm:rounded-lg">
          <table className="min-w-full divide-y divide-secondary-200">
            <thead className="bg-secondary-50">
              <tr>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                  Column
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                  Type
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                  Non-Null
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                  Distinct
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                  Min
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                  Max
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-secondary-200">
              {profile?.columns?.map((column, index) => {
                const nullFraction = nullFractionByColumn[column] || 0;
                const distinctCount = distinctCountByColumn[column] || 0;
                const stats = summaryStatistics[column] || {};

                return (
                  <tr key={column} className={index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-secondary-900">
                      {column}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                      {profile?.column_types?.[column] || 'unknown'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                      {formatPercentage(100 - (nullFraction * 100))}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                      {formatNumber(distinctCount)}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                      {stats.min !== undefined ? stats.min : '-'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                      {stats.max !== undefined ? stats.max : '-'}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default TableProfile;