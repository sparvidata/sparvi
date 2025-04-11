import React, { useState } from 'react';
import {
  PencilIcon,
  TrashIcon,
  BugAntIcon,
  ExclamationCircleIcon,
  CheckCircleIcon,
  XCircleIcon,
  ArrowPathIcon,
  ClockIcon,
  TableCellsIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { formatDate } from '../../../utils/formatting';
import validationService from '../../../services/validationService';

const ValidationRuleList = ({
  validations = [],
  isLoading = false,
  onEdit,
  onDebug,
  onRefreshList,
  onUpdateValidations, // New prop for optimistic updates
  tableName,
  connectionId,
  onRunSingle,
  isRunningValidation = false,
  runningRuleId = null,
  showNotification
}) => {
  const [isDeleting, setIsDeleting] = useState(false);
  const [validationToDelete, setValidationToDelete] = useState(null);
  const [filterStatus, setFilterStatus] = useState('all');

  // Filter out inactive rules and then apply status filter
  const filteredValidations = validations
    .filter(validation => validation.is_active !== false) // Always filter out inactive rules
    .filter(validation => {
      if (filterStatus === 'all') return true;
      if (filterStatus === 'passed') return validation.last_result === true;
      if (filterStatus === 'failed') return validation.last_result === false;
      if (filterStatus === 'error') return !!validation.error;
      if (filterStatus === 'notrun') return validation.last_result === undefined || validation.last_result === null;
      return true;
    });

  // Confirm deletion
  const confirmDelete = (validation) => {
    setValidationToDelete(validation);
    setIsDeleting(true);
  };

  // Deactivate Validation with optimistic UI update
  const handleDeactivateValidation = async () => {
    if (!validationToDelete || !tableName) return;

    try {
      // Store rule info for later use
      const ruleToDeactivate = { ...validationToDelete };

      // Close the modal immediately
      setIsDeleting(false);
      setValidationToDelete(null);

      // Optimistically remove the rule from the list by updating the parent's state
      if (typeof onUpdateValidations === 'function') {
        // Filter out the deactivated rule
        const updatedValidations = validations.filter(
          rule => (rule.id !== ruleToDeactivate.id) &&
                 (rule.rule_name !== ruleToDeactivate.rule_name)
        );

        // Update parent component's state
        onUpdateValidations(updatedValidations);
      }

      // Now make the actual API call
      await validationService.deactivateRule(
        connectionId,
        tableName,
        ruleToDeactivate.rule_name
      );

      showNotification(`Validation "${ruleToDeactivate.rule_name}" deactivated successfully`, 'success');

      // No need to refresh the list for successful case since we've already updated it optimistically
    } catch (error) {
      console.error('Error in deactivation operation:', error);
      showNotification(`Error deactivating rule: ${error.message}`, 'error');

      // Since optimistic update failed, refresh the list to get the correct state
      if (onRefreshList) {
        onRefreshList();
      }
    }
  };

  // Handle running a single validation
  const handleRunSingle = async (validation) => {
    try {
      if (onRunSingle) {
        await onRunSingle(validation);

        // Force refresh the list after running
        if (onRefreshList) {
          onRefreshList();
        }
      }
    } catch (error) {
      console.error('Error running validation:', error);
      showNotification(`Error running validation: ${error.message}`, 'error');
    }
  };

  // Format timestamp
  const formatTimestamp = (timestamp) => {
    if (!timestamp) return 'Never';
    return formatDate(timestamp, true);
  };

  // If loading, show loading state with the spinner overlay
  if (isLoading) {
    return (
      <div className="bg-white shadow rounded-lg relative min-h-[200px]">
        {/* Keep the filter UI */}
        <div className="p-2 bg-secondary-50 border-b border-secondary-200">
          <div className="flex items-center text-sm">
            <span className="mr-2 text-secondary-700">Filter:</span>
            <div className="flex space-x-1">
              <button
                onClick={() => setFilterStatus('all')}
                className="px-2 py-1 rounded bg-secondary-200 text-secondary-800"
              >
                All
              </button>
              <button
                onClick={() => setFilterStatus('passed')}
                className="px-2 py-1 rounded hover:bg-secondary-100"
              >
                <CheckCircleIcon className="inline h-4 w-4 mr-1" />
                Passed
              </button>
              <button
                onClick={() => setFilterStatus('failed')}
                className="px-2 py-1 rounded hover:bg-secondary-100"
              >
                <XCircleIcon className="inline h-4 w-4 mr-1" />
                Failed
              </button>
              <button
                onClick={() => setFilterStatus('error')}
                className="px-2 py-1 rounded hover:bg-secondary-100"
              >
                <ExclamationCircleIcon className="inline h-4 w-4 mr-1" />
                Errors
              </button>
              <button
                onClick={() => setFilterStatus('notrun')}
                className="px-2 py-1 rounded hover:bg-secondary-100"
              >
                Not Run
              </button>
            </div>
          </div>
        </div>

        {/* Loading overlay with spinner */}
        <div className="px-4 py-10 flex flex-col items-center justify-center">
          <LoadingSpinner size="lg" />
          <p className="mt-4 text-secondary-500">Loading validation rules for {tableName}...</p>
        </div>
      </div>
    );
  }

  // If no validations, show empty state
  if (!filteredValidations.length) {
    return (
      <div className="text-center py-10">
        <TableCellsIcon className="mx-auto h-12 w-12 text-secondary-400" />
        <h3 className="mt-2 text-sm font-medium text-secondary-900">No validation rules found</h3>
        <p className="mt-1 text-sm text-secondary-500">
          Create validation rules to ensure data quality for {tableName || 'this table'}.
        </p>
      </div>
    );
  }

  return (
    <div>
      {/* Filter UI */}
      <div className="p-2 bg-secondary-50 border-b border-secondary-200">
        <div className="flex items-center text-sm">
          <span className="mr-2 text-secondary-700">Filter:</span>
          <div className="flex space-x-1">
            <button
              onClick={() => setFilterStatus('all')}
              className={`px-2 py-1 rounded ${
                filterStatus === 'all' 
                  ? 'bg-secondary-200 text-secondary-800' 
                  : 'hover:bg-secondary-100'
              }`}
            >
              All
            </button>
            <button
              onClick={() => setFilterStatus('passed')}
              className={`px-2 py-1 rounded ${
                filterStatus === 'passed' 
                  ? 'bg-accent-100 text-accent-800' 
                  : 'hover:bg-secondary-100'
              }`}
            >
              <CheckCircleIcon className="inline h-4 w-4 mr-1" />
              Passed
            </button>
            <button
              onClick={() => setFilterStatus('failed')}
              className={`px-2 py-1 rounded ${
                filterStatus === 'failed' 
                  ? 'bg-danger-100 text-danger-800' 
                  : 'hover:bg-secondary-100'
              }`}
            >
              <XCircleIcon className="inline h-4 w-4 mr-1" />
              Failed
            </button>
            <button
              onClick={() => setFilterStatus('error')}
              className={`px-2 py-1 rounded ${
                filterStatus === 'error' 
                  ? 'bg-warning-100 text-warning-800' 
                  : 'hover:bg-secondary-100'
              }`}
            >
              <ExclamationCircleIcon className="inline h-4 w-4 mr-1" />
              Errors
            </button>
            <button
              onClick={() => setFilterStatus('notrun')}
              className={`px-2 py-1 rounded ${
                filterStatus === 'notrun' 
                  ? 'bg-secondary-100 text-secondary-800' 
                  : 'hover:bg-secondary-100'
              }`}
            >
              Not Run
            </button>
          </div>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-secondary-200">
          <thead className="bg-secondary-50">
            <tr>
              <th scope="col" className="py-3.5 pl-4 pr-3 text-left text-sm font-semibold text-secondary-900 sm:pl-6">
                Rule
              </th>
              <th scope="col" className="px-3 py-3.5 text-left text-sm font-semibold text-secondary-900">
                Status
              </th>
              <th scope="col" className="px-3 py-3.5 text-left text-sm font-semibold text-secondary-900">
                Last Run
              </th>
              <th scope="col" className="px-3 py-3.5 text-left text-sm font-semibold text-secondary-900">
                Result
              </th>
              <th scope="col" className="relative py-3.5 pl-3 pr-4 sm:pr-6">
                <span className="sr-only">Actions</span>
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-secondary-200 bg-white">
            {filteredValidations.map((validation) => (
              <tr key={validation.id || validation.rule_name} className="hover:bg-secondary-50">
                <td className="py-4 pl-4 pr-3 text-sm sm:pl-6">
                  <div className="font-medium text-secondary-900">
                    {validation.rule_name}
                  </div>
                  {validation.description && (
                    <div className="text-xs text-secondary-500">{validation.description}</div>
                  )}
                  <div className="mt-1 text-xs font-mono bg-secondary-50 p-2 rounded overflow-x-auto max-w-md">
                    {validation.query} {validation.operator} {validation.expected_value}
                  </div>
                </td>
                <td className="px-3 py-4 text-sm">
                  {validation.error ? (
                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-warning-100 text-warning-800">
                      <ExclamationCircleIcon className="-ml-0.5 mr-1 h-3 w-3" />
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
                      Not Run
                    </span>
                  )}
                </td>
                <td className="px-3 py-4 text-sm text-secondary-500">
                  {formatTimestamp(validation.last_run_at)}
                </td>
                <td className="px-3 py-4 text-sm">
                  {validation.actual_value !== undefined && validation.actual_value !== null ? (
                    <span className={validation.last_result === true ? 'text-accent-600' : 'text-danger-600 font-medium'}>
                      {validation.actual_value}
                    </span>
                  ) : '—'}

                  {/* Show error message if validation failed with error */}
                  {validation.error && (
                    <div className="mt-1 text-xs text-danger-600 bg-danger-50 p-1 rounded">
                      {validation.error}
                    </div>
                  )}

                  {/* Performance indicator */}
                  {validation.execution_time_ms && (
                    <div className="mt-1 text-xs text-secondary-500">
                      <ClockIcon className="inline-block h-3 w-3 mr-1" />
                      {validation.execution_time_ms}ms
                      {validation.execution_time_ms > 1000 && (
                        <span className="ml-1 text-warning-600">
                          (slow)
                        </span>
                      )}
                    </div>
                  )}
                </td>
                <td className="py-4 pl-3 pr-4 text-right text-sm font-medium sm:pr-6">
                  <div className="flex space-x-2 justify-end">
                    <button
                      type="button"
                      onClick={() => handleRunSingle(validation)}
                      disabled={isRunningValidation || runningRuleId === (validation.id || validation.rule_name)}
                      className="text-primary-600 hover:text-primary-900"
                      title="Run validation"
                    >
                      {runningRuleId === (validation.id || validation.rule_name) ? (
                        <LoadingSpinner size="sm" />
                      ) : (
                        <ArrowPathIcon className="h-5 w-5" />
                      )}
                    </button>

                    <button
                      type="button"
                      onClick={() => onEdit(validation)}
                      className="text-secondary-600 hover:text-secondary-900"
                      title="Edit validation"
                    >
                      <PencilIcon className="h-5 w-5" />
                    </button>

                    {onDebug && (
                      <button
                        type="button"
                        onClick={() => onDebug(validation)}
                        className="text-secondary-600 hover:text-primary-600"
                        title="Debug validation"
                      >
                        <BugAntIcon className="h-5 w-5" />
                      </button>
                    )}

                    <button
                      type="button"
                      onClick={() => confirmDelete(validation)}
                      className="text-secondary-600 hover:text-danger-600"
                      title="Delete validation"
                    >
                      <TrashIcon className="h-5 w-5" />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Delete confirmation modal */}
      {isDeleting && validationToDelete && (
        <div className="fixed z-10 inset-0 overflow-y-auto">
          <div className="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
            <div className="fixed inset-0 transition-opacity" aria-hidden="true">
              <div className="absolute inset-0 bg-secondary-500 opacity-75"></div>
            </div>

            <span className="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>

            <div className="inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full">
              <div className="bg-white px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
                <div className="sm:flex sm:items-start">
                  <div className="mx-auto flex-shrink-0 flex items-center justify-center h-12 w-12 rounded-full bg-danger-100 sm:mx-0 sm:h-10 sm:w-10">
                    <ExclamationCircleIcon className="h-6 w-6 text-danger-600" aria-hidden="true" />
                  </div>
                  <div className="mt-3 text-center sm:mt-0 sm:ml-4 sm:text-left">
                    <h3 className="text-lg leading-6 font-medium text-secondary-900">Deactivate validation rule</h3>
                    <div className="mt-2">
                      <p className="text-sm text-secondary-500">
                        Are you sure you want to deactivate the validation rule "{validationToDelete.rule_name}"?
                        The rule will no longer be executed, but historical results will be preserved.
                      </p>
                    </div>
                  </div>
                </div>
              </div>
              <div className="bg-secondary-50 px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse">
                <button
                  type="button"
                  className="inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-red-600 text-sm font-medium text-red-50 hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500 sm:ml-3 sm:w-auto"
                  onClick={handleDeactivateValidation}
                >
                  Deactivate
                </button>
                <button
                  type="button"
                  className="mt-3 inline-flex justify-center rounded-md border border-secondary-300 shadow-sm px-4 py-2 bg-white text-sm font-medium text-secondary-700 hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 sm:mt-0 sm:ml-3 sm:w-auto"
                  onClick={() => {
                    setIsDeleting(false);
                    setValidationToDelete(null);
                  }}
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ValidationRuleList;