import React, { useState } from 'react';
import {
  CheckCircleIcon,
  XCircleIcon,
  PencilIcon,
  TrashIcon,
  ArrowPathIcon,
  ExclamationCircleIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { validationsAPI } from '../../../api/apiService';
import { useUI } from '../../../contexts/UIContext';

const ValidationRuleList = ({
  validations = [],
  isLoading,
  onEdit,
  onRefreshList,
  tableName,
  connectionId
}) => {
  const { showNotification } = useUI();

  const [runningValidation, setRunningValidation] = useState(null);
  const [isDeleting, setIsDeleting] = useState(false);
  const [validationToDelete, setValidationToDelete] = useState(null);

  // Run a single validation
  const handleRunValidation = async (validation) => {
    if (!connectionId || !tableName) return;

    try {
      setRunningValidation(validation.id);

      // Build request with only the selected validation
      const response = await validationsAPI.runValidations(
        connectionId,
        tableName,
        null
      );

      // Find the result for this specific validation
      const result = response.data.results?.find((_, index) =>
        validations[index]?.id === validation.id
      );

      // Show notification based on result
      if (result) {
        if (result.is_valid) {
          showNotification(`Validation "${validation.rule_name}" passed`, 'success');
        } else {
          showNotification(`Validation "${validation.rule_name}" failed`, 'error');
        }
      } else {
        showNotification(`No result found for validation "${validation.rule_name}"`, 'warning');
      }

      // Refresh the list to show updated results
      if (onRefreshList) onRefreshList();
    } catch (error) {
      console.error(`Error running validation ${validation.rule_name}:`, error);
      showNotification(`Failed to run validation: ${error.message}`, 'error');
    } finally {
      setRunningValidation(null);
    }
  };

  // Confirm deletion
  const confirmDelete = (validation) => {
    setValidationToDelete(validation);
    setIsDeleting(true);
  };

  // Delete validation
  const handleDeleteValidation = async () => {
    if (!validationToDelete || !tableName) return;

    try {
      await validationsAPI.deleteRule(tableName, validationToDelete.rule_name);

      showNotification(`Validation "${validationToDelete.rule_name}" deleted`, 'success');

      // Refresh the list
      if (onRefreshList) onRefreshList();

      // Close the modal
      setIsDeleting(false);
      setValidationToDelete(null);
    } catch (error) {
      console.error(`Error deleting validation ${validationToDelete.rule_name}:`, error);
      showNotification(`Failed to delete validation: ${error.message}`, 'error');
    }
  };

  // If loading with no validations, show loading state
  if (isLoading && !validations.length) {
    return (
      <div className="flex justify-center py-10">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  // If no validations, show empty state
  if (!validations.length) {
    return (
      <div className="text-center py-10">
        <ExclamationCircleIcon className="mx-auto h-12 w-12 text-secondary-400" />
        <h3 className="mt-2 text-sm font-medium text-secondary-900">No validation rules found</h3>
        <p className="mt-1 text-sm text-secondary-500">
          Create validation rules to ensure data quality.
        </p>
      </div>
    );
  }

  return (
    <div>
      <ul className="divide-y divide-secondary-200">
        {validations.map((validation) => (
          <li key={validation.id} className="py-4 px-6 hover:bg-secondary-50">
            <div className="flex items-center justify-between">
              <div className="flex items-center flex-1 min-w-0">
                {validation.last_result === true && (
                  <CheckCircleIcon className="h-5 w-5 text-accent-500 mr-2" aria-hidden="true" />
                )}
                {validation.last_result === false && (
                  <XCircleIcon className="h-5 w-5 text-danger-500 mr-2" aria-hidden="true" />
                )}
                {(validation.last_result === null || validation.last_result === undefined) && (
                  <div className="h-5 w-5 mr-2" />
                )}

                <div>
                  <h4 className="text-sm font-medium text-secondary-900">{validation.rule_name}</h4>
                  {validation.description && (
                    <p className="text-sm text-secondary-500 truncate">{validation.description}</p>
                  )}
                </div>
              </div>

              <div className="flex items-center space-x-2">
                {validation.last_result !== undefined && validation.last_result !== null && (
                  <span className={`px-2.5 py-0.5 rounded-full text-xs font-medium ${
                    validation.last_result === true 
                      ? 'bg-accent-100 text-accent-800' 
                      : 'bg-danger-100 text-danger-800'
                  }`}>
                    {validation.last_result === true ? 'Passed' : 'Failed'}
                  </span>
                )}

                <button
                  type="button"
                  onClick={() => handleRunValidation(validation)}
                  disabled={runningValidation === validation.id}
                  className="p-1 text-secondary-400 hover:text-secondary-500 focus:outline-none"
                  title="Run validation"
                >
                  {runningValidation === validation.id ? (
                    <LoadingSpinner size="sm" />
                  ) : (
                    <ArrowPathIcon className="h-5 w-5" aria-hidden="true" />
                  )}
                </button>

                <button
                  type="button"
                  onClick={() => onEdit(validation)}
                  className="p-1 text-secondary-400 hover:text-secondary-500 focus:outline-none"
                  title="Edit validation"
                >
                  <PencilIcon className="h-5 w-5" aria-hidden="true" />
                </button>

                <button
                  type="button"
                  onClick={() => confirmDelete(validation)}
                  className="p-1 text-secondary-400 hover:text-danger-500 focus:outline-none"
                  title="Delete validation"
                >
                  <TrashIcon className="h-5 w-5" aria-hidden="true" />
                </button>
              </div>
            </div>

            <div className="mt-2 text-xs text-secondary-500 font-mono bg-secondary-50 p-2 rounded">
              {validation.query} {validation.operator} {validation.expected_value}
            </div>

            {validation.last_run_at && (
              <div className="mt-2 text-xs text-secondary-500">
                Last run: {new Date(validation.last_run_at).toLocaleString()}
                {validation.actual_value !== undefined && (
                  <span className="ml-2">
                    Actual value: <span className="font-medium">{validation.actual_value}</span>
                  </span>
                )}
              </div>
            )}
          </li>
        ))}
      </ul>

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
                    <h3 className="text-lg leading-6 font-medium text-secondary-900">Delete validation rule</h3>
                    <div className="mt-2">
                      <p className="text-sm text-secondary-500">
                        Are you sure you want to delete the validation rule "{validationToDelete.rule_name}"? This action cannot be undone.
                      </p>
                    </div>
                  </div>
                </div>
              </div>
              <div className="bg-secondary-50 px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse">
                <button
                  type="button"
                  className="w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-danger-600 text-base font-medium text-white hover:bg-danger-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-danger-500 sm:ml-3 sm:w-auto sm:text-sm"
                  onClick={handleDeleteValidation}
                >
                  Delete
                </button>
                <button
                  type="button"
                  className="mt-3 w-full inline-flex justify-center rounded-md border border-secondary-300 shadow-sm px-4 py-2 bg-white text-base font-medium text-secondary-700 hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm"
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