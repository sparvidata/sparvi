import React, {useEffect, useRef, useState} from 'react';
import {
  CheckCircleIcon,
  XCircleIcon,
  PencilIcon,
  TrashIcon,
  ArrowPathIcon,
  ExclamationCircleIcon,
  BugAntIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { validationsAPI } from '../../../api/enhancedApiService';
import { useUI } from '../../../contexts/UIContext';
import ValidationErrorHandler from './ValidationErrorHandler';

const ValidationRuleList = ({
  validations = [],
  isLoading,
  onEdit,
  onRefreshList,
  tableName,
  connectionId,
  onDebug,
  onUpdate // Added this prop
}) => {
  const { showNotification } = useUI();

  const [runningValidation, setRunningValidation] = useState(null);
  const [isDeleting, setIsDeleting] = useState(false);
  const [validationToDelete, setValidationToDelete] = useState(null);
  const [validationErrors, setValidationErrors] = useState(null);
  const [runningAll, setRunningAll] = useState(false);
  const componentIsMounted = useRef(true);

  // Add useEffect for cleanup
  useEffect(() => {
    // Set mounted flag
    componentIsMounted.current = true;

    // Cleanup function
    return () => {
      componentIsMounted.current = false;
    };
  }, []);

  // Run a single validation
  const handleRunValidation = async (validation) => {
    if (!connectionId || !tableName) return;

    try {
      // Set running state once
      setRunningValidation(validation.id);

      // Clear errors once - reducing state updates
      setValidationErrors(null);

      // Run the validation
      const response = await validationsAPI.runValidations(
        connectionId,  // Pass connectionId first
        tableName,
        null
      );

      // Process results only if component is still mounted (use a ref for this)
      if (!componentIsMounted.current) return;

      console.log("Single validation response:", response);

      // Check if response contains meaningful results
      if (response?.results && Array.isArray(response.results)) {
        // Find the result that matches our validation name
        const result = response.results.find(r => r.rule_name === validation.rule_name);

        // Process the result
        if (result) {
          // Update this validation with its result
          const updatedValidation = {
            ...validation,
            last_result: result.error ? null : result.is_valid,
            actual_value: result.actual_value,
            error: result.error,
            last_run_at: new Date().toISOString()
          };

          // Update the validations list with this updated validation - in a single state update
          if (typeof onUpdate === 'function') {
            const updatedValidations = validations.map(v =>
              v.id === validation.id ? updatedValidation : v
            );
            onUpdate(updatedValidations);
          }

          // Show appropriate notification
          if (result.error) {
            const errors = [{
              name: validation.rule_name,
              error: result.error,
              is_valid: false
            }];

            // Batch state updates
            setValidationErrors(errors);
            showNotification(`Error in validation "${validation.rule_name}"`, 'error');
          } else {
            if (result.is_valid) {
              showNotification(`Validation "${validation.rule_name}" passed`, 'success');
            } else {
              const message = `Validation "${validation.rule_name}" failed - Expected: ${validation.expected_value}, Actual: ${result.actual_value}`;
              showNotification(message, 'warning');
            }
          }
        } else {
          showNotification(`No result found for validation "${validation.rule_name}"`, 'warning');
        }
      } else {
        showNotification(`Unexpected response format from server`, 'error');
      }

      // Refresh the list only once after we're done - avoiding excessive refetches
      if (onRefreshList) {
        onRefreshList();
      }
    } catch (error) {
      if (!componentIsMounted.current) return;

      console.error(`Error running validation ${validation.rule_name}:`, error);
      showNotification(`Failed to run validation: ${error.message}`, 'error');
    } finally {
      if (componentIsMounted.current) {
        setRunningValidation(null);
      }
    }
  };


  // Run all validations
  const handleRunAllValidations = async () => {
    if (!connectionId || !tableName) return;

    try {
      setRunningAll(true);
      setValidationErrors(null);

      const response = await validationsAPI.runValidations(
        connectionId,  // Pass connectionId first
        tableName,
        null
      );

      console.log("Validation results response:", response);

      // Check if response contains meaningful results
      if (response && response.results && Array.isArray(response.results)) {
        // Extract validation results and any errors
        const errors = [];

        // Create a map of results by rule_name for easier access
        const resultsByRuleName = {};
        response.results.forEach(result => {
          if (result.rule_name) {
            resultsByRuleName[result.rule_name] = result;
          }

          if (result.error) {
            errors.push({
              name: result.rule_name || "Unknown validation",
              error: result.error,
              is_valid: false
            });
          }
        });

        // Update each validation with its current result
        const updatedValidations = validations.map(validation => {
          const result = resultsByRuleName[validation.rule_name];
          if (result) {
            return {
              ...validation,
              last_result: result.error ? null : result.is_valid,
              actual_value: result.actual_value,
              error: result.error,
              last_run_at: new Date().toISOString()
            };
          }
          return validation;
        });

        // Handle errors if any exist
        if (errors.length > 0) {
          setValidationErrors(errors);
          showNotification(`Encountered ${errors.length} errors running validations`, 'error');
        } else {
          // Count successes and failures
          const passedCount = response.results.filter(r => r.is_valid === true).length;
          const failedCount = response.results.filter(r => r.is_valid === false).length;

          showNotification(
            `Ran ${response.results.length} validations: ${passedCount} passed, ${failedCount} failed`,
            failedCount > 0 ? 'warning' : 'success'
          );
        }

        // Update the validations locally while we wait for the refresh
        if (typeof onUpdate === 'function') {
          onUpdate(updatedValidations);
        }
      } else {
        showNotification(`Unexpected response format from server`, 'error');
      }

      // Refresh the list to show updated results
      if (onRefreshList) {
        onRefreshList();
      }
    } catch (error) {
      console.error(`Error running all validations:`, error);
      showNotification(`Failed to run validations: ${error.message}`, 'error');
    } finally {
      setRunningAll(false);
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
      await validationsAPI.deleteRule(
        tableName,
        validationToDelete.rule_name,
        connectionId  // Pass connectionId
      );

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
      {/* Show error handler if we have validation errors */}
      {validationErrors && validationErrors.length > 0 && (
        <div className="mb-4">
          <ValidationErrorHandler
            errors={validationErrors}
            onRefresh={handleRunAllValidations}
            connectionId={connectionId}
            tableName={tableName}
          />
        </div>
      )}

      {/* Run all validations button */}
      <div className="py-3 px-6 border-b border-secondary-200">
        <button
          type="button"
          onClick={handleRunAllValidations}
          disabled={runningAll}
          className="w-full flex justify-center items-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
        >
          {runningAll ? (
            <>
              <LoadingSpinner size="sm" className="mr-2" />
              Running All Validations...
            </>
          ) : (
            <>
              <ArrowPathIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
              Run All Validations
            </>
          )}
        </button>
      </div>

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
                  disabled={runningValidation === validation.id || runningAll}
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

                {onDebug && (
                  <button
                    type="button"
                    onClick={() => onDebug(validation)}
                    className="p-1 text-secondary-400 hover:text-primary-500 focus:outline-none"
                    title="Debug validation"
                  >
                    <BugAntIcon className="h-5 w-5" aria-hidden="true" />
                  </button>
                )}

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

            <div className="mt-2 text-xs text-secondary-500">
              {validation.last_run_at ? (
                <>
                  Last run: <span className="font-medium">{new Date(validation.last_run_at).toLocaleString()}</span>
                  {validation.actual_value !== undefined && (
                    <span className="ml-2">
                      Actual value: <span className={`font-medium ${
                        validation.last_result === true ? 'text-accent-600' : 'text-danger-600'
                      }`}>{validation.actual_value}</span>
                    </span>
                  )}
                </>
              ) : (
                <span className="italic">Not run yet</span>
              )}
            </div>

            {/* Display error message if available */}
            {validation.error && (
              <div className="mt-2 text-xs text-danger-600 bg-danger-50 p-2 rounded-md">
                <span className="font-medium">Error:</span> {validation.error}
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