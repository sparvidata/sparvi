import React, { useState } from 'react';
import {
  PlusIcon,
  CheckCircleIcon,
  XCircleIcon,
  PencilIcon,
  TrashIcon,
  ArrowPathIcon,
  ExclamationCircleIcon,
  LightBulbIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { validationsAPI } from '../../../api/enhancedApiService';
import { useUI } from '../../../contexts/UIContext';

const TableValidations = ({
  validations = [],
  isLoading,
  connectionId,
  tableName,
  onUpdate
}) => {
  const { showNotification } = useUI();
  const [running, setRunning] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [validationResults, setValidationResults] = useState({});
  const [selectedValidation, setSelectedValidation] = useState(null);
  const [isDeleting, setIsDeleting] = useState(false);

  // Run all validations
  const handleRunValidations = async () => {
    if (!connectionId || !tableName) return;

    try {
      setRunning(true);

      const response = await validationsAPI.runValidations(
        connectionId,
        tableName,
        null
      );

      const results = response.data.results;

      // Convert results to object keyed by rule name for easy lookup
      const resultsObj = {};
      results.forEach((result, index) => {
        resultsObj[validations[index].rule_name] = result;
      });

      setValidationResults(resultsObj);
      showNotification('Validations completed', 'success');
    } catch (error) {
      console.error('Error running validations:', error);
      showNotification('Error running validations', 'error');
    } finally {
      setRunning(false);
    }
  };

  // Generate default validations
  const handleGenerateValidations = async () => {
    if (!connectionId || !tableName) return;

    try {
      setGenerating(true);

      const response = await validationsAPI.generateDefaultValidations(
        connectionId,
        tableName,
        null
      );

      // Refresh validation list
      const validationsResponse = await validationsAPI.getRules(tableName);
      onUpdate(validationsResponse.data.rules || []);

      showNotification(`Generated ${response.data.count} default validations`, 'success');
    } catch (error) {
      console.error('Error generating validations:', error);
      showNotification('Error generating validations', 'error');
    } finally {
      setGenerating(false);
    }
  };

  // Confirm delete validation
  const confirmDeleteValidation = (validation) => {
    setSelectedValidation(validation);
    setIsDeleting(true);
  };

  // Delete validation
  const handleDeleteValidation = async () => {
    if (!selectedValidation) return;

    try {
      await validationsAPI.deleteRule(tableName, selectedValidation.rule_name);

      // Refresh validation list
      const validationsResponse = await validationsAPI.getRules(tableName);
      onUpdate(validationsResponse.data.rules || []);

      showNotification('Validation rule deleted', 'success');
      setIsDeleting(false);
      setSelectedValidation(null);
    } catch (error) {
      console.error('Error deleting validation:', error);
      showNotification('Error deleting validation', 'error');
    }
  };

  // Get validation result status
  const getValidationStatus = (validation) => {
    const result = validationResults[validation.rule_name];
    if (!result) return 'unknown';

    return result.is_valid ? 'passed' : 'failed';
  };

  // If loading with no validations, show loading state
  if (isLoading && !validations.length) {
    return (
      <div className="flex justify-center py-10">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div>
      {/* Validations header with actions */}
      <div className="mb-6 flex justify-between items-center">
        <h3 className="text-lg font-medium text-secondary-900">
          Validation Rules ({validations.length})
        </h3>

        <div className="flex space-x-3">
          <button
            type="button"
            onClick={handleGenerateValidations}
            disabled={generating}
            className="inline-flex items-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md shadow-sm text-white bg-secondary-600 hover:bg-secondary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-secondary-500 disabled:opacity-50"
          >
            {generating ? (
              <>
                <LoadingSpinner size="sm" className="mr-2" />
                Generating...
              </>
            ) : (
              <>
                <LightBulbIcon className="-ml-0.5 mr-2 h-4 w-4" aria-hidden="true" />
                Generate Default
              </>
            )}
          </button>

          <button
            type="button"
            onClick={handleRunValidations}
            disabled={running || validations.length === 0}
            className="inline-flex items-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
          >
            {running ? (
              <>
                <LoadingSpinner size="sm" className="mr-2" />
                Running...
              </>
            ) : (
              <>
                <ArrowPathIcon className="-ml-0.5 mr-2 h-4 w-4" aria-hidden="true" />
                Run All
              </>
            )}
          </button>

          <button
            type="button"
            className="inline-flex items-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          >
            <PlusIcon className="-ml-0.5 mr-2 h-4 w-4" aria-hidden="true" />
            New Rule
          </button>
        </div>
      </div>

      {/* If no validations, show empty state */}
      {validations.length === 0 ? (
        <div className="text-center py-10 bg-white rounded-lg shadow">
          <ExclamationCircleIcon className="mx-auto h-12 w-12 text-secondary-400" />
          <h3 className="mt-2 text-sm font-medium text-secondary-900">No validation rules</h3>
          <p className="mt-1 text-sm text-secondary-500">
            Get started by adding validation rules to ensure your data quality.
          </p>
          <div className="mt-6 flex justify-center space-x-4">
            <button
              type="button"
              onClick={handleGenerateValidations}
              disabled={generating}
              className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-secondary-600 hover:bg-secondary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-secondary-500 disabled:opacity-50"
            >
              {generating ? (
                <>
                  <LoadingSpinner size="sm" className="mr-2" />
                  Generating...
                </>
              ) : (
                <>
                  <LightBulbIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
                  Generate Default Rules
                </>
              )}
            </button>

            <button
              type="button"
              className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              <PlusIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
              Add Validation Rule
            </button>
          </div>
        </div>
      ) : (
        <div className="bg-white shadow overflow-hidden sm:rounded-md">
          <ul className="divide-y divide-secondary-200">
            {validations.map((validation) => {
              const status = getValidationStatus(validation);
              return (
                <li key={validation.id}>
                  <div className="px-4 py-4 sm:px-6 hover:bg-secondary-50">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center flex-1 min-w-0">
                        {status === 'passed' && (
                          <CheckCircleIcon className="h-5 w-5 text-accent-500 mr-2" aria-hidden="true" />
                        )}
                        {status === 'failed' && (
                          <XCircleIcon className="h-5 w-5 text-danger-500 mr-2" aria-hidden="true" />
                        )}
                        {status === 'unknown' && (
                          <div className="h-5 w-5 text-secondary-400 mr-2" />
                        )}

                        <p className="text-sm font-medium text-secondary-900 truncate flex-1">
                          {validation.rule_name}
                        </p>
                      </div>

                      <div className="flex items-center space-x-2">
                        {status !== 'unknown' && (
                          <span className={`px-2.5 py-0.5 rounded-full text-xs font-medium ${
                            status === 'passed' 
                              ? 'bg-accent-100 text-accent-800' 
                              : 'bg-danger-100 text-danger-800'
                          }`}>
                            {status === 'passed' ? 'Passed' : 'Failed'}
                          </span>
                        )}

                        <button
                          type="button"
                          className="p-1 text-secondary-400 hover:text-secondary-500 focus:outline-none"
                          title="Edit validation"
                        >
                          <PencilIcon className="h-5 w-5" aria-hidden="true" />
                        </button>

                        <button
                          type="button"
                          onClick={() => confirmDeleteValidation(validation)}
                          className="p-1 text-secondary-400 hover:text-danger-500 focus:outline-none"
                          title="Delete validation"
                        >
                          <TrashIcon className="h-5 w-5" aria-hidden="true" />
                        </button>
                      </div>
                    </div>

                    <div className="mt-2 sm:flex sm:justify-between">
                      <div className="sm:flex">
                        <p className="flex items-center text-sm text-secondary-500">
                          Query: <code className="ml-1 text-xs bg-secondary-100 px-1 py-0.5 rounded">{validation.query}</code>
                        </p>
                      </div>
                      <div className="mt-2 flex items-center text-sm text-secondary-500 sm:mt-0">
                        <p>
                          <span className="font-medium">{validation.operator}</span> {validation.expected_value}
                        </p>

                        {validationResults[validation.rule_name]?.actual_value !== undefined && (
                          <p className="ml-4">
                            Actual: <span className={`font-medium ${
                              status === 'passed' ? 'text-accent-600' : 'text-danger-600'
                            }`}>
                              {validationResults[validation.rule_name].actual_value}
                            </span>
                          </p>
                        )}
                      </div>
                    </div>

                    {validation.description && (
                      <div className="mt-2 text-sm text-secondary-500">
                        <p>{validation.description}</p>
                      </div>
                    )}
                  </div>
                </li>
              );
            })}
          </ul>
        </div>
      )}

      {/* Delete confirmation modal */}
      {isDeleting && selectedValidation && (
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
                        Are you sure you want to delete the validation rule "{selectedValidation.rule_name}"? This action cannot be undone.
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
                    setSelectedValidation(null);
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

export default TableValidations;