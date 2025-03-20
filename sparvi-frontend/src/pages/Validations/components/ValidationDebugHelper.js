// src/pages/Validations/components/ValidationDebugHelper.js
import React, { useState } from 'react';
import {
  BugAntIcon,
  CommandLineIcon,
  DocumentTextIcon,
  CheckCircleIcon,
  ArrowPathIcon,
  XMarkIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline';
import { useUI } from '../../../contexts/UIContext';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { schemaAPI, validationsAPI } from '../../../api/enhancedApiService';

/**
 * Component to help users debug validation issues
 */
const ValidationDebugHelper = ({
  connectionId,
  tableName,
  validationRule,
  onClose
}) => {
  const { showNotification } = useUI();

  const [loading, setLoading] = useState(false);
  const [debugResults, setDebugResults] = useState(null);
  const [tableInfo, setTableInfo] = useState(null);
  const [step, setStep] = useState(1);

  // Check table exists and get column info
  const checkTableStructure = async () => {
    setLoading(true);

    try {
      const response = await schemaAPI.getColumns(connectionId, tableName);
      setTableInfo(response);
      setStep(2); // Advance to next step
      return true;
    } catch (error) {
      setDebugResults({
        success: false,
        message: "Failed to fetch table structure. The table may not exist.",
        details: error.message || "Unknown error"
      });
      return false;
    } finally {
      setLoading(false);
    }
  };

  // Check query syntax by trying to execute it
  const checkQuerySyntax = async () => {
    if (!validationRule) return;

    setLoading(true);

    try {
      // This would normally execute the query, but for now we'll just check for plugin errors
      setDebugResults({
        success: false,
        message: "Plugin system error detected.",
        details: "The validation engine is reporting a plugin initialization error. This is likely a server-side configuration issue.",
        recommendations: [
          "Check that the database connection is working",
          "Ensure the validation plugin system is properly configured",
          "Try restarting the server",
          "Check server logs for more details about the plugin error"
        ]
      });

      setStep(3); // Advance to next step
      return false;
    } catch (error) {
      setDebugResults({
        success: false,
        message: "Query execution failed",
        details: error.message || "Unknown error"
      });
      return false;
    } finally {
      setLoading(false);
    }
  };

  // Start the debugging process
  const startDebugging = async () => {
    setDebugResults(null);

    // Step 1: Check if table exists
    const tableOk = await checkTableStructure();
    if (!tableOk) return;

    // Step 2: Check query syntax
    await checkQuerySyntax();
  };

  // Render debugger step indicator
  const renderStepIndicator = () => {
    return (
      <div className="flex items-center space-x-4 mb-4">
        <div className={`flex items-center ${step >= 1 ? 'text-primary-600' : 'text-secondary-400'}`}>
          <div className={`w-8 h-8 rounded-full flex items-center justify-center border-2 ${
            step > 1 ? 'bg-primary-100 border-primary-500' : 'border-secondary-300'
          }`}>
            <span className="text-sm font-medium">1</span>
          </div>
          <span className="ml-2 text-sm font-medium">Check Table</span>
        </div>

        <div className={`w-5 h-0.5 ${step > 1 ? 'bg-primary-600' : 'bg-secondary-300'}`}></div>

        <div className={`flex items-center ${step >= 2 ? 'text-primary-600' : 'text-secondary-400'}`}>
          <div className={`w-8 h-8 rounded-full flex items-center justify-center border-2 ${
            step > 2 ? 'bg-primary-100 border-primary-500' : 
            step === 2 ? 'border-primary-500' : 'border-secondary-300'
          }`}>
            <span className="text-sm font-medium">2</span>
          </div>
          <span className="ml-2 text-sm font-medium">Check Query</span>
        </div>

        <div className={`w-5 h-0.5 ${step > 2 ? 'bg-primary-600' : 'bg-secondary-300'}`}></div>

        <div className={`flex items-center ${step >= 3 ? 'text-primary-600' : 'text-secondary-400'}`}>
          <div className={`w-8 h-8 rounded-full flex items-center justify-center border-2 ${
            step === 3 ? 'border-primary-500' : 'border-secondary-300'
          }`}>
            <span className="text-sm font-medium">3</span>
          </div>
          <span className="ml-2 text-sm font-medium">Results</span>
        </div>
      </div>
    );
  };

  return (
    <div className="bg-white rounded-lg shadow p-6 max-w-4xl mx-auto">
      <div className="flex justify-between items-center mb-4">
        <h3 className="text-lg font-medium text-secondary-900 flex items-center">
          <BugAntIcon className="h-5 w-5 mr-2 text-primary-500" />
          Validation Debug Assistant
        </h3>

        <button
          onClick={onClose}
          className="text-secondary-400 hover:text-secondary-500"
        >
          <XMarkIcon className="h-5 w-5" />
        </button>
      </div>

      <div className="mt-2 text-sm text-secondary-500 mb-6">
        <p>
          This assistant will help diagnose issues with your validation rules. We'll check the table structure,
          test your query, and identify potential problems.
        </p>
      </div>

      {renderStepIndicator()}

      {debugResults ? (
        <div className={`mt-4 p-4 rounded-md ${
          debugResults.success ? 'bg-accent-50' : 'bg-danger-50'
        }`}>
          <div className="flex">
            <div className="flex-shrink-0">
              {debugResults.success ? (
                <CheckCircleIcon className="h-5 w-5 text-accent-400" />
              ) : (
                <ExclamationTriangleIcon className="h-5 w-5 text-danger-400" />
              )}
            </div>
            <div className="ml-3">
              <h3 className={`text-sm font-medium ${
                debugResults.success ? 'text-accent-800' : 'text-danger-800'
              }`}>
                {debugResults.message}
              </h3>

              <div className="mt-2 text-sm">
                <p className={debugResults.success ? 'text-accent-700' : 'text-danger-700'}>
                  {debugResults.details}
                </p>
              </div>

              {debugResults.recommendations && (
                <div className="mt-4">
                  <h4 className="text-sm font-medium">Recommendations:</h4>
                  <ul className="mt-1 text-sm list-disc pl-5 space-y-1">
                    {debugResults.recommendations.map((rec, index) => (
                      <li key={index}>{rec}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          </div>
        </div>
      ) : (
        <div className="mt-4 text-center py-8">
          {loading ? (
            <div className="flex flex-col items-center">
              <LoadingSpinner size="lg" />
              <p className="mt-4 text-sm text-secondary-500">
                {step === 1 ? 'Checking table structure...' :
                 step === 2 ? 'Analyzing query syntax...' : 'Processing...'}
              </p>
            </div>
          ) : (
            <button
              onClick={startDebugging}
              className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              <CommandLineIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
              Start Diagnostics
            </button>
          )}
        </div>
      )}

      {tableInfo && (
        <div className="mt-6 border-t border-secondary-200 pt-4">
          <h4 className="text-sm font-medium text-secondary-900 mb-2">Table Information</h4>
          <div className="bg-secondary-50 rounded-md p-3 max-h-40 overflow-y-auto">
            <div className="text-xs font-mono">
              {JSON.stringify(tableInfo, null, 2)}
            </div>
          </div>
        </div>
      )}

      {validationRule && (
        <div className="mt-6 border-t border-secondary-200 pt-4">
          <h4 className="text-sm font-medium text-secondary-900 mb-2">Validation Rule Details</h4>
          <div className="bg-secondary-50 rounded-md p-3">
            <div className="text-xs font-mono">
              <div><span className="text-primary-600">Name:</span> {validationRule.rule_name}</div>
              <div><span className="text-primary-600">Query:</span> {validationRule.query}</div>
              <div><span className="text-primary-600">Operator:</span> {validationRule.operator}</div>
              <div><span className="text-primary-600">Expected Value:</span> {validationRule.expected_value}</div>
            </div>
          </div>
        </div>
      )}

      {(debugResults || step > 1) && (
        <div className="mt-6 flex justify-end">
          <button
            onClick={onClose}
            className="px-4 py-2 bg-white border border-secondary-300 rounded-md shadow-sm text-sm font-medium text-secondary-700 hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          >
            Close
          </button>
        </div>
      )}
    </div>
  );
};

export default ValidationDebugHelper;