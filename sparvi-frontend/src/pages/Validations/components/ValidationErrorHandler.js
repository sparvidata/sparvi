import React from 'react';
import {
  ExclamationTriangleIcon,
  InformationCircleIcon,
  ArrowPathIcon,
  ServerIcon
} from '@heroicons/react/24/outline';

/**
 * Component to handle and display validation errors
 */
const ValidationErrorHandler = ({
  errors,
  onRefresh,
  connectionId,
  tableName
}) => {
  // Parse the error pattern to determine the error type
  const getErrorType = (errorMessage) => {
    const pluginError = errorMessage.includes("_instantiate_plugins");
    const connectionError = errorMessage.includes("connection") || errorMessage.includes("Authentication");
    const schemaError =
      errorMessage.includes("no such column") ||
      errorMessage.includes("table not found") ||
      errorMessage.includes("does not exist");

    if (pluginError) return "plugin";
    if (connectionError) return "connection";
    if (schemaError) return "schema";
    return "unknown";
  };

  // Group errors by type
  const errorCounts = errors.reduce((acc, error) => {
    const errorType = getErrorType(error.error);
    acc[errorType] = (acc[errorType] || 0) + 1;
    return acc;
  }, {});

  // Determine the primary error type (most common)
  const primaryErrorType = Object.entries(errorCounts)
    .sort((a, b) => b[1] - a[1])[0]?.[0] || "unknown";

  // Get a friendly message based on the error type
  const getErrorMessage = () => {
    switch (primaryErrorType) {
      case "plugin":
        return "The validation system is having trouble with plugins. This is likely a backend configuration issue.";
      case "connection":
        return "There was a problem connecting to your database. Please check your connection details.";
      case "schema":
        return "The table schema doesn't match what the validation rules expect. The table structure may have changed.";
      default:
        return "There was an unexpected error running the validations. Please check the error details below.";
    }
  };

  // Get action suggestions based on the error type
  const getActionSuggestions = () => {
    switch (primaryErrorType) {
      case "plugin":
        return (
          <div className="mt-2 space-y-2">
            <div className="flex items-start">
              <div className="flex-shrink-0">
                <InformationCircleIcon className="h-5 w-5 text-primary-500" />
              </div>
              <div className="ml-3 text-sm">
                <p className="text-secondary-700">
                  This appears to be a server-side issue with the validation plugin system.
                </p>
              </div>
            </div>
            <div className="flex items-start">
              <div className="flex-shrink-0">
                <ArrowPathIcon className="h-5 w-5 text-primary-500" />
              </div>
              <div className="ml-3 text-sm">
                <p className="text-secondary-700">
                  Try refreshing the metadata for this table and run validations again.
                </p>
              </div>
            </div>
          </div>
        );
      case "connection":
        return (
          <div className="mt-2 space-y-2">
            <div className="flex items-start">
              <div className="flex-shrink-0">
                <ServerIcon className="h-5 w-5 text-primary-500" />
              </div>
              <div className="ml-3 text-sm">
                <p className="text-secondary-700">
                  Check your database connection settings and make sure your credentials are correct.
                </p>
              </div>
            </div>
            <div className="flex items-start">
              <div className="flex-shrink-0">
                <ArrowPathIcon className="h-5 w-5 text-primary-500" />
              </div>
              <div className="ml-3 text-sm">
                <p className="text-secondary-700">
                  Test the database connection and try running validations again.
                </p>
              </div>
            </div>
          </div>
        );
      case "schema":
        return (
          <div className="mt-2 space-y-2">
            <div className="flex items-start">
              <div className="flex-shrink-0">
                <InformationCircleIcon className="h-5 w-5 text-primary-500" />
              </div>
              <div className="ml-3 text-sm">
                <p className="text-secondary-700">
                  The table schema may have changed since these validation rules were created.
                </p>
              </div>
            </div>
            <div className="flex items-start">
              <div className="flex-shrink-0">
                <ArrowPathIcon className="h-5 w-5 text-primary-500" />
              </div>
              <div className="ml-3 text-sm">
                <p className="text-secondary-700">
                  Try refreshing the metadata for this table and update your validation rules.
                </p>
              </div>
            </div>
          </div>
        );
      default:
        return (
          <div className="mt-2">
            <div className="flex items-start">
              <div className="flex-shrink-0">
                <InformationCircleIcon className="h-5 w-5 text-primary-500" />
              </div>
              <div className="ml-3 text-sm">
                <p className="text-secondary-700">
                  Please review the error details below for more information.
                </p>
              </div>
            </div>
          </div>
        );
    }
  };

  return (
    <div className="rounded-md bg-danger-50 p-4">
      <div className="flex">
        <div className="flex-shrink-0">
          <ExclamationTriangleIcon className="h-5 w-5 text-danger-400" aria-hidden="true" />
        </div>
        <div className="ml-3 w-full">
          <h3 className="text-sm font-medium text-danger-800">Validation Errors</h3>
          <div className="mt-2 text-sm text-danger-700">
            <p>{getErrorMessage()}</p>
          </div>

          {getActionSuggestions()}

          <div className="mt-4">
            <div className="-mx-2 -my-1.5 flex">
              <button
                type="button"
                onClick={onRefresh}
                className="bg-danger-50 px-2 py-1.5 rounded-md text-sm font-medium text-danger-800 hover:bg-danger-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-danger-500"
              >
                Try Again
              </button>
            </div>
          </div>

          {/* Collapsible error details */}
          <details className="mt-4">
            <summary className="text-sm font-medium text-danger-800 cursor-pointer">
              View Error Details
            </summary>
            <div className="mt-2 max-h-48 overflow-auto bg-white rounded border border-danger-200 p-2">
              <ul className="text-xs text-secondary-700 font-mono space-y-1">
                {errors.map((error, index) => (
                  <li key={index} className="border-b border-danger-100 pb-1">
                    <span className="font-semibold">{error.name}:</span> {error.error}
                  </li>
                ))}
              </ul>
            </div>
          </details>
        </div>
      </div>
    </div>
  );
};

export default ValidationErrorHandler;