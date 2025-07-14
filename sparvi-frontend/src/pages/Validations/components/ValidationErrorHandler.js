import React from 'react';
import {
  ExclamationTriangleIcon,
  InformationCircleIcon,
  ArrowPathIcon,
  ServerIcon,
  CommandLineIcon,
  ShieldExclamationIcon,
  ClockIcon,
  DocumentTextIcon
} from '@heroicons/react/24/outline';

/**
 * Component to handle and display validation errors with enhanced categorization
 */
const ValidationErrorHandler = ({
  errors,
  onRefresh,
  connectionId,
  tableName,
  onRefreshMetadata,
  onDebugErrors
}) => {
  // Enhanced error pattern detection with more categories
  const getErrorType = (errorMessage) => {
    if (!errorMessage || errorMessage === "Unknown error") {
      return "unknown";
    }

    // More comprehensive pattern matching
    const patterns = {
      syntax: ['syntax error', 'SQL compilation', 'unexpected token', 'expected but found', 'missing expression', 'unexpected end'],
      schema: ['no such column', 'table not found', 'does not exist', 'undefined column', 'undefined table', 'invalid column'],
      connection: ['connection', 'Authentication', 'Failed to connect', 'network error', 'timeout connecting', 'access denied'],
      plugin: ['plugin', '_instantiate_plugins', 'module not found', 'cannot load plugin'],
      timeout: ['timeout', 'execution time', 'query cancelled', 'exceeded', 'operation aborted'],
      permission: ['permission', 'access denied', 'privileges', 'not authorized', 'insufficient privilege'],
      regex: ['regular expression', 'regex', 'invalid pattern', 'invalid flag', 'unterminated group'],
      datatype: ['data type', 'conversion failed', 'cannot cast', 'incompatible types', 'invalid format'],
      concurrency: ['deadlock', 'concurrent', 'transaction conflict', 'resource busy', 'locked'],
      memory: ['out of memory', 'memory limit', 'resource limit', 'insufficient resources'],
    };

    // Find matching error type
    for (const [type, keywords] of Object.entries(patterns)) {
      if (keywords.some(keyword => errorMessage.toLowerCase().includes(keyword.toLowerCase()))) {
        return type;
      }
    }

    return "unknown";
  };

  // Group errors by type for better analysis
  const errorsByType = errors.reduce((acc, error) => {
    const errorType = getErrorType(error.error);
    if (!acc[errorType]) {
      acc[errorType] = [];
    }
    acc[errorType].push(error);
    return acc;
  }, {});

  // Determine the primary error type (most common)
  const primaryErrorType = Object.entries(errorsByType)
    .sort((a, b) => b[1].length - a[1].length)[0]?.[0] || "unknown";

  // Count unique error types for summary
  const uniqueErrorTypes = Object.keys(errorsByType).length;

  // Get a friendly message based on the error type
  const getErrorMessage = () => {
    const errorTypeMessages = {
      syntax: "There are SQL syntax issues in one or more validation rules.",
      schema: "The table schema doesn't match what the validation rules expect. The table structure may have changed.",
      connection: "There was a problem connecting to your database. Please check your connection details.",
      plugin: "The validation system is having trouble with plugins. This is likely a backend configuration issue.",
      timeout: "Some validation queries timed out. They may be too complex or the database is busy.",
      permission: "There are permission issues accessing some tables or data. Check database user privileges.",
      regex: "There are problems with regular expressions in one or more validation rules.",
      datatype: "There are data type mismatches in your validation queries.",
      concurrency: "Database resource conflicts occurred. The database might be under heavy load.",
      memory: "The system ran out of resources when executing some validations.",
      unknown: "There were unexpected errors running the validations. Please check the error details below."
    };

    return errorTypeMessages[primaryErrorType] || errorTypeMessages.unknown;
  };

  // Get the appropriate icon for the error type
  const getErrorIcon = () => {
    const icons = {
      syntax: CommandLineIcon,
      schema: DocumentTextIcon,
      connection: ServerIcon,
      plugin: ShieldExclamationIcon,
      timeout: ClockIcon,
      permission: ShieldExclamationIcon,
      regex: CommandLineIcon,
      datatype: DocumentTextIcon,
      concurrency: ClockIcon,
      memory: ServerIcon,
      unknown: ExclamationTriangleIcon
    };

    const Icon = icons[primaryErrorType] || ExclamationTriangleIcon;
    return <Icon className="h-5 w-5 text-danger-400" aria-hidden="true" />;
  };

  // Get action suggestions based on the error type
  const getActionSuggestions = () => {
    // More comprehensive suggestions for each error type
    const suggestions = {
      syntax: (
        <div className="mt-2 space-y-2">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <CommandLineIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                Check your SQL syntax in the validation rules. Look for missing parentheses, typos, or incorrect operators.
              </p>
            </div>
          </div>
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <InformationCircleIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                Make sure your SQL is compatible with the specific database type you're using (Snowflake, PostgreSQL, etc.).
              </p>
            </div>
          </div>
        </div>
      ),

      schema: (
        <div className="mt-2 space-y-2">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <DocumentTextIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                The table schema has likely changed since these validation rules were created.
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
      ),

      connection: (
        <div className="mt-2 space-y-2">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <ServerIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                Check your database connection settings and make sure your credentials are correct and haven't expired.
              </p>
            </div>
          </div>
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <InformationCircleIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                Ensure network access to the database is available and firewall rules permit connections.
              </p>
            </div>
          </div>
        </div>
      ),

      plugin: (
        <div className="mt-2 space-y-2">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <ShieldExclamationIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                This is a server-side issue with the validation plugin system.
              </p>
            </div>
          </div>
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <ArrowPathIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                Try restarting your browser session. If the issue persists, contact support as the backend might need maintenance.
              </p>
            </div>
          </div>
        </div>
      ),

      timeout: (
        <div className="mt-2 space-y-2">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <ClockIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                Some validation queries are too complex or the database is under heavy load.
              </p>
            </div>
          </div>
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <InformationCircleIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                Try simplifying complex queries or running validations during periods of lower database activity.
              </p>
            </div>
          </div>
        </div>
      ),

      permission: (
        <div className="mt-2 space-y-2">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <ShieldExclamationIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                The database user doesn't have sufficient permissions to access some tables or data.
              </p>
            </div>
          </div>
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <InformationCircleIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                Check that your database user has the necessary permissions (SELECT, etc.) for all tables referenced in validations.
              </p>
            </div>
          </div>
        </div>
      ),

      regex: (
        <div className="mt-2 space-y-2">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <CommandLineIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                There's an issue with regular expressions in your validation rules.
              </p>
            </div>
          </div>
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <InformationCircleIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                Check for invalid regex patterns. Remember that different databases may have different regex syntax.
              </p>
            </div>
          </div>
        </div>
      ),

      datatype: (
        <div className="mt-2 space-y-2">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <DocumentTextIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                There are data type mismatches in your validation queries.
              </p>
            </div>
          </div>
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <InformationCircleIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                Ensure you're using appropriate type casting or conversion in your queries where needed.
              </p>
            </div>
          </div>
        </div>
      ),

      concurrency: (
        <div className="mt-2 space-y-2">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <ClockIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                There are database resource conflicts. The database might be under heavy load.
              </p>
            </div>
          </div>
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <ArrowPathIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                Try running validations when there's less activity on the database or run them individually.
              </p>
            </div>
          </div>
        </div>
      ),

      memory: (
        <div className="mt-2 space-y-2">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <ServerIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                The system ran out of resources when executing validations.
              </p>
            </div>
          </div>
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <InformationCircleIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                Try simplifying complex queries or reducing the number of validations run simultaneously.
              </p>
            </div>
          </div>
        </div>
      ),

      unknown: (
        <div className="mt-2">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <InformationCircleIcon className="h-5 w-5 text-primary-500" />
            </div>
            <div className="ml-3 text-sm">
              <p className="text-secondary-700">
                Please review the error details below for more specific information on what went wrong.
              </p>
            </div>
          </div>
        </div>
      )
    };

    return suggestions[primaryErrorType] || suggestions.unknown;
  };

  // Advanced error analysis
  const errorFrequency = Object.entries(errorsByType).map(([type, typeErrors]) => ({
    type,
    count: typeErrors.length,
    percentage: Math.round((typeErrors.length / errors.length) * 100)
  })).sort((a, b) => b.count - a.count);

  // Error message patterns - find common patterns in errors
  const getErrorPatterns = () => {
    // Extract common patterns from error messages
    const patterns = {};

    errors.forEach(error => {
      if (!error.error) return;

      // Look for common substrings or patterns in errors
      const patternMatches = [
        { regex: /column ['"]([^'"]+)['"]/, group: 1, type: 'column' },
        { regex: /table ['"]([^'"]+)['"]/, group: 1, type: 'table' },
        { regex: /near ['"]([^'"]+)['"]/, group: 1, type: 'syntax' },
        { regex: /line (\d+)/, group: 1, type: 'line' },
        { regex: /timeout after (\d+)/, group: 1, type: 'timeout' }
      ];

      patternMatches.forEach(pattern => {
        const match = error.error.match(pattern.regex);
        if (match && match[pattern.group]) {
          const key = `${pattern.type}:${match[pattern.group]}`;
          patterns[key] = (patterns[key] || 0) + 1;
        }
      });
    });

    // Return the most common patterns
    return Object.entries(patterns)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([pattern, count]) => {
        const [type, value] = pattern.split(':');
        return { type, value, count };
      });
  };

  const commonPatterns = getErrorPatterns();

  // Handle refresh metadata action - check if the prop exists first
  const handleRefreshMetadata = () => {
    if (typeof onRefreshMetadata === 'function') {
      onRefreshMetadata();
    } else {
      // Fallback message if handler not provided
      alert('Please refresh the metadata for this table from the schema page.');
    }
  };

  // Handle debug errors action - check if the prop exists first
  const handleDebugErrors = () => {
    if (typeof onDebugErrors === 'function') {
      onDebugErrors();
    } else {
      // Fallback message if handler not provided
      alert('Use the debug button next to each validation to troubleshoot SQL issues.');
    }
  };

  return (
    <div className="rounded-md bg-danger-50 p-4">
      <div className="flex">
        <div className="flex-shrink-0">
          {getErrorIcon()}
        </div>
        <div className="ml-3 w-full">
          <h3 className="text-sm font-medium text-danger-800">
            Validation Errors {uniqueErrorTypes > 1 ? `(${uniqueErrorTypes} types detected)` : ''}
          </h3>
          <div className="mt-2 text-sm text-danger-700">
            <p>{getErrorMessage()}</p>
          </div>

          {getActionSuggestions()}

          {/* Common error patterns */}
          {commonPatterns.length > 0 && (
            <div className="mt-4">
              <h4 className="text-xs font-medium text-danger-800">Common patterns in errors:</h4>
              <ul className="mt-1 text-xs">
                {commonPatterns.map((pattern, idx) => (
                  <li key={idx} className="text-danger-700">
                    {pattern.type === 'column' && `Column "${pattern.value}" mentioned in ${pattern.count} errors`}
                    {pattern.type === 'table' && `Table "${pattern.value}" mentioned in ${pattern.count} errors`}
                    {pattern.type === 'syntax' && `Syntax error near "${pattern.value}" in ${pattern.count} errors`}
                    {pattern.type === 'line' && `Error on line ${pattern.value} in ${pattern.count} errors`}
                    {pattern.type === 'timeout' && `Timeout after ${pattern.value}s in ${pattern.count} errors`}
                  </li>
                ))}
              </ul>
            </div>
          )}

          <div className="mt-4">
            <div className="-mx-2 -my-1.5 flex flex-wrap">
              <button
                type="button"
                onClick={onRefresh}
                className="bg-danger-50 px-2 py-1.5 rounded-md text-sm font-medium text-danger-800 hover:bg-danger-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-danger-500 mr-2 mb-2"
              >
                <ArrowPathIcon className="inline-block h-4 w-4 mr-1" />
                Try Again
              </button>

              {/* Additional action buttons based on error type */}
              {primaryErrorType === 'schema' && (
                <button
                  type="button"
                  className="bg-primary-50 px-2 py-1.5 rounded-md text-sm font-medium text-primary-700 hover:bg-primary-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 mr-2 mb-2"
                  onClick={handleRefreshMetadata}
                >
                  <DocumentTextIcon className="inline-block h-4 w-4 mr-1" />
                  Refresh Schema
                </button>
              )}

              {primaryErrorType === 'syntax' && (
                <button
                  type="button"
                  className="bg-primary-50 px-2 py-1.5 rounded-md text-sm font-medium text-primary-700 hover:bg-primary-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 mr-2 mb-2"
                  onClick={handleDebugErrors}
                >
                  <CommandLineIcon className="inline-block h-4 w-4 mr-1" />
                  Debug SQL
                </button>
              )}
            </div>
          </div>

          {/* Collapsible error details with improved formatting */}
          <details className="mt-4">
            <summary className="text-sm font-medium text-danger-800 cursor-pointer">
              View Error Details ({errors.length} {errors.length === 1 ? 'error' : 'errors'})
            </summary>
            <div className="mt-2 max-h-48 overflow-auto bg-white rounded border border-danger-200 p-2">
              <ul className="text-xs text-secondary-700 font-mono space-y-1">
                {errors.map((error, index) => (
                  <li key={index} className="border-b border-danger-100 pb-1">
                    <div className="flex flex-col">
                      <div className="font-semibold">{error.name || `Validation #${index + 1}`}:</div>
                      <div className="pl-2 break-words">
                        {/* Show the error message with basic formatting */}
                        {error.error === "Unknown error"
                          ? "Failed to execute validation rule. Check the rule syntax."
                          : error.error || "No error details available"
                        }
                      </div>
                      {error.query && (
                        <div className="pl-2 mt-1 text-xs text-secondary-500">
                          Query: <span className="font-mono">{truncateText(error.query, 80)}</span>
                        </div>
                      )}
                    </div>
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

// Helper function to truncate text
const truncateText = (text, maxLength = 80) => {
  if (!text) return '';
  if (text.length <= maxLength) return text;
  return `${text.substring(0, maxLength)}...`;
};

export default ValidationErrorHandler;