import React, {useState, useEffect, useMemo, useRef} from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import {
  ClipboardDocumentCheckIcon,
  TableCellsIcon,
  ServerIcon,
  PlusIcon,
  CheckCircleIcon,
  XCircleIcon,
  ArrowPathIcon,
  FunnelIcon,
  MagnifyingGlassIcon
} from '@heroicons/react/24/outline';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { validationsAPI } from '../../api/enhancedApiService';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import EmptyState from '../../components/common/EmptyState';
import ValidationRuleEditor from './components/ValidationRuleEditor';
import ValidationRuleList from './components/ValidationRuleList';
import ValidationErrorHandler from './components/ValidationErrorHandler';
import ValidationDebugHelper from './components/ValidationDebugHelper';
import ValidationResultsSummary from './components/ValidationResultsSummary';
import ValidationResultsTrend from './components/ValidationResultsTrend';
import SearchInput from '../../components/common/SearchInput';
import { useTablesData } from '../../hooks/useTablesData';
import { useTableValidations } from '../../hooks/useValidationsData';
import { processValidationResults } from '../../utils/validationResultsProcessor';
import { ValidationResultsProvider, useValidationResults } from '../../contexts/ValidationResultsContext';
import { queryClient } from '../../api/queryClient';
import { formatDate } from '../../utils/formatting';
import { useValidationRulesList } from '../../hooks/useValidationRulesList';

const ValidationPage = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { connections, activeConnection, setCurrentConnection } = useConnection();
  const { updateBreadcrumbs, showNotification, setLoading, loadingStates } = useUI();
  const {
    updateResultsAfterRun,
    setSelectedTable: setContextSelectedTable
  } = useValidationResults();

  const [selectedTable, setSelectedTable] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');
  const [showEditor, setShowEditor] = useState(false);
  const [editingRule, setEditingRule] = useState(null);
  const [showDebugHelper, setShowDebugHelper] = useState(false);
  const [selectedValidationForDebug, setSelectedValidationForDebug] = useState(null);
  const [validationErrors, setValidationErrors] = useState(null);
  const [initialLoadComplete, setInitialLoadComplete] = useState(false);
  const runInitialValidationsComplete = useRef(false);

  const {
    validations: currentValidations,
    setValidations: setCurrentValidations,
    loading: loadingValidations,
    error: validationsError,
    loadRules: refreshValidations,
    runAllRules,
    runSingleRule
  } = useValidationRulesList(selectedTable, activeConnection?.id, []);

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Validations', href: '/validations' }
    ]);
  }, [updateBreadcrumbs]);

  // Handle table selection from URL params
  useEffect(() => {
    const tableParam = searchParams.get('table');
    if (tableParam) {
      setSelectedTable(tableParam);
      // Also set in context
      if (setContextSelectedTable) {
        setContextSelectedTable(tableParam);
      }
    }
  }, [searchParams, setContextSelectedTable]);

  // Use React Query to fetch tables data
  const tablesQuery = useTablesData(
    activeConnection?.id,
    { enabled: !!activeConnection }
  );

  // Use React Query to fetch validation rules using existing hook
  const validationsQuery = useTableValidations(
    selectedTable,
    {
      enabled: !!selectedTable,
      connectionId: activeConnection?.id  // Pass connectionId to the hook
    }
  );

  // When validations query data changes and it's not the initial load
  useEffect(() => {
    if (validationsQuery.data && initialLoadComplete) {
      console.log('Validation query data updated:', validationsQuery.data);

      // Extract the validations from the response
      let newValidations = [];
      if (Array.isArray(validationsQuery.data)) {
        newValidations = validationsQuery.data;
      } else if (validationsQuery.data.rules && Array.isArray(validationsQuery.data.rules)) {
        newValidations = validationsQuery.data.rules;
      } else if (validationsQuery.data.data && validationsQuery.data.data.rules &&
                Array.isArray(validationsQuery.data.data.rules)) {
        newValidations = validationsQuery.data.data.rules;
      }

      if (newValidations.length > 0) {
        console.log(`Updating state with ${newValidations.length} validations from query`);
        setCurrentValidations(newValidations);
      }
    }
  }, [validationsQuery.data, initialLoadComplete]);

  // Get last run timestamp for all validations
  const getLastRunTimestamp = (validations) => {
    const timestamps = validations
      .filter(v => v.last_run_at)
      .map(v => new Date(v.last_run_at).getTime());

    return timestamps.length > 0 ? new Date(Math.max(...timestamps)) : null;
  };

  // When validations data loads initially, update local state
  useEffect(() => {
    if (validationsQuery.data && !initialLoadComplete) {
      setCurrentValidations(validationsQuery.data);
      setInitialLoadComplete(true);
    }
  }, [validationsQuery.data, initialLoadComplete]);

  // Separate effect for running validations - prevents the loop
  useEffect(() => {
    // Only run initial validations once when data first loads and we have validations
    if (validationsQuery.data?.length > 0 &&
        initialLoadComplete &&
        !runInitialValidationsComplete.current &&
        activeConnection?.id) {

      // This ensures we only run this code once
      runInitialValidationsComplete.current = true;

      // Don't automatically run validations - check if we already have results first
      const hasAnyResults = validationsQuery.data.some(v => v.last_result !== undefined);

      if (!hasAnyResults) {
        console.log("No existing validation results found - showing empty state");
        // Don't run validations automatically - just update the UI to show no results

        // Just update the validation results context instead
        if (updateResultsAfterRun) {
          updateResultsAfterRun([], selectedTable);
        }
      }
    }
  }, [validationsQuery.data, initialLoadComplete, activeConnection?.id, selectedTable, updateResultsAfterRun]);

  // Compute filtered validations for display based on filters
  const filteredValidations = useMemo(() => {
    if (!currentValidations || !currentValidations.length) return [];

    let filtered = [...currentValidations];

    // Apply search query
    if (searchQuery) {
      filtered = filtered.filter(rule =>
        rule.rule_name.toLowerCase().includes(searchQuery.toLowerCase()) ||
        rule.description?.toLowerCase().includes(searchQuery.toLowerCase()) ||
        rule.query.toLowerCase().includes(searchQuery.toLowerCase())
      );
    }

    // Apply status filter
    if (statusFilter !== 'all') {
      filtered = filtered.filter(rule => {
        if (statusFilter === 'passed') return rule.last_result === true;
        if (statusFilter === 'failed') return rule.last_result === false;
        if (statusFilter === 'unknown') return rule.last_result === null || rule.last_result === undefined;
        return true;
      });
    }

    return filtered;
  }, [currentValidations, searchQuery, statusFilter]);

  // Handle table selection
  const handleTableSelect = (tableName) => {
    setSelectedTable(tableName);
    setSearchParams({ table: tableName });
    setInitialLoadComplete(false); // Reset for the new table
    runInitialValidationsComplete.current = false; // Reset for new table

    // Also set the table in the ValidationResultsContext
    if (setContextSelectedTable) {
      setContextSelectedTable(tableName);
    }
  };

  // Handle search
  const handleSearch = (query) => {
    setSearchQuery(query);
  };

  // Handle status filter
  const handleStatusFilter = (status) => {
    setStatusFilter(status);
  };

  // Handle debugging a validation rule
  const handleDebugValidation = (rule) => {
    setSelectedValidationForDebug(rule);
    setShowDebugHelper(true);
  };

  // Close the debug helper
  const handleCloseDebugHelper = () => {
    setShowDebugHelper(false);
    setSelectedValidationForDebug(null);
  };

  // Handle run single validation rule
  const handleRunSingleValidation = async (validation) => {
    if (!activeConnection || !selectedTable || !validation) return;

    try {
      const result = await runSingleRule(validation);

      if (result) {
        showNotification(
          `Validation ${result.is_valid ? 'passed' : 'failed'}: ${validation.rule_name}`,
          result.is_valid ? 'success' : 'warning'
        );
      }
    } catch (error) {
      console.error(`Error running validation ${validation.rule_name}:`, error);
      showNotification(`Failed to run validation: ${error.message}`, 'error');
    }
  };

  // Handle run all validations with status updating
  const handleRunAll = async () => {
    if (!activeConnection || !selectedTable) return;

    try {
      setLoading('validations', true);
      setValidationErrors(null);

      const result = await runAllRules();

      // Process results for metrics
      const processedResults = processValidationResults(result.results);

      // Handle errors if any
      const errorsCount = processedResults.metrics.counts.error;
      if (errorsCount > 0) {
        // Extract errors
        const errorsList = processedResults.processed
          .filter(r => r.status === 'error')
          .map(r => ({
            name: r.rule_name,
            error: r.error,
            query: r.query,
            is_valid: false
          }));

        setValidationErrors(errorsList);

        // Show notification
        showNotification(
          `${errorsList.length} validation errors encountered. See details below.`,
          'warning'
        );
      } else {
        // Clear any previous errors
        setValidationErrors(null);

        // Success message
        const passedCount = processedResults.metrics.counts.passed;
        const failedCount = processedResults.metrics.counts.failed;

        showNotification(
          `Ran ${processedResults.processed.length} validations: ${passedCount} passed, ${failedCount} failed`,
          failedCount > 0 ? 'warning' : 'success'
        );
      }
    } catch (error) {
      console.error('Error running validations:', error);
      showNotification(
        `Failed to run validations: ${error.message || "Unknown error"}`,
        'error'
      );
    } finally {
      setLoading('validations', false);
    }
  };

  // Create new validation
  const handleNewValidation = () => {
    setEditingRule(null);
    setShowEditor(true);
  };

  // Edit a validation
  const handleEditValidation = (rule) => {
    setEditingRule(rule);
    setShowEditor(true);
  };

  // Close editor
  const handleCloseEditor = () => {
    setShowEditor(false);
    setEditingRule(null);
  };

  // Save validation
  const handleSaveValidation = async () => {
    // Invalidate the query to refetch validation rules
    queryClient.invalidateQueries(['table-validations', selectedTable]);

    setShowEditor(false);
    setEditingRule(null);
  };

  // Generate default validations
  const handleGenerateValidations = async () => {
    if (!activeConnection || !selectedTable) return;

    try {
      setLoading('generating', true);

      // Log the connection ID and table name for debugging
      console.log(`Generating validations for table ${selectedTable} with connection ID ${activeConnection.id}`);

      const response = await validationsAPI.generateDefaultValidations(
        activeConnection.id,
        selectedTable,
        null
      );

      console.log('Full generation response:', response);

      // Check if we have the rules already in the response
      let newRules = [];

      if (response.rules) {
        // Extract rules from the nested response
        if (Array.isArray(response.rules)) {
          newRules = response.rules;
        } else if (response.rules.rules && Array.isArray(response.rules.rules)) {
          newRules = response.rules.rules;
        } else if (response.rules.data && response.rules.data.rules && Array.isArray(response.rules.data.rules)) {
          newRules = response.rules.data.rules;
        }

        console.log(`Found ${newRules.length} rules in the response`);
      }

      // If we got rules directly, use them
      if (newRules.length > 0) {
        setCurrentValidations(newRules);
        showNotification(`Generated ${newRules.length} default validations for ${selectedTable}`, 'success');
      } else {
        // Otherwise, force a refetch
        try {
          // Invalidate the query cache first
          queryClient.invalidateQueries(['table-validations', selectedTable]);

          // Wait a moment for the backend to process
          await new Promise(resolve => setTimeout(resolve, 800));

          // Explicitly refetch with the connection ID
          const fetched = await validationsAPI.getRules(
            selectedTable,
            {
              connectionId: activeConnection.id,
              forceFresh: true
            }
          );

          console.log('Fetched rules after generation:', fetched);

          // Extract rules based on the response format
          let rules = [];
          if (Array.isArray(fetched)) {
            rules = fetched;
          } else if (fetched.rules && Array.isArray(fetched.rules)) {
            rules = fetched.rules;
          } else if (fetched.data && fetched.data.rules && Array.isArray(fetched.data.rules)) {
            rules = fetched.data.rules;
          }

          if (rules.length > 0) {
            setCurrentValidations(rules);
          }

          // Get count from either response
          const count = (response.generation && response.generation.count) ||
                         (response.count) ||
                         rules.length;

          showNotification(`Generated ${count} default validations for ${selectedTable}`, 'success');
        } catch (fetchError) {
          console.error('Error fetching validations after generation:', fetchError);

          // Fallback notification with count from the generation response
          const count = response.generation?.count || response.count || 0;
          showNotification(`Generated ${count} default validations for ${selectedTable}. Refresh to view.`, 'success');
        }
      }

      // Explicitly triggering a refetch
      validationsQuery.refetch();

      // Reset initialization flags to force a refresh
      setInitialLoadComplete(false);
      runInitialValidationsComplete.current = false;
    } catch (error) {
      console.error('Error generating validations:', error);
      showNotification('Failed to generate validations: ' + (error.message || 'Unknown error'), 'error');
    } finally {
      setLoading('generating', false);
    }
  };

  // Helper function to update validations with results
  const updateValidationsWithResults = (validations, results) => {
    if (!results || !Array.isArray(results)) return validations;

    // Create a map of results by rule_name
    const resultsByRuleName = {};
    results.forEach(result => {
      if (result.rule_name) {
        resultsByRuleName[result.rule_name] = result;
      }
    });

    // Update validations with results
    return validations.map(validation => {
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
  };

  // Helper function to categorize system errors
  const categorizeSystemError = (error) => {
    // Network or server errors
    if (error.message?.includes('timeout') || error.code === 'ECONNABORTED') {
      return {
        type: 'timeout',
        message: 'Request timed out. The validation may be too complex or the server is busy.',
        recommendedAction: 'Try running validations individually or try again later.'
      };
    }

    if (error.message?.includes('Network Error') || error.response?.status === 502) {
      return {
        type: 'network',
        message: 'Network connection issue or server unavailable.',
        recommendedAction: 'Check your internet connection and try again.'
      };
    }

    // Authentication errors
    if (error.response?.status === 401 || error.response?.status === 403) {
      return {
        type: 'auth',
        message: 'Authentication error. Your session may have expired.',
        recommendedAction: 'Try logging in again.'
      };
    }

    // Default unknown error
    return {
      type: 'unknown',
      message: error.message || 'Unknown error occurred',
      recommendedAction: 'Check the console for more details.'
    };
  };

  // Helper function to categorize validation errors
  const categorizeValidationError = (errorMessage) => {
    if (!errorMessage) {
      return {
        type: 'unknown',
        shortMessage: 'Unknown error',
        details: 'No error details available'
      };
    }

    // SQL syntax errors
    if (errorMessage.includes('syntax error') ||
        errorMessage.includes('SQL compilation error') ||
        errorMessage.includes('invalid syntax')) {
      return {
        type: 'syntax',
        shortMessage: 'SQL Syntax Error',
        details: errorMessage
      };
    }

    // Schema-related errors
    if (errorMessage.includes('no such column') ||
        errorMessage.includes('table not found') ||
        errorMessage.includes('does not exist')) {
      return {
        type: 'schema',
        shortMessage: 'Schema Error',
        details: 'The table or column referenced in the validation may no longer exist.'
      };
    }

    // Default case
    return {
      type: 'execution',
      shortMessage: 'Execution Error',
      details: errorMessage
    };
  };

  // Helper functions for error analysis
  const categorizeErrors = (errorsList) => {
    const categories = {
      syntax: 0,
      schema: 0,
      connection: 0,
      plugin: 0,
      timeout: 0,
      permission: 0,
      regex: 0,
      unknown: 0
    };

    errorsList.forEach(error => {
      const errorMsg = error.error || '';

      if (errorMsg.includes('syntax') || errorMsg.includes('SQL compilation')) {
        categories.syntax++;
      } else if (errorMsg.includes('no such column') || errorMsg.includes('table not found')) {
        categories.schema++;
      } else if (errorMsg.includes('connection') || errorMsg.includes('Authentication')) {
        categories.connection++;
      } else if (errorMsg.includes('plugin') || errorMsg.includes('_instantiate_plugins')) {
        categories.plugin++;
      } else if (errorMsg.includes('timeout') || errorMsg.includes('execution time')) {
        categories.timeout++;
      } else if (errorMsg.includes('permission') || errorMsg.includes('access denied')) {
        categories.permission++;
      } else if (errorMsg.includes('regular expression') || errorMsg.includes('regex')) {
        categories.regex++;
      } else {
        categories.unknown++;
      }
    });

    return categories;
  };

  const getPrimaryErrorCategory = (categories) => {
    // Find the category with the most errors
    let maxCount = 0;
    let primaryCategory = 'unknown';

    for (const [category, count] of Object.entries(categories)) {
      if (count > maxCount) {
        maxCount = count;
        primaryCategory = category;
      }
    }

    // Map category to user-friendly message
    const categoryMessages = {
      syntax: 'SQL syntax issues',
      schema: 'table schema mismatches',
      connection: 'database connection problems',
      plugin: 'validation plugin errors',
      timeout: 'execution timeouts',
      permission: 'permission denied errors',
      regex: 'regular expression problems',
      unknown: 'unknown issues'
    };

    return categoryMessages[primaryCategory] || 'unknown issues';
  };

  // If no connections, show empty state
  if (!connections.length) {
    return (
      <EmptyState
        icon={ServerIcon}
        title="No connections configured"
        description="Create a connection to start defining validation rules"
        actionText="Add Connection"
        actionLink="/connections/new"
      />
    );
  }

  return (
    <div className="py-4">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold text-secondary-900">Data Validations</h1>
      </div>

      <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-12">
        {/* Side panel with tables */}
        <div className="lg:col-span-3">
          <div className="bg-white shadow rounded-lg">
            <div className="px-4 py-3 border-b border-secondary-200">
              <h3 className="text-sm font-medium text-secondary-900">Tables</h3>
            </div>
            <div className="p-4">
              {tablesQuery.isLoading ? (
                <div className="flex justify-center py-4">
                  <LoadingSpinner size="md" />
                </div>
              ) : tablesQuery.isError ? (
                <div className="text-center py-4">
                  <p className="text-sm text-danger-500">Error loading tables</p>
                  <button
                    onClick={() => tablesQuery.refetch()}
                    className="mt-2 text-sm text-primary-600 hover:text-primary-500"
                  >
                    Try again
                  </button>
                </div>
              ) : !tablesQuery.data || tablesQuery.data.length === 0 ? (
                <div className="text-center py-4">
                  <p className="text-sm text-secondary-500">No tables found</p>
                </div>
              ) : (
                <div className="space-y-1 max-h-96 overflow-y-auto">
                  {tablesQuery.data.map(table => (
                    <button
                      key={table}
                      className={`w-full text-left px-3 py-2 rounded-md text-sm ${
                        selectedTable === table 
                          ? 'bg-primary-50 text-primary-700 font-medium' 
                          : 'text-secondary-700 hover:bg-secondary-100 hover:text-secondary-900'
                      }`}
                      onClick={() => handleTableSelect(table)}
                    >
                      <div className="flex items-center">
                        <TableCellsIcon className="h-4 w-4 mr-2 flex-shrink-0" />
                        <span className="truncate">{table}</span>
                      </div>
                    </button>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Main content with validations */}
        <div className="lg:col-span-9">
          {!selectedTable ? (
            <div className="bg-white shadow rounded-lg p-6 text-center">
              <ClipboardDocumentCheckIcon className="mx-auto h-12 w-12 text-secondary-400" />
              <h3 className="mt-2 text-sm font-medium text-secondary-900">Select a table to see validations</h3>
              <p className="mt-1 text-sm text-secondary-500">
                Choose a table from the list to view and manage validation rules.
              </p>
            </div>
          ) : showEditor ? (
            <ValidationRuleEditor
              connectionId={activeConnection?.id}
              tableName={selectedTable}
              rule={editingRule}
              onSave={handleSaveValidation}
              onCancel={handleCloseEditor}
            />
          ) : (
            <div className="space-y-4">
              {/* Results Summary and Trend */}
              {selectedTable && (
                <div className="space-y-4">
                  <ValidationResultsSummary
                    onRunAll={handleRunAll}
                    isRunning={loadingStates?.validations}
                  />
                  <ValidationResultsTrend />
                </div>
              )}

              <div className="bg-white shadow rounded-lg">
                <div className="px-4 py-3 border-b border-secondary-200 flex items-center justify-between">
                  <h3 className="text-lg font-medium text-secondary-900">
                    Validation Rules: {selectedTable}
                  </h3>
                  <div className="flex space-x-2">
                    <button
                      type="button"
                      onClick={handleGenerateValidations}
                      className="inline-flex items-center px-3 py-1.5 border border-secondary-300 shadow-sm text-sm font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-secondary-500"
                    >
                      <ClipboardDocumentCheckIcon className="-ml-1 mr-2 h-4 w-4" aria-hidden="true" />
                      Generate Default
                    </button>

                    <button
                      type="button"
                      onClick={handleNewValidation}
                      className="inline-flex items-center px-3 py-1.5 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                    >
                      <PlusIcon className="-ml-1 mr-2 h-4 w-4" aria-hidden="true" />
                      New Rule
                    </button>

                    <button
                      type="button"
                      onClick={handleRunAll}
                      disabled={loadingStates?.validations}
                      className="inline-flex items-center px-3 py-1.5 border border-transparent shadow-sm text-sm font-medium rounded-md text-green-50 bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:opacity-50"
                    >
                      {loadingStates?.validations ? (
                        <>
                          <LoadingSpinner size="sm" className="mr-2" />
                          Running...
                        </>
                      ) : (
                        <>
                          <ArrowPathIcon className="-ml-1 mr-2 h-4 w-4" />
                          Run All
                        </>
                      )}
                    </button>
                  </div>
                </div>

                {/* Display validation errors at the top if present */}
                {validationErrors && validationErrors.length > 0 && (
                  <div className="px-4 py-3 bg-white">
                    <ValidationErrorHandler
                      errors={validationErrors}
                      onRefresh={handleRunAll}
                      connectionId={activeConnection?.id}
                      tableName={selectedTable}
                    />
                  </div>
                )}

                {/* Last run summary */}
                {currentValidations.length > 0 && (
                  <div className="px-4 py-3 bg-secondary-50 border-b border-secondary-200">
                    <div className="text-sm text-secondary-500 flex items-center justify-between">
                      <div>
                        <span className="font-medium text-secondary-700">{currentValidations.length}</span> validation rules
                        {" | "}
                        <span className="font-medium text-accent-600">{currentValidations.filter(v => v.last_result === true).length}</span> passing
                        {" | "}
                        <span className="font-medium text-danger-600">{currentValidations.filter(v => v.last_result === false).length}</span> failing
                        {" | "}
                        <span className="font-medium text-secondary-600">{currentValidations.filter(v => v.last_result === undefined || v.last_result === null).length}</span> not run
                      </div>

                      <div>
                        <span className="text-sm text-secondary-500">
                          Last run: {getLastRunTimestamp(currentValidations) ? formatDate(getLastRunTimestamp(currentValidations), true) : 'Never'}
                        </span>
                      </div>
                    </div>
                  </div>
                )}

                {/* Search and filters */}
                <div className="px-4 py-3 border-b border-secondary-200 bg-white sm:px-6">
                  <div className="flex items-center justify-between">
                    <div className="flex-1">
                      <SearchInput
                        onSearch={handleSearch}
                        placeholder="Search validations..."
                        initialValue={searchQuery}
                      />
                    </div>

                    <div className="flex items-center space-x-2 ml-4">
                      <span className="text-sm text-secondary-700">Status:</span>
                      <div className="flex space-x-1">
                        <button
                          type="button"
                          onClick={() => handleStatusFilter('all')}
                          className={`px-3 py-1 rounded-md text-xs font-medium ${
                            statusFilter === 'all' 
                              ? 'bg-secondary-200 text-secondary-800' 
                              : 'bg-white text-secondary-600 hover:bg-secondary-100'
                          }`}
                        >
                          All
                        </button>
                        <button
                          type="button"
                          onClick={() => handleStatusFilter('passed')}
                          className={`px-3 py-1 rounded-md text-xs font-medium ${
                            statusFilter === 'passed' 
                              ? 'bg-accent-100 text-accent-800' 
                              : 'bg-white text-secondary-600 hover:bg-secondary-100'
                          }`}
                        >
                          <CheckCircleIcon className="inline-block h-3 w-3 mr-1" />
                          Passed
                        </button>
                        <button
                          type="button"
                          onClick={() => handleStatusFilter('failed')}
                          className={`px-3 py-1 rounded-md text-xs font-medium ${
                            statusFilter === 'failed' 
                              ? 'bg-danger-100 text-danger-800' 
                              : 'bg-white text-secondary-600 hover:bg-secondary-100'
                          }`}
                        >
                          <XCircleIcon className="inline-block h-3 w-3 mr-1" />
                          Failed
                        </button>
                        <button
                          type="button"
                          onClick={() => handleStatusFilter('unknown')}
                          className={`px-3 py-1 rounded-md text-xs font-medium ${
                            statusFilter === 'unknown' 
                              ? 'bg-secondary-300 text-secondary-800' 
                              : 'bg-white text-secondary-600 hover:bg-secondary-100'
                          }`}
                        >
                          Not Run
                        </button>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Enhanced Validation rule list */}
                <ValidationRuleList
                  validations={currentValidations}
                  isLoading={loadingValidations || loadingStates?.validations}
                  onEdit={handleEditValidation}
                  onDebug={handleDebugValidation}
                  onRefreshList={refreshValidations}
                  onUpdate={setCurrentValidations}
                  onRunSingle={handleRunSingleValidation}
                  isRunningValidation={loadingStates?.validations}
                  tableName={selectedTable}
                  connectionId={activeConnection?.id}
                />
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Debug Helper Modal */}
      {showDebugHelper && (
        <div className="fixed inset-0 bg-secondary-900 bg-opacity-75 z-50 flex items-center justify-center px-4">
          <ValidationDebugHelper
            connectionId={activeConnection?.id}
            tableName={selectedTable}
            validationRule={selectedValidationForDebug}
            onClose={handleCloseDebugHelper}
          />
        </div>
      )}
    </div>
  );
};

// Wrap the component with the provider for export
const ValidationPageWithProvider = () => {
  return (
    <ValidationResultsProvider>
      <ValidationPage />
    </ValidationResultsProvider>
  );
};

export default ValidationPageWithProvider;