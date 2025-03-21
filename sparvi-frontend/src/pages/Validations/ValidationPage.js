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
import SearchInput from '../../components/common/SearchInput';
import { useTablesData } from '../../hooks/useTablesData';
import { useTableValidations } from '../../hooks/useValidationsData';
import { queryClient } from '../../api/queryClient';

const ValidationPage = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { connections, activeConnection, setCurrentConnection } = useConnection();
  const { updateBreadcrumbs, showNotification, setLoading } = useUI();

  const [currentValidations, setCurrentValidations] = useState([]);
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
    }
  }, [searchParams]);

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

      // Only auto-run if no validation has results yet
      if (!validationsQuery.data.some(v => v.last_result !== undefined)) {
        console.log("Running initial validations");

        // Use a setTimeout to prevent potential render loop issues
        setTimeout(() => {
          validationsAPI.runValidations(
            activeConnection.id,
            selectedTable,
            null
          ).then(response => {
            // Process response and update state...
            if (response?.results) {
              // Update validation results in component state
              const newValidations = updateValidationsWithResults(
                currentValidations,
                response.results
              );
              setCurrentValidations(newValidations);
            }
          }).catch(error => {
            console.error("Error running initial validations:", error);
          });
        }, 500); // Add a small delay
      }
    }
  }, [validationsQuery.data, initialLoadComplete, activeConnection?.id, selectedTable]);

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

  // Handle run all validations with status updating
  const handleRunAll = async () => {
    if (!activeConnection || !selectedTable) return;

    try {
      setLoading('validations', true);
      setValidationErrors(null);

      const response = await validationsAPI.runValidations(
        activeConnection.id,  // Pass connectionId first
        selectedTable,
        null
      );

      console.log("Full validation response:", response);

      // Check if we have a valid response with results
      if (response && response.results && Array.isArray(response.results)) {
        // Process each validation result
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
        if (currentValidations.length > 0) {
          const updatedValidations = currentValidations.map(validation => {
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

          // Update the current validations with new results
          setCurrentValidations(updatedValidations);
        }

        if (errors.length > 0) {
          setValidationErrors(errors);
          showNotification(`${errors.length} validations encountered errors. See details below.`, 'warning');
        } else {
          // Clear any previous errors
          setValidationErrors(null);

          // Count successes and failures
          const passedCount = response.results.filter(r => r.is_valid === true).length;
          const failedCount = response.results.filter(r => r.is_valid === false).length;

          showNotification(
            `Ran ${response.results.length} validations: ${passedCount} passed, ${failedCount} failed`,
            failedCount > 0 ? 'warning' : 'success'
          );
        }

        // Invalidate the validations query to trigger a refetch after a short delay
        // This helps ensure our UI updates have time to apply first
        setTimeout(() => {
          queryClient.invalidateQueries(['table-validations', selectedTable]);
        }, 500);
      } else {
        showNotification('Unexpected response format from server', 'error');
      }
    } catch (error) {
      console.error('Error running validations:', error);
      showNotification('Failed to run validations. Error: ' + (error.message || 'Unknown error'), 'error');
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

      const response = await validationsAPI.generateDefaultValidations(
        activeConnection.id,  // Pass connectionId first
        selectedTable,
        null
      );

      // Invalidate the query to refetch validation rules
      queryClient.invalidateQueries(['table-validations', selectedTable]);

      showNotification(`Generated ${response.data.count} default validations for ${selectedTable}`, 'success');
    } catch (error) {
      console.error('Error generating validations:', error);
      showNotification('Failed to generate validations', 'error');
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
                    onClick={handleRunAll}
                    className="inline-flex items-center px-3 py-1.5 border border-secondary-300 shadow-sm text-sm font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-secondary-500"
                  >
                    <ArrowPathIcon className="-ml-1 mr-2 h-4 w-4" aria-hidden="true" />
                    Run All
                  </button>

                  <button
                    type="button"
                    onClick={handleNewValidation}
                    className="inline-flex items-center px-3 py-1.5 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                  >
                    <PlusIcon className="-ml-1 mr-2 h-4 w-4" aria-hidden="true" />
                    New Rule
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

              {/* Search and filters */}
              <div className="px-4 py-3 border-b border-secondary-200 bg-secondary-50 sm:px-6">
                <div className="flex items-center justify-between">
                  <div className="flex-1">
                    <SearchInput
                      onSearch={handleSearch}
                      placeholder="Search validations..."
                      initialValue={searchQuery}
                    />
                  </div>

                  <div className="flex items-center space-x-2">
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

              {/* Validation list */}
              <ValidationRuleList
                validations={filteredValidations}
                isLoading={validationsQuery.isLoading}
                onEdit={handleEditValidation}
                onDebug={handleDebugValidation}
                onRefreshList={() => validationsQuery.refetch()}
                onUpdate={setCurrentValidations}
                tableName={selectedTable}
                connectionId={activeConnection?.id}
              />
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

export default ValidationPage;