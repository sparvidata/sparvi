import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useTablesData } from '../../hooks/useTablesData';
import { useAutomationStatus } from '../../hooks/useAutomationStatus';
import useValidations from '../../hooks/useValidations';

// Enhanced components
import ValidationErrorBoundary from './components/ValidationErrorBoundary';
import ValidationHealthDashboard from './components/ValidationHealthDashboard';
import ValidationResultsSummary from './components/ValidationResultsSummary';
import ValidationOverviewDashboard from './components/ValidationOverviewDashboard';
import ValidationRuleList from './components/ValidationRuleList';
import ValidationRuleEditor from './components/ValidationRuleEditor';
import ValidationDebugHelper from './components/ValidationDebugHelper';
import ValidationErrorHandler from './components/ValidationErrorHandler';
import ValidationAutomationControls from './components/ValidationAutomationControls';

// Common components
import LoadingSpinner from '../../components/common/LoadingSpinner';
import EmptyState from '../../components/common/EmptyState';
import { ServerIcon, ArrowLeftIcon, ExclamationTriangleIcon } from '@heroicons/react/24/outline';

// Services
import validationService from '../../services/validationService';
import { schemaAPI } from '../../api/enhancedApiService';

const ValidationPage = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { activeConnection } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();

  // Local state
  const [selectedTable, setSelectedTable] = useState('');
  const [showEditor, setShowEditor] = useState(false);
  const [editingRule, setEditingRule] = useState(null);
  const [showDebugHelper, setShowDebugHelper] = useState(false);
  const [selectedValidationForDebug, setSelectedValidationForDebug] = useState(null);
  const [validationErrors, setValidationErrors] = useState(null);
  const [columnsCache, setColumnsCache] = useState({});
  const [pageError, setPageError] = useState(null);

  // Connection and automation data
  const connectionId = activeConnection?.id;
  const { status: automationStatus, toggleAutomation, triggerManualRun } = useAutomationStatus(connectionId);

  // Set breadcrumbs based on selected table
  useEffect(() => {
    try {
      if (selectedTable) {
        updateBreadcrumbs([
          { name: 'Validations', href: '/validations' },
          { name: selectedTable }
        ]);
      } else {
        updateBreadcrumbs([
          { name: 'Validations', href: '/validations' }
        ]);
      }
    } catch (error) {
      console.error('Error updating breadcrumbs:', error);
    }
  }, [updateBreadcrumbs, selectedTable]);

  // Handle table selection from URL params
  useEffect(() => {
    try {
      const tableParam = searchParams.get('table');
      if (tableParam && tableParam !== selectedTable) {
        setSelectedTable(tableParam);
      } else if (!tableParam && selectedTable) {
        setSelectedTable('');
      }
    } catch (error) {
      console.error('Error handling URL params:', error);
      setPageError('Failed to process URL parameters');
    }
  }, [searchParams, selectedTable]);

  // Enhanced data fetching with error handling
  const tablesQuery = useTablesData(connectionId, {
    enabled: !!connectionId,
    onError: (error) => {
      console.error('Error loading tables:', error);
      showNotification('Failed to load tables list', 'error');
    }
  });

  const validationData = useValidations(connectionId, selectedTable);

  // Enhanced column preloading with error handling
  const preloadTableColumns = useCallback(async (tableName) => {
    if (!connectionId || !tableName || columnsCache[tableName]) return;

    try {
      console.log(`[ValidationPage] Preloading columns for table: ${tableName}`);
      const response = await schemaAPI.getColumns(connectionId, tableName);

      // Handle different possible response structures
      let columnsData = [];
      if (response?.columns && Array.isArray(response.columns)) {
        columnsData = response.columns;
      } else if (response?.data?.columns && Array.isArray(response.data.columns)) {
        columnsData = response.data.columns;
      } else if (Array.isArray(response)) {
        columnsData = response;
      }

      // Update cache
      setColumnsCache(prev => ({
        ...prev,
        [tableName]: columnsData
      }));

      console.log(`[ValidationPage] Cached ${columnsData.length} columns for ${tableName}`);
    } catch (error) {
      console.error(`[ValidationPage] Error preloading columns for ${tableName}:`, error);
      // Don't show notification for column preloading errors as they're not critical
    }
  }, [connectionId, columnsCache]);

  // Preload columns when table is selected
  useEffect(() => {
    if (selectedTable) {
      preloadTableColumns(selectedTable);
    }
  }, [selectedTable, preloadTableColumns]);

  // Enhanced table selection with error handling
  const handleTableSelect = useCallback((tableName) => {
    try {
      setSelectedTable(tableName);
      setSearchParams({ table: tableName });
      preloadTableColumns(tableName);

      // Reset error states when changing tables
      setValidationErrors(null);
      setPageError(null);
    } catch (error) {
      console.error('Error selecting table:', error);
      showNotification('Failed to select table', 'error');
    }
  }, [setSearchParams, preloadTableColumns, showNotification]);

  // Enhanced back to overview handler
  const handleBackToOverview = useCallback(() => {
    try {
      setSelectedTable('');
      setSearchParams({});
      // Reset all table-specific state
      setShowEditor(false);
      setEditingRule(null);
      setShowDebugHelper(false);
      setSelectedValidationForDebug(null);
      setValidationErrors(null);
      setPageError(null);
    } catch (error) {
      console.error('Error returning to overview:', error);
      showNotification('Failed to return to overview', 'error');
    }
  }, [setSearchParams, showNotification]);

  // Extract validations with errors with safety checks
  const errorValidations = useMemo(() => {
    try {
      if (!validationData.validations) return [];
      return validationData.validations.filter(v => v.error);
    } catch (error) {
      console.error('Error filtering error validations:', error);
      return [];
    }
  }, [validationData.validations]);

  // Set validation errors when they occur
  useEffect(() => {
    if (errorValidations.length > 0) {
      setValidationErrors(errorValidations);
    } else {
      setValidationErrors(null);
    }
  }, [errorValidations]);

  // Enhanced validation generation handler
  const handleGenerateValidations = useCallback(async () => {
    if (!connectionId || !selectedTable) return;

    try {
      showNotification('Generating default validations...', 'info');

      await validationService.generateDefaultValidations(connectionId, selectedTable);

      // Reload validations after generation
      await validationData.loadValidations(true);

      showNotification('Default validations generated successfully', 'success');
    } catch (err) {
      console.error('Error generating validations:', err);
      showNotification(`Failed to generate validations: ${err.message}`, 'error');
    }
  }, [connectionId, selectedTable, validationData, showNotification]);

  // Enhanced validation save handler
  const handleSaveValidation = useCallback(() => {
    try {
      // First close the editor immediately
      setShowEditor(false);
      setEditingRule(null);

      // Then load validations in the background
      validationData.loadValidations(true).catch(err => {
        console.error('Error reloading validations:', err);
        showNotification('Rule was saved, but there was an error refreshing the list', 'warning');
      });
    } catch (error) {
      console.error('Error in save validation handler:', error);
      showNotification('Error processing validation save', 'error');
    }
  }, [validationData, showNotification]);

  // Enhanced run all validations handler
  const handleRunAll = useCallback(async () => {
    try {
      showNotification('Running validations...', 'info');

      // Run all validations
      await validationData.runAllValidations();

      // Force refresh validations data
      await validationData.loadValidations(true);

      showNotification('Validations completed successfully', 'success');
    } catch (err) {
      console.error('Error running validations:', err);
      showNotification(`Failed to run validations: ${err.message}`, 'error');
    }
  }, [validationData, showNotification]);

  // Enhanced optimistic UI updates handler
  const handleUpdateValidations = useCallback((updatedValidations) => {
    try {
      if (!validationData) return;

      // Update the validations in the hook's data
      validationData.validations = updatedValidations;

      // Update metrics
      if (validationData.metrics) {
        const updatedMetrics = validationService.calculateMetrics(updatedValidations);
        validationData.metrics = updatedMetrics;
      }
    } catch (error) {
      console.error('Error updating validations:', error);
    }
  }, [validationData]);

  // Enhanced automation handlers
  const handleToggleValidationAutomation = useCallback(async () => {
    try {
      const currentState = automationStatus.connection_config?.validation_automation?.enabled;
      const success = await toggleAutomation('validation_automation', !currentState);

      if (success) {
        showNotification(
          `Validation automation ${!currentState ? 'enabled' : 'disabled'}`,
          'success'
        );
      } else {
        showNotification('Failed to toggle validation automation', 'error');
      }
    } catch (error) {
      console.error('Error toggling validation automation:', error);
      showNotification('Error toggling validation automation', 'error');
    }
  }, [automationStatus, toggleAutomation, showNotification]);

  const handleTriggerAutomatedRun = useCallback(async () => {
    try {
      const success = await triggerManualRun('validation_automation');

      if (success) {
        showNotification('Validation automation triggered', 'success');
      } else {
        showNotification('Failed to trigger validation automation', 'error');
      }
    } catch (error) {
      console.error('Error triggering automated run:', error);
      showNotification('Error triggering automation', 'error');
    }
  }, [triggerManualRun, showNotification]);

  // Show page-level error if one occurred
  if (pageError) {
    return (
      <div className="py-4">
        <div className="bg-danger-50 border border-danger-200 rounded-lg p-6">
          <div className="flex items-center">
            <ExclamationTriangleIcon className="h-6 w-6 text-danger-400 mr-3" />
            <div>
              <h3 className="text-lg font-medium text-danger-800">Page Error</h3>
              <p className="mt-1 text-danger-700">{pageError}</p>
              <button
                onClick={() => window.location.reload()}
                className="mt-3 px-4 py-2 bg-danger-100 text-danger-800 rounded-md hover:bg-danger-200"
              >
                Reload Page
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // If no connections, show empty state
  if (!activeConnection) {
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
    <ValidationErrorBoundary
      tableName={selectedTable}
      connectionId={connectionId}
    >
      <div className="py-4">
        <div className="flex justify-between items-center">
          <div className="flex items-center">
            {/* Back button when viewing a specific table */}
            {selectedTable && (
              <button
                onClick={handleBackToOverview}
                className="mr-4 inline-flex items-center px-3 py-2 border border-secondary-300 shadow-sm text-sm leading-4 font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
              >
                <ArrowLeftIcon className="-ml-0.5 mr-2 h-4 w-4" />
                Back to Overview
              </button>
            )}

            <h1 className="text-2xl font-semibold text-secondary-900">
              {selectedTable ? `Validations: ${selectedTable}` : 'Data Validations'}
            </h1>
          </div>

          {/* Optional: Add a breadcrumb-style navigation in the header */}
          {selectedTable && (
            <div className="text-sm text-secondary-500">
              <button
                onClick={handleBackToOverview}
                className="text-primary-600 hover:text-primary-700 hover:underline"
              >
                All Tables
              </button>
              <span className="mx-2">›</span>
              <span>{selectedTable}</span>
            </div>
          )}
        </div>

        <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-12">
          {/* Side panel with tables and automation controls */}
          <div className="lg:col-span-3 space-y-6">
            {/* Validation Automation Controls */}
            <ValidationErrorBoundary>
              <ValidationAutomationControls
                connectionId={connectionId}
                tableName={selectedTable}
                automationStatus={automationStatus}
                onToggleValidationAutomation={handleToggleValidationAutomation}
                onTriggerAutomatedRun={handleTriggerAutomatedRun}
              />
            </ValidationErrorBoundary>

            {/* Tables List */}
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
                        className={`w-full text-left px-3 py-2 rounded-md text-sm transition-colors ${
                          selectedTable === table 
                            ? 'bg-primary-50 text-primary-700 font-medium' 
                            : 'text-secondary-700 hover:bg-secondary-100 hover:text-secondary-900'
                        }`}
                        onClick={() => handleTableSelect(table)}
                      >
                        <div className="flex items-center">
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
              /* Show overview dashboard when no table is selected */
              <ValidationErrorBoundary>
                <ValidationOverviewDashboard
                  connectionId={connectionId}
                  onTableSelect={handleTableSelect}
                />
              </ValidationErrorBoundary>
            ) : showEditor ? (
              <ValidationErrorBoundary tableName={selectedTable}>
                <ValidationRuleEditor
                  connectionId={connectionId}
                  tableName={selectedTable}
                  rule={editingRule}
                  cachedColumns={columnsCache[selectedTable] || []}
                  onSave={handleSaveValidation}
                  onCancel={() => {
                    setShowEditor(false);
                    setEditingRule(null);
                  }}
                />
              </ValidationErrorBoundary>
            ) : (
              <div className="space-y-6">
                {/* Results Summary */}
                {selectedTable && (
                  <ValidationErrorBoundary>
                    <ValidationResultsSummary
                      metrics={validationData.metrics}
                      connectionId={connectionId}
                      tableName={selectedTable}
                      onRunAll={handleRunAll}
                      isRunning={validationData.runningValidation}
                      isLoading={validationData.loading}
                    />
                  </ValidationErrorBoundary>
                )}

                {/* Enhanced Health Dashboard with Error Boundary */}
                {selectedTable && (
                  <ValidationErrorBoundary tableName={selectedTable}>
                    <ValidationHealthDashboard
                      connectionId={connectionId}
                      tableName={selectedTable}
                      days={30}
                    />
                  </ValidationErrorBoundary>
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
                        Generate Default
                      </button>

                      <button
                        type="button"
                        onClick={() => {
                          setEditingRule(null);
                          setShowEditor(true);
                        }}
                        className="inline-flex items-center px-3 py-1.5 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                      >
                        New Rule
                      </button>
                    </div>
                  </div>

                  {/* Display validation errors at the top if present */}
                  {validationErrors && validationErrors.length > 0 && (
                    <div className="px-4 py-3 bg-white">
                      <ValidationErrorHandler
                        errors={validationErrors}
                        onRefresh={validationData.runAllValidations}
                        connectionId={connectionId}
                        tableName={selectedTable}
                      />
                    </div>
                  )}

                  {/* Enhanced Validation rule list with error boundary */}
                  <ValidationErrorBoundary tableName={selectedTable}>
                    <ValidationRuleList
                      validations={validationData.validations}
                      isLoading={validationData.loading}
                      onEdit={(rule) => {
                        setEditingRule(rule);
                        setShowEditor(true);
                      }}
                      onDebug={(rule) => {
                        setSelectedValidationForDebug(rule);
                        setShowDebugHelper(true);
                      }}
                      onRefreshList={() => validationData.loadValidations(true)}
                      onUpdateValidations={handleUpdateValidations}
                      tableName={selectedTable}
                      connectionId={connectionId}
                      onRunSingle={validationData.runSingleValidation}
                      isRunningValidation={validationData.runningValidation}
                      runningRuleId={validationData.runningRuleId}
                      showNotification={showNotification}
                    />
                  </ValidationErrorBoundary>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Debug Helper Modal */}
        {showDebugHelper && (
          <div className="fixed inset-0 bg-secondary-900 bg-opacity-75 z-50 flex items-center justify-center px-4">
            <ValidationErrorBoundary>
              <ValidationDebugHelper
                connectionId={connectionId}
                tableName={selectedTable}
                validationRule={selectedValidationForDebug}
                onClose={() => {
                  setShowDebugHelper(false);
                  setSelectedValidationForDebug(null);
                }}
              />
            </ValidationErrorBoundary>
          </div>
        )}
      </div>
    </ValidationErrorBoundary>
  );
};

export default ValidationPage;