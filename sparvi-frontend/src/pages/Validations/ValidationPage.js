import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useTablesData } from '../../hooks/useTablesData';
import { useAutomationStatus } from '../../hooks/useAutomationStatus';
import useValidations from '../../hooks/useValidations';
import ValidationResultsSummary from './components/ValidationResultsSummary';
import ValidationHealthDashboard from './components/ValidationHealthDashboard';
import ValidationRuleList from './components/ValidationRuleList';
import ValidationRuleEditor from './components/ValidationRuleEditor';
import ValidationDebugHelper from './components/ValidationDebugHelper';
import ValidationErrorHandler from './components/ValidationErrorHandler';
import ValidationAutomationControls from './components/ValidationAutomationControls';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import EmptyState from '../../components/common/EmptyState';
import { ClipboardDocumentCheckIcon, ServerIcon } from '@heroicons/react/24/outline';
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
  const [forceRender, setForceRender] = useState(false); // Used to force re-renders

  // Automation hooks
  const connectionId = activeConnection?.id;
  const { status: automationStatus, toggleAutomation, triggerManualRun } = useAutomationStatus(connectionId);

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

  // Use hooks for data fetching
  const tablesQuery = useTablesData(connectionId, { enabled: !!connectionId });
  const validationData = useValidations(connectionId, selectedTable);

  // Preload columns for selected table
  const preloadTableColumns = useCallback(async (tableName) => {
    if (!connectionId || !tableName || columnsCache[tableName]) return;

    try {
      console.log(`Preloading columns for table: ${tableName}`);
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

      console.log(`Cached ${columnsData.length} columns for ${tableName}`);
    } catch (error) {
      console.error(`Error preloading columns for ${tableName}:`, error);
    }
  }, [connectionId, columnsCache]);

  // Preload columns when table is selected
  useEffect(() => {
    if (selectedTable) {
      preloadTableColumns(selectedTable);
    }
  }, [selectedTable, preloadTableColumns]);

  // Handle table selection
  const handleTableSelect = (tableName) => {
    setSelectedTable(tableName);
    setSearchParams({ table: tableName });
    preloadTableColumns(tableName);
  };

  // Extract validations with errors
  const errorValidations = useMemo(() => {
    if (!validationData.validations) return [];
    return validationData.validations.filter(v => v.error);
  }, [validationData.validations, forceRender]); // Add forceRender to dependencies

  // Set validation errors when they occur
  useEffect(() => {
    if (errorValidations.length > 0) {
      setValidationErrors(errorValidations);
    } else {
      setValidationErrors(null);
    }
  }, [errorValidations]);

  // Handle generating default validations
  const handleGenerateValidations = async () => {
    if (!connectionId || !selectedTable) return;

    try {
      showNotification('Generating default validations...', 'info');

      await validationService.generateDefaultValidations(connectionId, selectedTable);

      // Reload validations after generation
      await validationData.loadValidations(true);

      showNotification('Default validations generated successfully', 'success');
    } catch (err) {
      showNotification(`Failed to generate validations: ${err.message}`, 'error');
    }
  };

  // Handle saving a validation rule
  const handleSaveValidation = () => {
    // First close the editor immediately
    setShowEditor(false);
    setEditingRule(null);

    // Then load validations in the background
    validationData.loadValidations(true).catch(err => {
      console.error('Error reloading validations:', err);
      showNotification('Rule was saved, but there was an error refreshing the list', 'warning');
    });
  };

  // Handle running all validations
  const handleRunAll = async () => {
    try {
      showNotification('Running validations...', 'info');

      // Run all validations
      await validationData.runAllValidations();

      // Force refresh validations data
      await validationData.loadValidations(true);

      // Show success notification
      showNotification('Validations completed successfully', 'success');
    } catch (err) {
      showNotification(`Failed to run validations: ${err.message}`, 'error');
    }
  };

  // Handle optimistic UI updates to validation list
  const handleUpdateValidations = useCallback((updatedValidations) => {
    if (!validationData) return;

    // Update the validations in the hook's data
    validationData.validations = updatedValidations;

    // Force a re-render
    setForceRender(prev => !prev);

    // Update metrics
    if (validationData.metrics) {
      const updatedMetrics = validationService.calculateMetrics(updatedValidations);
      validationData.metrics = updatedMetrics;
    }
  }, [validationData]);

  // Handle automation toggle for validation automation
  const handleToggleValidationAutomation = async () => {
    const success = await toggleAutomation('validation_automation', !automationStatus.connection_config?.validation_automation?.enabled);
    if (success) {
      showNotification(
        `Validation automation ${!automationStatus.connection_config?.validation_automation?.enabled ? 'enabled' : 'disabled'}`,
        'success'
      );
    } else {
      showNotification('Failed to toggle validation automation', 'error');
    }
  };

  // Handle triggering automated validation run
  const handleTriggerAutomatedRun = async () => {
    const success = await triggerManualRun('validation_automation');
    if (success) {
      showNotification('Validation automation triggered', 'success');
    } else {
      showNotification('Failed to trigger validation automation', 'error');
    }
  };

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
    <div className="py-4">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold text-secondary-900">Data Validations</h1>
      </div>

      <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-12">
        {/* Side panel with tables and automation controls */}
        <div className="lg:col-span-3 space-y-6">
          {/* Validation Automation Controls */}
          <ValidationAutomationControls
            connectionId={connectionId}
            tableName={selectedTable}
            automationStatus={automationStatus}
            onToggleValidationAutomation={handleToggleValidationAutomation}
            onTriggerAutomatedRun={handleTriggerAutomatedRun}
          />

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
                      className={`w-full text-left px-3 py-2 rounded-md text-sm ${
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
            <div className="bg-white shadow rounded-lg p-6 text-center">
              <ClipboardDocumentCheckIcon className="mx-auto h-12 w-12 text-secondary-400" />
              <h3 className="mt-2 text-sm font-medium text-secondary-900">Select a table to see validations</h3>
              <p className="mt-1 text-sm text-secondary-500">
                Choose a table from the list to view and manage validation rules.
              </p>
            </div>
          ) : showEditor ? (
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
          ) : (
            <div className="space-y-6">
              {/* Results Summary */}
              {selectedTable && (
                <ValidationResultsSummary
                  metrics={validationData.metrics}
                  connectionId={connectionId}
                  tableName={selectedTable}
                  onRunAll={handleRunAll}
                  isRunning={validationData.runningValidation}
                  isLoading={validationData.loading}
                />
              )}

              {/* NEW: Modular Health Dashboard */}
              {selectedTable && (
                <ValidationHealthDashboard
                  connectionId={connectionId}
                  tableName={selectedTable}
                  days={30}
                />
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

                {/* Enhanced Validation rule list with optimistic updates */}
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
                  onUpdateValidations={handleUpdateValidations} // New prop for optimistic updates
                  tableName={selectedTable}
                  connectionId={connectionId}
                  onRunSingle={validationData.runSingleValidation}
                  isRunningValidation={validationData.runningValidation}
                  runningRuleId={validationData.runningRuleId}
                  showNotification={showNotification}
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
            connectionId={connectionId}
            tableName={selectedTable}
            validationRule={selectedValidationForDebug}
            onClose={() => {
              setShowDebugHelper(false);
              setSelectedValidationForDebug(null);
            }}
          />
        </div>
      )}
    </div>
  );
};

export default ValidationPage;