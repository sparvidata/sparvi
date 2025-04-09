import React, { useState, useEffect, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useTablesData } from '../../hooks/useTablesData';
import useValidations from '../../hooks/useValidations';
import ValidationResultsSummary from './components/ValidationResultsSummary';
import ValidationResultsTrend from './components/ValidationResultsTrend';
import ValidationRuleList from './components/ValidationRuleList';
import ValidationRuleEditor from './components/ValidationRuleEditor';
import ValidationDebugHelper from './components/ValidationDebugHelper';
import ValidationErrorHandler from './components/ValidationErrorHandler';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import EmptyState from '../../components/common/EmptyState';
import { ClipboardDocumentCheckIcon, ServerIcon } from '@heroicons/react/24/outline';
import validationService from '../../services/validationService';

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
  const connectionId = activeConnection?.id;
  const tablesQuery = useTablesData(connectionId, { enabled: !!connectionId });
  const validationData = useValidations(connectionId, selectedTable);

  // Handle table selection
  const handleTableSelect = (tableName) => {
    setSelectedTable(tableName);
    setSearchParams({ table: tableName });
  };

  // Extract validations with errors
  const errorValidations = useMemo(() => {
    if (!validationData.validations) return [];
    return validationData.validations.filter(v => v.error);
  }, [validationData.validations]);

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
  const handleSaveValidation = async () => {
    await validationData.loadValidations(true);
    setShowEditor(false);
    setEditingRule(null);
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
              onSave={handleSaveValidation}
              onCancel={() => {
                setShowEditor(false);
                setEditingRule(null);
              }}
            />
          ) : (
            <div className="space-y-4">
              {/* Results Summary */}
              {selectedTable && (
                <div className="space-y-4">
                  <ValidationResultsSummary
                    metrics={validationData.metrics}
                    connectionId={connectionId}
                    tableName={selectedTable}
                    onRunAll={validationData.runAllValidations}
                    isRunning={validationData.runningValidation}
                  />
                  <ValidationResultsTrend
                    trendData={validationData.trends}
                    isLoading={validationData.loadingHistory}
                    tableName={selectedTable}
                  />
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

                {/* Enhanced Validation rule list */}
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