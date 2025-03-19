import React, { useState, useEffect } from 'react';
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
import { useConnection } from '../../contexts/ConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { validationsAPI, schemaAPI } from '../../api/enhancedApiService';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import EmptyState from '../../components/common/EmptyState';
import ValidationRuleEditor from './components/ValidationRuleEditor';
import ValidationRuleList from './components/ValidationRuleList';
import SearchInput from '../../components/common/SearchInput';

const ValidationPage = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { connections, activeConnection, setCurrentConnection } = useConnection();
  const { updateBreadcrumbs, showNotification, setLoading } = useUI();

  const [validations, setValidations] = useState([]);
  const [filteredValidations, setFilteredValidations] = useState([]);
  const [tables, setTables] = useState([]);
  const [loading, setIsLoading] = useState(true);
  const [selectedTable, setSelectedTable] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');
  const [showEditor, setShowEditor] = useState(false);
  const [editingRule, setEditingRule] = useState(null);

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

  // Load tables for active connection
  useEffect(() => {
    const loadTables = async () => {
      if (!activeConnection) return;

      try {
        setIsLoading(true);

        const response = await schemaAPI.getTables(activeConnection.id);
        setTables(response.data.tables || []);
      } catch (error) {
        console.error('Error loading tables:', error);
        showNotification('Failed to load tables', 'error');
      } finally {
        setIsLoading(false);
      }
    };

    loadTables();
  }, [activeConnection, showNotification]);

  // Load validations for selected table
  useEffect(() => {
    const loadValidations = async () => {
      if (!selectedTable) {
        setValidations([]);
        setFilteredValidations([]);
        return;
      }

      try {
        setIsLoading(true);

        const response = await validationsAPI.getRules(selectedTable);
        setValidations(response.data.rules || []);
        setFilteredValidations(response.data.rules || []);
      } catch (error) {
        console.error(`Error loading validations for ${selectedTable}:`, error);
        showNotification(`Failed to load validations for ${selectedTable}`, 'error');
      } finally {
        setIsLoading(false);
      }
    };

    loadValidations();
  }, [selectedTable, showNotification]);

  // Apply filters and search
  useEffect(() => {
    if (!validations.length) {
      setFilteredValidations([]);
      return;
    }

    let filtered = [...validations];

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

    setFilteredValidations(filtered);
  }, [validations, searchQuery, statusFilter]);

  // Handle table selection
  const handleTableSelect = (tableName) => {
    setSelectedTable(tableName);
    setSearchParams({ table: tableName });
  };

  // Handle search
  const handleSearch = (query) => {
    setSearchQuery(query);
  };

  // Handle status filter
  const handleStatusFilter = (status) => {
    setStatusFilter(status);
  };

  // Handle run all validations
  const handleRunAll = async () => {
    if (!activeConnection || !selectedTable) return;

    try {
      setLoading('validations', true);

      const response = await validationsAPI.runValidations(
        activeConnection.id,
        selectedTable,
        null
      );

      // Refresh validation list
      const validationsResponse = await validationsAPI.getRules(selectedTable);
      setValidations(validationsResponse.data.rules || []);

      showNotification(`Ran ${response.data.results?.length || 0} validations for ${selectedTable}`, 'success');
    } catch (error) {
      console.error('Error running validations:', error);
      showNotification('Failed to run validations', 'error');
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
    // Refresh validation list
    try {
      const response = await validationsAPI.getRules(selectedTable);
      setValidations(response.data.rules || []);
    } catch (error) {
      console.error('Error refreshing validations:', error);
    }

    setShowEditor(false);
    setEditingRule(null);
  };

  // Generate default validations
  const handleGenerateValidations = async () => {
    if (!activeConnection || !selectedTable) return;

    try {
      setLoading('generating', true);

      const response = await validationsAPI.generateDefaultValidations(
        activeConnection.id,
        selectedTable,
        null
      );

      // Refresh validation list
      const validationsResponse = await validationsAPI.getRules(selectedTable);
      setValidations(validationsResponse.data.rules || []);

      showNotification(`Generated ${response.data.count} default validations for ${selectedTable}`, 'success');
    } catch (error) {
      console.error('Error generating validations:', error);
      showNotification('Failed to generate validations', 'error');
    } finally {
      setLoading('generating', false);
    }
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
              {loading && !tables.length ? (
                <div className="flex justify-center py-4">
                  <LoadingSpinner size="md" />
                </div>
              ) : tables.length === 0 ? (
                <div className="text-center py-4">
                  <p className="text-sm text-secondary-500">No tables found</p>
                </div>
              ) : (
                <div className="space-y-1 max-h-96 overflow-y-auto">
                  {tables.map(table => (
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

              {/* Search and filters */}
              <div className="px-4 py-3 border-b border-secondary-200 bg-secondary-50 sm:px-6">
                <div className="flex items-center justify-between">
                  <div className="flex-1">
                    <SearchInput
                      onSearch={handleSearch}
                      placeholder="Search tables..."
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
                isLoading={loading}
                onEdit={handleEditValidation}
                onRefreshList={handleSaveValidation}
                tableName={selectedTable}
                connectionId={activeConnection?.id}
              />
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ValidationPage;