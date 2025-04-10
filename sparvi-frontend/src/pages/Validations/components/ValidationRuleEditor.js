// Updated ValidationRuleEditor.js with working Simple Builder functionality

import React, { useState, useEffect } from 'react';
import {
  XMarkIcon,
  CheckIcon,
  CodeBracketIcon,
  LightBulbIcon
} from '@heroicons/react/24/outline';
import { useUI } from '../../../contexts/UIContext';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { validationsAPI, schemaAPI } from '../../../api/enhancedApiService';

const ValidationRuleEditor = ({
  connectionId,
  tableName,
  rule = null,
  cachedColumns = [],
  onSave,
  onCancel
}) => {
  const { showNotification } = useUI();

  // Form state
  const [formData, setFormData] = useState({
    name: '',
    description: '',
    query: '',
    operator: '=',
    expected_value: '',
  });

  // Add state for builder selections
  const [selectedColumn, setSelectedColumn] = useState('');
  const [selectedValidationType, setSelectedValidationType] = useState('not_null');
  const [validationParams, setValidationParams] = useState({
    min: '',
    max: '',
    pattern: '',
    refTable: '',
    refColumn: ''
  });

  const [suggestions, setSuggestions] = useState([]);
  const [columns, setColumns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [loadingColumns, setLoadingColumns] = useState(false);
  const [errors, setErrors] = useState({});
  const [activeTab, setActiveTab] = useState('builder');  // 'builder' or 'advanced'

  // Initialize with existing rule data if editing
  useEffect(() => {
    if (rule) {
      setFormData({
        name: rule.rule_name || '',
        description: rule.description || '',
        query: rule.query || '',
        operator: rule.operator || '=',
        expected_value: rule.expected_value || '',
      });

      // Switch to advanced tab when editing an existing rule
      setActiveTab('advanced');
    }
  }, [rule]);

  // Load columns for the table
  useEffect(() => {
    const loadColumns = async () => {
      if (!connectionId || !tableName) return;

      // If we have cached columns, use them immediately
      if (cachedColumns.length > 0) {
        console.log(`Using ${cachedColumns.length} cached columns for ${tableName}`);
        setColumns(cachedColumns);
        return;
      }

      try {
        setLoadingColumns(true);

        const response = await schemaAPI.getColumns(connectionId, tableName);

        // Add debug log to inspect response structure
        console.log('Columns response:', response);

        // Handle different possible response structures
        let columnsData = [];

        if (response?.columns && Array.isArray(response.columns)) {
          // Direct columns array in response
          columnsData = response.columns;
        } else if (response?.data?.columns && Array.isArray(response.data.columns)) {
          // Nested in data property
          columnsData = response.data.columns;
        } else if (Array.isArray(response)) {
          // Response is the array itself
          columnsData = response;
        }

        console.log(`Found ${columnsData.length} columns for table ${tableName}`);
        setColumns(columnsData);
      } catch (error) {
        console.error(`Error loading columns for ${tableName}:`, error);
        showNotification(`Failed to load columns for ${tableName}`, 'error');
      } finally {
        setLoadingColumns(false);
      }
    };

    loadColumns();
  }, [connectionId, tableName, cachedColumns, showNotification]);

  // Handle input changes for main form
  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));

    // Clear error for this field
    if (errors[name]) {
      setErrors(prev => {
        const newErrors = { ...prev };
        delete newErrors[name];
        return newErrors;
      });
    }
  };

  // Handle column selection
  const handleColumnChange = (e) => {
    const column = e.target.value;
    setSelectedColumn(column);

    // Generate a descriptive name based on selection
    if (column) {
      const validationType = selectedValidationType;
      const columnName = column.split(' ')[0]; // Extract just the column name
      const ruleName = `${tableName}_${columnName}_${validationType}`;

      setFormData(prev => ({
        ...prev,
        name: ruleName,
        description: generateDescription(columnName, validationType)
      }));

      // Generate the query based on new selections
      generateQueryFromBuilder(column, validationType);
    }
  };

  // Handle validation type selection
  const handleValidationTypeChange = (e) => {
    const validationType = e.target.value;
    setSelectedValidationType(validationType);

    // Update rule name if column is already selected
    if (selectedColumn) {
      const columnName = selectedColumn.split(' ')[0]; // Extract just the column name
      const ruleName = `${tableName}_${columnName}_${validationType}`;

      setFormData(prev => ({
        ...prev,
        name: ruleName,
        description: generateDescription(columnName, validationType)
      }));

      // Generate the query based on new selections
      generateQueryFromBuilder(selectedColumn, validationType);
    }

    // Show additional params for certain validation types
    if (validationType === 'range') {
      setValidationParams({
        ...validationParams,
        showRange: true
      });
    } else {
      setValidationParams({
        ...validationParams,
        showRange: false
      });
    }
  };

  // Handle validation parameter changes (for range, pattern, etc.)
  const handleParamChange = (e) => {
    const { name, value } = e.target;
    setValidationParams(prev => ({
      ...prev,
      [name]: value
    }));

    // Regenerate query if needed
    if (selectedColumn && selectedValidationType) {
      generateQueryFromBuilder(selectedColumn, selectedValidationType, {
        ...validationParams,
        [name]: value
      });
    }
  };

  // Generate a description based on column and validation type
  const generateDescription = (columnName, validationType) => {
    switch (validationType) {
      case 'not_null':
        return `Ensure ${columnName} is not null`;
      case 'unique':
        return `Ensure ${columnName} values are unique`;
      case 'range':
        return `Ensure ${columnName} values are within valid range`;
      case 'pattern':
        return `Ensure ${columnName} values match the required pattern`;
      case 'referential':
        return `Ensure ${columnName} references valid values`;
      case 'custom':
        return `Custom validation for ${columnName}`;
      default:
        return `Validation rule for ${columnName}`;
    }
  };

  // Generate SQL query from builder selections
  const generateQueryFromBuilder = (column, validationType, params = validationParams) => {
    if (!column || !validationType) return;

    const columnName = column.split(' ')[0]; // Extract just the column name
    let query = '';
    let operator = '=';
    let expectedValue = '0';

    switch (validationType) {
      case 'not_null':
        query = `SELECT COUNT(*) FROM ${tableName} WHERE ${columnName} IS NULL`;
        break;
      case 'unique':
        query = `SELECT COUNT(*) FROM (SELECT ${columnName} FROM ${tableName} GROUP BY ${columnName} HAVING COUNT(*) > 1) as duplicates`;
        break;
      case 'range':
        if (params.min !== '' && params.max !== '') {
          query = `SELECT COUNT(*) FROM ${tableName} WHERE ${columnName} < ${params.min} OR ${columnName} > ${params.max}`;
        } else if (params.min !== '') {
          query = `SELECT COUNT(*) FROM ${tableName} WHERE ${columnName} < ${params.min}`;
        } else if (params.max !== '') {
          query = `SELECT COUNT(*) FROM ${tableName} WHERE ${columnName} > ${params.max}`;
        } else {
          query = `SELECT COUNT(*) FROM ${tableName} WHERE ${columnName} IS NULL`;
        }
        break;
      case 'pattern':
        // This syntax might need to be adjusted based on your database
        if (params.pattern) {
          query = `SELECT COUNT(*) FROM ${tableName} WHERE ${columnName} NOT REGEXP '${params.pattern}'`;
        } else {
          query = `SELECT COUNT(*) FROM ${tableName} WHERE ${columnName} IS NULL`;
        }
        break;
      case 'referential':
        if (params.refTable && params.refColumn) {
          query = `SELECT COUNT(*) FROM ${tableName} t1 LEFT JOIN ${params.refTable} t2 ON t1.${columnName} = t2.${params.refColumn} WHERE t1.${columnName} IS NOT NULL AND t2.${params.refColumn} IS NULL`;
        } else {
          query = `SELECT COUNT(*) FROM ${tableName} WHERE ${columnName} IS NULL`;
        }
        break;
      case 'custom':
        // For custom, we'll keep the query empty and let the user fill it in
        query = '';
        break;
      default:
        query = `SELECT COUNT(*) FROM ${tableName} WHERE ${columnName} IS NULL`;
    }

    // Update form data with generated query
    setFormData(prev => ({
      ...prev,
      query,
      operator,
      expected_value: expectedValue
    }));
  };

  // Validate form
  const validateForm = () => {
    const newErrors = {};

    if (!formData.name.trim()) {
      newErrors.name = 'Rule name is required';
    }

    if (!formData.query.trim()) {
      newErrors.query = 'Query is required';
    }

    if (!formData.expected_value.trim()) {
      newErrors.expected_value = 'Expected value is required';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  // Handle form submission
  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!validateForm()) return;

    try {
      setLoading(true);

      const ruleData = {
        name: formData.name,
        description: formData.description,
        query: formData.query,
        operator: formData.operator,
        expected_value: formData.expected_value
      };

      if (rule) {
        // Update existing rule
        await validationsAPI.updateRule(rule.id, tableName, ruleData, connectionId);
        showNotification('Validation rule updated successfully', 'success');
      } else {
        // Create new rule
        await validationsAPI.createRule(tableName, ruleData, connectionId);
        showNotification('Validation rule created successfully', 'success');
      }

      // Call onSave to refresh rules list
      if (onSave) onSave();
    } catch (error) {
      console.error('Error saving validation rule:', error);
      showNotification(`Failed to save validation rule: ${error.response?.data?.error || error.message}`, 'error');
    } finally {
      setLoading(false);
    }
  };

  // Handle generating rule suggestions
  const handleGenerateSuggestions = async () => {
    try {
      // Get first column name for examples if available
      const firstColumn = columns.length > 0 ? columns[0].name : 'id';

      setSuggestions([
        {
          name: `${tableName}_not_null`,
          description: `Ensure ${firstColumn} is not null`,
          query: `SELECT COUNT(*) FROM ${tableName} WHERE ${firstColumn} IS NULL`,
          operator: '=',
          expected_value: '0'
        },
        {
          name: `${tableName}_row_count`,
          description: `Ensure minimum row count`,
          query: `SELECT COUNT(*) FROM ${tableName}`,
          operator: '>',
          expected_value: '0'
        },
        {
          name: `${tableName}_unique_check`,
          description: `Ensure ${firstColumn} values are unique`,
          query: `SELECT COUNT(*) FROM (SELECT ${firstColumn} FROM ${tableName} GROUP BY ${firstColumn} HAVING COUNT(*) > 1) as duplicates`,
          operator: '=',
          expected_value: '0'
        }
      ]);
    } catch (error) {
      console.error('Error generating rule suggestions:', error);
      showNotification('Failed to generate rule suggestions', 'error');
    }
  };

  // Apply a suggestion
  const applySuggestion = (suggestion) => {
    setFormData({
      name: suggestion.name,
      description: suggestion.description,
      query: suggestion.query,
      operator: suggestion.operator,
      expected_value: suggestion.expected_value
    });

    setSuggestions([]);
  };

  // Render additional inputs based on validation type
  const renderAdditionalInputs = () => {
    switch (selectedValidationType) {
      case 'range':
        return (
          <div className="grid grid-cols-2 gap-4 mt-4">
            <div>
              <label className="block text-sm font-medium text-secondary-700">
                Minimum Value
              </label>
              <input
                type="text"
                name="min"
                value={validationParams.min}
                onChange={handleParamChange}
                className="mt-1 shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-secondary-300 rounded-md"
                placeholder="Min value"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-secondary-700">
                Maximum Value
              </label>
              <input
                type="text"
                name="max"
                value={validationParams.max}
                onChange={handleParamChange}
                className="mt-1 shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-secondary-300 rounded-md"
                placeholder="Max value"
              />
            </div>
          </div>
        );
      case 'pattern':
        return (
          <div className="mt-4">
            <label className="block text-sm font-medium text-secondary-700">
              Pattern (Regular Expression)
            </label>
            <input
              type="text"
              name="pattern"
              value={validationParams.pattern}
              onChange={handleParamChange}
              className="mt-1 shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-secondary-300 rounded-md"
              placeholder="e.g., ^[A-Za-z0-9]+$"
            />
            <p className="mt-1 text-xs text-secondary-500">
              Enter a regular expression pattern that values should match
            </p>
          </div>
        );
      case 'referential':
        return (
          <div className="grid grid-cols-2 gap-4 mt-4">
            <div>
              <label className="block text-sm font-medium text-secondary-700">
                Referenced Table
              </label>
              <input
                type="text"
                name="refTable"
                value={validationParams.refTable}
                onChange={handleParamChange}
                className="mt-1 shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-secondary-300 rounded-md"
                placeholder="e.g., customers"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-secondary-700">
                Referenced Column
              </label>
              <input
                type="text"
                name="refColumn"
                value={validationParams.refColumn}
                onChange={handleParamChange}
                className="mt-1 shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-secondary-300 rounded-md"
                placeholder="e.g., id"
              />
            </div>
          </div>
        );
      default:
        return null;
    }
  };

  return (
    <div className="bg-white shadow sm:rounded-lg overflow-hidden">
      <div className="px-4 py-5 sm:p-6">
        <h3 className="text-lg leading-6 font-medium text-secondary-900">
          {rule ? 'Edit Validation Rule' : 'Create Validation Rule'}
        </h3>

        {/* Tabs */}
        <div className="mt-3 border-b border-secondary-200">
          <nav className="-mb-px flex space-x-8">
            <button
              onClick={() => setActiveTab('builder')}
              className={`${
                activeTab === 'builder'
                  ? 'border-primary-500 text-primary-600'
                  : 'border-transparent text-secondary-500 hover:text-secondary-700 hover:border-secondary-300'
              } whitespace-nowrap pb-3 px-1 border-b-2 font-medium text-sm flex items-center`}
            >
              <CheckIcon
                className={`${
                  activeTab === 'builder' ? 'text-primary-500' : 'text-secondary-400'
                } -ml-0.5 mr-2 h-5 w-5`}
                aria-hidden="true"
              />
              Simple Builder
            </button>

            <button
              onClick={() => setActiveTab('advanced')}
              className={`${
                activeTab === 'advanced'
                  ? 'border-primary-500 text-primary-600'
                  : 'border-transparent text-secondary-500 hover:text-secondary-700 hover:border-secondary-300'
              } whitespace-nowrap pb-3 px-1 border-b-2 font-medium text-sm flex items-center`}
            >
              <CodeBracketIcon
                className={`${
                  activeTab === 'advanced' ? 'text-primary-500' : 'text-secondary-400'
                } -ml-0.5 mr-2 h-5 w-5`}
                aria-hidden="true"
              />
              Advanced SQL
            </button>
          </nav>
        </div>

        <form onSubmit={handleSubmit} className="mt-5 space-y-6">
          {/* Rule name and description */}
          <div className="grid grid-cols-1 gap-y-6 gap-x-4 sm:grid-cols-6">
            <div className="sm:col-span-3">
              <label htmlFor="name" className="block text-sm font-medium text-secondary-700">
                Rule Name*
              </label>
              <div className="mt-1">
                <input
                  type="text"
                  name="name"
                  id="name"
                  value={formData.name}
                  onChange={handleChange}
                  className={`shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-secondary-300 rounded-md ${
                    errors.name ? 'border-danger-300' : ''
                  }`}
                  placeholder="e.g., customers_email_check"
                />
                {errors.name && (
                  <p className="mt-2 text-sm text-danger-600">{errors.name}</p>
                )}
              </div>
            </div>

            <div className="sm:col-span-3">
              <label htmlFor="description" className="block text-sm font-medium text-secondary-700">
                Description
              </label>
              <div className="mt-1">
                <input
                  type="text"
                  name="description"
                  id="description"
                  value={formData.description}
                  onChange={handleChange}
                  className="shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-secondary-300 rounded-md"
                  placeholder="e.g., Check that all customer emails are valid"
                />
              </div>
            </div>
          </div>

          {/* Builder or Advanced view */}
          {activeTab === 'builder' ? (
            <div className="space-y-6">
              {/* Simple builder with column selection */}
              <div>
                <label className="block text-sm font-medium text-secondary-700">
                  Column to Validate
                </label>
                <div className="mt-1">
                  <select
                    className="shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-secondary-300 rounded-md"
                    disabled={loadingColumns}
                    value={selectedColumn}
                    onChange={handleColumnChange}
                  >
                    {loadingColumns ? (
                      <option>Loading columns...</option>
                    ) : columns.length === 0 ? (
                      <option>No columns available</option>
                    ) : (
                      <>
                        <option value="">Select a column</option>
                        {columns.map(column => (
                          <option key={column.name} value={`${column.name} (${column.type})`}>
                            {column.name} ({column.type})
                          </option>
                        ))}
                      </>
                    )}
                  </select>
                </div>
              </div>

              {/* Validation type */}
              <div>
                <label className="block text-sm font-medium text-secondary-700">
                  Validation Type
                </label>
                <div className="mt-1">
                  <select
                    className="shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-secondary-300 rounded-md"
                    value={selectedValidationType}
                    onChange={handleValidationTypeChange}
                  >
                    <option value="not_null">Not Null</option>
                    <option value="unique">Unique Values</option>
                    <option value="range">Value in Range</option>
                    <option value="pattern">Match Pattern</option>
                    <option value="referential">Referential Integrity</option>
                    <option value="custom">Custom SQL Query</option>
                  </select>
                </div>
              </div>

              {/* Additional inputs based on validation type */}
              {renderAdditionalInputs()}

              {/* Preview of generated SQL */}
              {formData.query && (
                <div className="mt-4">
                  <label className="block text-sm font-medium text-secondary-700">
                    Generated SQL Query
                  </label>
                  <div className="mt-1 bg-secondary-50 p-3 rounded-md border border-secondary-200">
                    <pre className="text-xs font-mono overflow-auto">
                      {formData.query}
                    </pre>
                  </div>
                </div>
              )}

              {/* Rule suggestions button */}
              <div className="flex justify-end">
                <button
                  type="button"
                  onClick={handleGenerateSuggestions}
                  className="inline-flex items-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md shadow-sm text-secondary-700 bg-secondary-100 hover:bg-secondary-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-secondary-500"
                >
                  <LightBulbIcon className="-ml-0.5 mr-2 h-4 w-4" aria-hidden="true" />
                  Suggest Rules
                </button>
              </div>
            </div>
          ) : (
            <div className="space-y-6">
              {/* Advanced SQL editor */}
              <div>
                <label htmlFor="query" className="block text-sm font-medium text-secondary-700">
                  SQL Query*
                </label>
                <div className="mt-1">
                  <textarea
                    id="query"
                    name="query"
                    rows={4}
                    value={formData.query}
                    onChange={handleChange}
                    className={`shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-secondary-300 rounded-md font-mono ${
                      errors.query ? 'border-danger-300' : ''
                    }`}
                    placeholder={`SELECT COUNT(*) FROM ${tableName} WHERE...`}
                  />
                  {errors.query && (
                    <p className="mt-2 text-sm text-danger-600">{errors.query}</p>
                  )}
                  <p className="mt-2 text-xs text-secondary-500">
                    The query should return a single value that will be compared against the expected value.
                  </p>
                </div>
              </div>

              <div className="grid grid-cols-1 gap-y-6 gap-x-4 sm:grid-cols-6">
                <div className="sm:col-span-2">
                  <label htmlFor="operator" className="block text-sm font-medium text-secondary-700">
                    Operator*
                  </label>
                  <div className="mt-1">
                    <select
                      id="operator"
                      name="operator"
                      value={formData.operator}
                      onChange={handleChange}
                      className="shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-secondary-300 rounded-md"
                    >
                      <option value="=">=</option>
                      <option value="!=">!=</option>
                      <option value="<">&lt;</option>
                      <option value="<=">&lt;=</option>
                      <option value=">">&gt;</option>
                      <option value=">=">&gt;=</option>
                    </select>
                  </div>
                </div>

                <div className="sm:col-span-4">
                  <label htmlFor="expected_value" className="block text-sm font-medium text-secondary-700">
                    Expected Value*
                  </label>
                  <div className="mt-1">
                    <input
                      type="text"
                      name="expected_value"
                      id="expected_value"
                      value={formData.expected_value}
                      onChange={handleChange}
                      className={`shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-secondary-300 rounded-md ${
                        errors.expected_value ? 'border-danger-300' : ''
                      }`}
                      placeholder="e.g., 0"
                    />
                    {errors.expected_value && (
                      <p className="mt-2 text-sm text-danger-600">{errors.expected_value}</p>
                    )}
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Rule suggestions */}
          {suggestions.length > 0 && (
            <div className="mt-4">
              <h4 className="text-sm font-medium text-secondary-900 mb-2">Suggested Rules</h4>
              <div className="space-y-2">
                {suggestions.map((suggestion, index) => (
                  <div
                    key={index}
                    className="bg-secondary-50 p-3 rounded-md border border-secondary-200 hover:bg-secondary-100 cursor-pointer"
                    onClick={() => applySuggestion(suggestion)}
                  >
                    <div className="flex justify-between items-center">
                      <h5 className="text-sm font-medium text-secondary-900">{suggestion.name}</h5>
                      <button
                        type="button"
                        className="text-primary-600 hover:text-primary-900 text-xs font-medium"
                      >
                        Apply
                      </button>
                    </div>
                    <p className="text-xs text-secondary-500 mt-1">{suggestion.description}</p>
                    <div className="mt-2 text-xs text-secondary-700 font-mono bg-white p-2 rounded border border-secondary-200">
                      {suggestion.query} {suggestion.operator} {suggestion.expected_value}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Form actions */}
          <div className="flex justify-end space-x-3">
            <button
              type="button"
              onClick={onCancel}
              className="inline-flex items-center px-4 py-2 border border-secondary-300 shadow-sm text-sm font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              <XMarkIcon className="-ml-0.5 mr-2 h-4 w-4" aria-hidden="true" />
              Cancel
            </button>
            <button
              type="submit"
              disabled={loading}
              className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
            >
              {loading ? (
                <>
                  <LoadingSpinner size="sm" className="mr-2" />
                  Saving...
                </>
              ) : (
                <>
                  <CheckIcon className="-ml-0.5 mr-2 h-4 w-4" aria-hidden="true" />
                  {rule ? 'Update Rule' : 'Create Rule'}
                </>
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};

export default ValidationRuleEditor;