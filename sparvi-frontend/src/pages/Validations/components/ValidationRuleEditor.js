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
    }
  }, [rule]);

  // Load columns for the table
  useEffect(() => {
    const loadColumns = async () => {
      if (!connectionId || !tableName) return;

      try {
        setLoadingColumns(true);

        const response = await schemaAPI.getColumns(connectionId, tableName);
        setColumns(response.data.columns || []);
      } catch (error) {
        console.error(`Error loading columns for ${tableName}:`, error);
        showNotification(`Failed to load columns for ${tableName}`, 'error');
      } finally {
        setLoadingColumns(false);
      }
    };

    loadColumns();
  }, [connectionId, tableName, showNotification]);

  // Handle input changes
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
        await validationsAPI.updateRule(rule.id, tableName, ruleData);
        showNotification('Validation rule updated successfully', 'success');
      } else {
        // Create new rule
        await validationsAPI.createRule(tableName, ruleData);
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
      setSuggestions([
        {
          name: `${tableName}_not_null`,
          description: `Ensure ${columns[0]?.name || 'id'} is not null`,
          query: `SELECT COUNT(*) FROM ${tableName} WHERE ${columns[0]?.name || 'id'} IS NULL`,
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
          description: `Ensure ${columns[0]?.name || 'id'} values are unique`,
          query: `SELECT COUNT(*) FROM ${tableName} GROUP BY ${columns[0]?.name || 'id'} HAVING COUNT(*) > 1`,
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
                  >
                    {loadingColumns ? (
                      <option>Loading columns...</option>
                    ) : (
                      <>
                        <option value="">Select a column</option>
                        {columns.map(column => (
                          <option key={column.name} value={column.name}>
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