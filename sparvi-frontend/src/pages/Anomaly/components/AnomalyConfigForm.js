// Replace the BatchRequest section in your AnomalyConfigForm.js with this direct loading approach

import React, { useState, useEffect } from 'react';
import { useConnection } from '../../../contexts/EnhancedConnectionContext';
import { useUI } from '../../../contexts/UIContext';
import { useParams, useNavigate, Link } from 'react-router-dom';
import {
  ArrowLeftIcon,
  CheckIcon,
  ExclamationTriangleIcon,
  ServerIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import EmptyState from '../../../components/common/EmptyState';
import { schemaAPI } from '../../../api/enhancedApiService';

const AnomalyConfigForm = () => {
  const { activeConnection, loading: connectionLoading } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();
  const { connectionId, configId } = useParams();
  const navigate = useNavigate();

  const isEdit = configId && configId !== 'new';

  // Config state
  const [config, setConfig] = useState({
    table_name: '',
    column_name: '',
    metric_name: '',
    detection_method: 'zscore',
    sensitivity: 1.0,
    min_data_points: 7,
    baseline_window_days: 14,
    is_active: true,
    config_params: {}
  });

  // Loading states
  const [loading, setLoading] = useState(isEdit);
  const [saving, setSaving] = useState(false);
  const [loadingTables, setLoadingTables] = useState(false);
  const [error, setError] = useState(null);

  // Data states
  const [tables, setTables] = useState([]);

  // Define available metrics
  const availableMetrics = [
    { value: 'row_count', label: 'Row Count', level: 'table' },
    { value: 'null_percentage', label: 'Null Percentage', level: 'column' },
    { value: 'distinct_count', label: 'Distinct Value Count', level: 'column' },
    { value: 'distinct_percentage', label: 'Distinct Value Percentage', level: 'column' },
    { value: 'min_value', label: 'Minimum Value', level: 'column' },
    { value: 'max_value', label: 'Maximum Value', level: 'column' },
    { value: 'avg_value', label: 'Average Value', level: 'column' },
    { value: 'std_dev', label: 'Standard Deviation', level: 'column' }
  ];

  // Handle redirect if connection ID is missing but we have activeConnection
  useEffect(() => {
    if (!connectionLoading && !connectionId && activeConnection) {
      navigate(`/anomalies/${activeConnection.id}/configs${isEdit ? `/${configId}` : '/new'}`, { replace: true });
    }
  }, [connectionId, configId, activeConnection, connectionLoading, isEdit, navigate]);

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Dashboard', href: '/dashboard' },
      { name: 'Anomaly Detection', href: '/anomalies' },
      { name: activeConnection?.name || 'Connection', href: `/anomalies/${connectionId}` },
      { name: 'Configurations', href: `/anomalies/${connectionId}/configs` },
      { name: isEdit ? 'Edit Configuration' : 'New Configuration' }
    ]);
  }, [updateBreadcrumbs, connectionId, activeConnection, isEdit]);

  // Load tables - using direct API call like ValidationRuleEditor
  useEffect(() => {
    const loadTables = async () => {
      if (!connectionId || connectionId === 'undefined') return;

      try {
        setLoadingTables(true);
        console.log(`Loading tables for connection ${connectionId}`);

        const response = await schemaAPI.getTables(connectionId);
        console.log('Tables response:', response);

        // Handle different possible response structures (same as ValidationRuleEditor)
        let tablesData = [];

        if (response?.tables && Array.isArray(response.tables)) {
          // Direct tables array in response
          tablesData = response.tables;
        } else if (response?.data?.tables && Array.isArray(response.data.tables)) {
          // Nested in data property
          tablesData = response.data.tables;
        } else if (Array.isArray(response)) {
          // Response is the array itself
          tablesData = response;
        } else {
          console.warn('Unexpected tables response format:', response);
        }

        console.log(`Found ${tablesData.length} tables for connection ${connectionId}`);
        setTables(tablesData);
      } catch (error) {
        console.error(`Error loading tables for connection ${connectionId}:`, error);
        showNotification(`Failed to load tables: ${error.message}`, 'error');
        setError(`Failed to load tables: ${error.message}`);
      } finally {
        setLoadingTables(false);
      }
    };

    loadTables();
  }, [connectionId, showNotification]);

  // Fetch config data if editing
  useEffect(() => {
    if (isEdit && connectionId && connectionId !== 'undefined') {
      const fetchConfig = async () => {
        try {
          setLoading(true);
          const response = await fetch(`/api/connections/${connectionId}/anomalies/configs/${configId}`);

          if (!response.ok) {
            throw new Error('Failed to fetch configuration');
          }

          const data = await response.json();
          setConfig(data.config);
        } catch (err) {
          console.error('Error fetching config:', err);
          setError('Failed to load configuration');
        } finally {
          setLoading(false);
        }
      };

      fetchConfig();
    }
  }, [connectionId, configId, isEdit]);

  // Handle input changes
  const handleInputChange = (e) => {
    const { name, value, type, checked } = e.target;

    // Handle nested config_params
    if (name.startsWith('config_params.')) {
      const paramName = name.split('.')[1];
      setConfig(prev => ({
        ...prev,
        config_params: {
          ...prev.config_params,
          [paramName]: type === 'number' ? parseFloat(value) : value
        }
      }));
    } else {
      // Handle regular inputs
      setConfig(prev => ({
        ...prev,
        [name]: type === 'checkbox' ? checked :
                type === 'number' ? parseFloat(value) : value
      }));
    }
  };

  // Handle form submission
  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!connectionId || connectionId === 'undefined') {
      setError("Cannot save configuration: Connection ID is missing");
      return;
    }

    try {
      setSaving(true);
      setError(null);

      // Prepare the API endpoint and method
      const url = isEdit
        ? `/api/connections/${connectionId}/anomalies/configs/${configId}`
        : `/api/connections/${connectionId}/anomalies/configs`;

      const method = isEdit ? 'PUT' : 'POST';

      // Add connection_id to the config data
      const configData = {
        ...config,
        connection_id: connectionId
      };

      const response = await fetch(url, {
        method,
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(configData)
      });

      if (!response.ok) {
        throw new Error(`Failed to ${isEdit ? 'update' : 'create'} configuration`);
      }

      const result = await response.json();

      showNotification({
        type: 'success',
        message: `Configuration ${isEdit ? 'updated' : 'created'} successfully`
      });

      // Navigate back to configs list
      navigate(`/anomalies/${connectionId}/configs`);
    } catch (err) {
      console.error('Error saving config:', err);
      setError(err.message || `Failed to ${isEdit ? 'update' : 'create'} configuration`);
    } finally {
      setSaving(false);
    }
  };

  // Check if we can render the form
  const shouldRenderForm = !connectionLoading && connectionId && connectionId !== 'undefined';

  // If no active connection is available
  if (!connectionLoading && !activeConnection) {
    return (
      <EmptyState
        icon={ServerIcon}
        title="No connection selected"
        description="Please select a database connection to manage anomaly configurations"
        actionText="Manage Connections"
        actionLink="/connections"
      />
    );
  }

  // Show loading spinner while connection is loading or redirecting
  if (connectionLoading) {
    return (
      <div className="flex justify-center py-12">
        <LoadingSpinner size="lg" />
        <p className="ml-4 text-gray-500">Loading connection data...</p>
      </div>
    );
  }

  // If no connection ID is available (and no active connection to redirect to)
  if (!connectionId || connectionId === 'undefined') {
    return (
      <div className="text-center py-10">
        <p className="text-red-500">No connection ID available. Please select a connection.</p>
        <Link
          to="/anomalies"
          className="mt-4 inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700"
        >
          Go to Anomaly Dashboard
        </Link>
      </div>
    );
  }

  // Show loading spinner while fetching configuration
  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="flex flex-col space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center">
          <button
            onClick={() => navigate(`/anomalies/${connectionId}/configs`)}
            className="mr-4 p-2 rounded-full text-gray-400 hover:text-gray-500"
          >
            <ArrowLeftIcon className="h-5 w-5" />
          </button>
          <h1 className="text-2xl font-bold text-gray-900">
            {isEdit ? 'Edit Configuration' : 'New Anomaly Detection Configuration'}
          </h1>
        </div>
      </div>

      {error && (
        <div className="rounded-md bg-red-50 p-4">
          <div className="flex">
            <div className="flex-shrink-0">
              <ExclamationTriangleIcon className="h-5 w-5 text-red-400" aria-hidden="true" />
            </div>
            <div className="ml-3">
              <h3 className="text-sm font-medium text-red-800">Error</h3>
              <div className="mt-2 text-sm text-red-700">{error}</div>
            </div>
          </div>
        </div>
      )}

      {shouldRenderForm && (
        <form onSubmit={handleSubmit}>
          <div className="bg-white shadow overflow-hidden sm:rounded-lg">
            <div className="px-4 py-5 sm:p-6">
              <div className="grid grid-cols-1 gap-6">
                {/* Table selection */}
                <div>
                  <label htmlFor="table_name" className="block text-sm font-medium text-gray-700">
                    Table <span className="text-red-500">*</span>
                  </label>
                  <select
                    id="table_name"
                    name="table_name"
                    value={config.table_name}
                    onChange={handleInputChange}
                    required
                    disabled={loadingTables}
                    className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                  >
                    {loadingTables ? (
                      <option>Loading tables...</option>
                    ) : tables.length === 0 ? (
                      <option>No tables available</option>
                    ) : (
                      <>
                        <option value="">Select a table</option>
                        {tables.map((table, index) => (
                          <option key={index} value={table}>
                            {table}
                          </option>
                        ))}
                      </>
                    )}
                  </select>
                  {loadingTables && (
                    <div className="mt-2 flex items-center text-sm text-gray-500">
                      <LoadingSpinner size="xs" className="mr-2" />
                      Loading tables...
                    </div>
                  )}
                </div>

                {/* Column selection - only needed for column-level metrics */}
                {config.table_name && (
                  <div>
                    <label htmlFor="column_name" className="block text-sm font-medium text-gray-700">
                      Column {config.metric_name && availableMetrics.find(m => m.value === config.metric_name)?.level === 'column' && <span className="text-red-500">*</span>}
                    </label>
                    <div className="mt-1 flex rounded-md shadow-sm">
                      <input
                        type="text"
                        name="column_name"
                        id="column_name"
                        value={config.column_name || ''}
                        onChange={handleInputChange}
                        required={config.metric_name && availableMetrics.find(m => m.value === config.metric_name)?.level === 'column'}
                        className="block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                        placeholder="Enter column name"
                      />
                    </div>
                    <p className="mt-1 text-sm text-gray-500">
                      Only required for column-level metrics.
                    </p>
                  </div>
                )}

                {/* Metric selection */}
                <div>
                  <label htmlFor="metric_name" className="block text-sm font-medium text-gray-700">
                    Metric <span className="text-red-500">*</span>
                  </label>
                  <select
                    id="metric_name"
                    name="metric_name"
                    value={config.metric_name}
                    onChange={handleInputChange}
                    required
                    className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                  >
                    <option value="">Select a metric</option>
                    {availableMetrics.map((metric) => (
                      <option key={metric.value} value={metric.value}>
                        {metric.label} ({metric.level}-level)
                      </option>
                    ))}
                  </select>
                </div>

                {/* Detection method */}
                <div>
                  <label htmlFor="detection_method" className="block text-sm font-medium text-gray-700">
                    Detection Method <span className="text-red-500">*</span>
                  </label>
                  <select
                    id="detection_method"
                    name="detection_method"
                    value={config.detection_method}
                    onChange={handleInputChange}
                    required
                    className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                  >
                    <option value="zscore">Z-Score</option>
                    <option value="iqr">IQR (Interquartile Range)</option>
                    <option value="moving_average">Moving Average</option>
                  </select>
                </div>

                {/* Sensitivity */}
                <div>
                  <label htmlFor="sensitivity" className="block text-sm font-medium text-gray-700">
                    Sensitivity
                  </label>
                  <div className="mt-1 flex items-center">
                    <input
                      type="range"
                      name="sensitivity"
                      id="sensitivity"
                      min="0.5"
                      max="2"
                      step="0.1"
                      value={config.sensitivity}
                      onChange={handleInputChange}
                      className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer"
                    />
                    <span className="ml-2 text-sm text-gray-700">
                      {config.sensitivity.toFixed(1)}
                    </span>
                  </div>
                  <p className="mt-1 text-sm text-gray-500">
                    Higher values make detection more sensitive (more anomalies).
                  </p>
                </div>

                {/* Advanced settings section */}
                <div className="border-t border-gray-200 pt-4">
                  <h3 className="text-lg font-medium text-gray-900">Advanced Settings</h3>
                </div>

                {/* Minimum data points */}
                <div>
                  <label htmlFor="min_data_points" className="block text-sm font-medium text-gray-700">
                    Minimum Data Points
                  </label>
                  <input
                    type="number"
                    name="min_data_points"
                    id="min_data_points"
                    min="3"
                    value={config.min_data_points}
                    onChange={handleInputChange}
                    className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                  />
                  <p className="mt-1 text-sm text-gray-500">
                    Minimum number of data points required for detection.
                  </p>
                </div>

                {/* Baseline window days */}
                <div>
                  <label htmlFor="baseline_window_days" className="block text-sm font-medium text-gray-700">
                    Baseline Window (days)
                  </label>
                  <input
                    type="number"
                    name="baseline_window_days"
                    id="baseline_window_days"
                    min="1"
                    max="90"
                    value={config.baseline_window_days}
                    onChange={handleInputChange}
                    className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                  />
                  <p className="mt-1 text-sm text-gray-500">
                    Number of days of historical data to use as baseline.
                  </p>
                </div>

                {/* Method-specific config params */}
                {config.detection_method === 'moving_average' && (
                  <div>
                    <label htmlFor="config_params.window" className="block text-sm font-medium text-gray-700">
                      Moving Average Window Size
                    </label>
                    <input
                      type="number"
                      name="config_params.window"
                      id="config_params.window"
                      min="3"
                      value={config.config_params?.window || 7}
                      onChange={handleInputChange}
                      className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                    />
                    <p className="mt-1 text-sm text-gray-500">
                      Number of data points to use for calculating the moving average.
                    </p>
                  </div>
                )}

                {/* Z-score and IQR window size */}
                {(config.detection_method === 'zscore' || config.detection_method === 'iqr') && (
                  <div>
                    <label htmlFor="config_params.window" className="block text-sm font-medium text-gray-700">
                      Window Size (Optional)
                    </label>
                    <input
                      type="number"
                      name="config_params.window"
                      id="config_params.window"
                      min="0"
                      value={config.config_params?.window || ''}
                      onChange={handleInputChange}
                      className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
                    />
                    <p className="mt-1 text-sm text-gray-500">
                      Optional: Number of recent data points to use for detection. Leave blank to use all data.
                    </p>
                  </div>
                )}

                {/* Active status */}
                <div className="relative flex items-start">
                  <div className="flex items-center h-5">
                    <input
                      id="is_active"
                      name="is_active"
                      type="checkbox"
                      checked={config.is_active}
                      onChange={handleInputChange}
                      className="h-4 w-4 text-primary-600 border-gray-300 rounded focus:ring-primary-500"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label htmlFor="is_active" className="font-medium text-gray-700">
                      Active
                    </label>
                    <p className="text-gray-500">Enable or disable anomaly detection for this configuration.</p>
                  </div>
                </div>

              </div>
            </div>
            <div className="px-4 py-3 bg-gray-50 text-right sm:px-6">
              <button
                type="button"
                onClick={() => navigate(`/anomalies/${connectionId}/configs`)}
                className="inline-flex justify-center py-2 px-4 border border-gray-300 shadow-sm text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 mr-3"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={saving}
                className="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
              >
                {saving ? (
                  <>
                    <LoadingSpinner size="xs" className="mr-2" />
                    Saving...
                  </>
                ) : (
                  <>
                    <CheckIcon className="h-4 w-4 mr-2" />
                    {isEdit ? 'Update' : 'Create'} Configuration
                  </>
                )}
              </button>
            </div>
          </div>
        </form>
      )}
    </div>
  );
};

export default AnomalyConfigForm;