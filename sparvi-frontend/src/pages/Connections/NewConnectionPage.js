import React, { useState, useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { ArrowLeftIcon, ServerIcon, CheckCircleIcon, XCircleIcon } from '@heroicons/react/24/outline';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import SnowflakeConnectionForm from './components/SnowflakeConnectionForm';
import PostgreSQLConnectionForm from './components/PostgreSQLConnectionForm';

const NewConnectionPage = () => {
  const [connectionType, setConnectionType] = useState('snowflake');
  const [connectionName, setConnectionName] = useState('');
  const [connectionDetails, setConnectionDetails] = useState({});
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [testLoading, setTestLoading] = useState(false);
  const [errors, setErrors] = useState({});

  const { createConnection, testConnection } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();
  const navigate = useNavigate();

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Connections', href: '/connections' },
      { name: 'New Connection' }
    ]);
  }, [updateBreadcrumbs]);

  // Handle connection type change
  const handleTypeChange = (e) => {
    setConnectionType(e.target.value);
    setConnectionDetails({});
    setTestResult(null);
  };

  // Handle connection name change
  const handleNameChange = (e) => {
    setConnectionName(e.target.value);
  };

  // Handle connection details change
  const handleDetailsChange = (details) => {
    setConnectionDetails(details);
    setTestResult(null);
  };

  // Test connection
  const handleTestConnection = async () => {
    // Validate form
    const formErrors = validateForm();
    if (Object.keys(formErrors).length > 0) {
      setErrors(formErrors);
      return;
    }

    try {
      setTestLoading(true);

      const testData = {
        connection_type: connectionType,
        connection_details: connectionDetails
      };

      const result = await testConnection(testData);

      setTestResult({
        success: true,
        message: 'Connection successful!',
        details: result
      });

      showNotification('Connection test successful!', 'success');
    } catch (error) {
      console.error('Connection test failed:', error);

      setTestResult({
        success: false,
        message: 'Connection failed',
        details: error.response?.data?.error || error.message
      });

      showNotification(`Connection test failed: ${error.response?.data?.error || error.message}`, 'error');
    } finally {
      setTestLoading(false);
    }
  };

  // Validate form
  const validateForm = () => {
    const errors = {};

    if (!connectionName.trim()) {
      errors.name = 'Connection name is required';
    }

    // Validate details based on connection type
    if (connectionType === 'snowflake') {
      if (!connectionDetails.account) errors.account = 'Account is required';
      if (!connectionDetails.username) errors.username = 'Username is required';
      if (!connectionDetails.password) errors.password = 'Password is required';
      if (!connectionDetails.database) errors.database = 'Database is required';
      if (!connectionDetails.warehouse) errors.warehouse = 'Warehouse is required';
    } else if (connectionType === 'postgresql') {
      if (!connectionDetails.host) errors.host = 'Host is required';
      if (!connectionDetails.port) errors.port = 'Port is required';
      if (!connectionDetails.database) errors.database = 'Database is required';
      if (!connectionDetails.username) errors.username = 'Username is required';
      if (!connectionDetails.password) errors.password = 'Password is required';
    }

    return errors;
  };

  // Handle form submission
  const handleSubmit = async (e) => {
    e.preventDefault();

    // Validate form
    const formErrors = validateForm();
    if (Object.keys(formErrors).length > 0) {
      setErrors(formErrors);
      return;
    }

    try {
      setIsSubmitting(true);

      const connectionData = {
        name: connectionName,
        connection_type: connectionType,
        connection_details: connectionDetails
      };

      await createConnection(connectionData);

      showNotification('Connection created successfully!', 'success');
      navigate('/connections');
    } catch (error) {
      console.error('Failed to create connection:', error);
      showNotification(`Failed to create connection: ${error.response?.data?.error || error.message}`, 'error');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="py-4">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold text-secondary-900">New Connection</h1>
        <Link
          to="/connections"
          className="inline-flex items-center px-4 py-2 border border-secondary-300 rounded-md shadow-sm text-sm font-medium text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
        >
          <ArrowLeftIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
          Back to Connections
        </Link>
      </div>

      <div className="mt-6 bg-white shadow px-4 py-5 sm:rounded-lg sm:p-6">
        <div className="md:grid md:grid-cols-3 md:gap-6">
          <div className="md:col-span-1">
            <h3 className="text-lg font-medium leading-6 text-secondary-900">Connection Details</h3>
            <p className="mt-1 text-sm text-secondary-500">
              Connect to your data warehouse or database to start analyzing your data quality.
            </p>

            {testResult && (
              <div className={`mt-4 p-4 rounded-md ${testResult.success ? 'bg-accent-50' : 'bg-danger-50'}`}>
                <div className="flex">
                  <div className="flex-shrink-0">
                    {testResult.success ? (
                      <CheckCircleIcon className="h-5 w-5 text-accent-400" aria-hidden="true" />
                    ) : (
                      <XCircleIcon className="h-5 w-5 text-danger-400" aria-hidden="true" />
                    )}
                  </div>
                  <div className="ml-3">
                    <h3 className={`text-sm font-medium ${testResult.success ? 'text-accent-800' : 'text-danger-800'}`}>
                      {testResult.message}
                    </h3>
                    {testResult.details && (
                      <div className="mt-2 text-sm">
                        {typeof testResult.details === 'object' ? (
                          <pre className="text-xs overflow-auto p-2 bg-white rounded">
                            {JSON.stringify(testResult.details, null, 2)}
                          </pre>
                        ) : (
                          <p>{testResult.details}</p>
                        )}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}
          </div>

          <div className="mt-5 md:mt-0 md:col-span-2">
            <form onSubmit={handleSubmit}>
              <div className="grid grid-cols-6 gap-6">
                <div className="col-span-6 sm:col-span-3">
                  <label htmlFor="connection-name" className="block text-sm font-medium text-secondary-700">
                    Connection Name
                  </label>
                  <input
                    type="text"
                    name="connection-name"
                    id="connection-name"
                    value={connectionName}
                    onChange={handleNameChange}
                    className={`mt-1 focus:ring-primary-500 focus:border-primary-500 block w-full shadow-sm sm:text-sm border-secondary-300 rounded-md ${
                      errors.name ? 'border-danger-300' : ''
                    }`}
                    placeholder="Production Snowflake"
                  />
                  {errors.name && (
                    <p className="mt-2 text-sm text-danger-600">{errors.name}</p>
                  )}
                </div>

                <div className="col-span-6 sm:col-span-3">
                  <label htmlFor="connection-type" className="block text-sm font-medium text-secondary-700">
                    Connection Type
                  </label>
                  <select
                    id="connection-type"
                    name="connection-type"
                    value={connectionType}
                    onChange={handleTypeChange}
                    className="mt-1 block w-full py-2 px-3 border border-secondary-300 bg-white rounded-md shadow-sm focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                  >
                    <option value="snowflake">Snowflake</option>
                    <option value="postgresql">PostgreSQL</option>
                  </select>
                </div>

                <div className="col-span-6">
                  {connectionType === 'snowflake' && (
                    <SnowflakeConnectionForm
                      details={connectionDetails}
                      onChange={handleDetailsChange}
                      errors={errors}
                    />
                  )}

                  {connectionType === 'postgresql' && (
                    <PostgreSQLConnectionForm
                      details={connectionDetails}
                      onChange={handleDetailsChange}
                      errors={errors}
                    />
                  )}
                </div>
              </div>

              <div className="mt-8 flex justify-end">
                <button
                  type="button"
                  onClick={handleTestConnection}
                  disabled={testLoading}
                  className="mr-3 inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-secondary-600 hover:bg-secondary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-secondary-500 disabled:opacity-50"
                >
                  {testLoading ? (
                    <>
                      <ServerIcon className="animate-pulse -ml-1 mr-2 h-5 w-5" aria-hidden="true" />
                      Testing...
                    </>
                  ) : (
                    <>
                      <ServerIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
                      Test Connection
                    </>
                  )}
                </button>

                <button
                  type="submit"
                  disabled={isSubmitting}
                  className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
                >
                  {isSubmitting ? 'Creating...' : 'Create Connection'}
                </button>
              </div>
            </form>
          </div>
        </div>
      </div>
    </div>
  );
};

export default NewConnectionPage;