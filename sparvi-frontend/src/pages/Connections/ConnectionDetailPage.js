import React, { useState, useEffect } from 'react';
import { Link, useParams, useNavigate } from 'react-router-dom';
import { ArrowLeftIcon, ServerIcon, CheckCircleIcon, XCircleIcon } from '@heroicons/react/24/outline';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import SnowflakeConnectionForm from './components/SnowflakeConnectionForm';
import PostgreSQLConnectionForm from './components/PostgreSQLConnectionForm';

const ConnectionDetailPage = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  const { getConnection, updateConnection, testConnection } = useConnection();
  const { updateBreadcrumbs, showNotification, setLoading } = useUI();

  const [connection, setConnection] = useState(null);
  const [connectionName, setConnectionName] = useState('');
  const [connectionDetails, setConnectionDetails] = useState({});
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [testLoading, setTestLoading] = useState(false);
  const [errors, setErrors] = useState({});

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Connections', href: '/connections' },
      { name: 'Edit Connection' }
    ]);
  }, [updateBreadcrumbs]);

  // Load connection
  useEffect(() => {
    const loadConnection = async () => {
      try {
        setLoading('connection', true);
        const connectionData = await getConnection(id);
        setConnection(connectionData);
        setConnectionName(connectionData.name);
        setConnectionDetails(connectionData.connection_details || {});
      } catch (error) {
        console.error('Error loading connection:', error);
        showNotification('Failed to load connection', 'error');
        navigate('/connections');
      } finally {
        setLoading('connection', false);
      }
    };

    loadConnection();
  }, [id, getConnection, navigate, setLoading]);

  // Handle connection name change
  const handleNameChange = (e) => {
    setConnectionName(e.target.value);
  };

  // Handle connection details change
  const handleDetailsChange = (details) => {
    setConnectionDetails(details);
    setTestResult(null);
  };

  // Test connection with better validation
  const handleTestConnection = async () => {
    // Clear previous test result
    setTestResult(null);

    // Validate form
    const formErrors = validateForm();
    if (Object.keys(formErrors).length > 0) {
      setErrors(formErrors);
      showNotification('Please fix the form errors before testing', 'warning');
      return;
    }

    try {
      setTestLoading(true);

      const testData = {
        connection_type: connection.connection_type, // or connection.connection_type for edit page
        connection_details: connectionDetails
      };

      console.log('Testing connection with:', JSON.stringify(testData, null, 2));
      const result = await testConnection(testData);

      // Check for specific success indicators in the result
      const isSuccess = result.success === true ||
                        result.message?.toLowerCase().includes('success') ||
                        result.status === 'success';

      if (isSuccess) {
        setTestResult({
          success: true,
          message: 'Connection successful!',
          details: result
        });

        showNotification('Connection test successful!', 'success');
      } else {
        // If we got a response but no clear success indicator, treat as failure
        setTestResult({
          success: false,
          message: 'Connection failed - unexpected response',
          details: result
        });

        showNotification('Connection test failed with an unexpected response', 'error');
      }
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
    if (connection?.connection_type === 'snowflake') {
      if (!connectionDetails.account) errors.account = 'Account is required';
      if (!connectionDetails.username) errors.username = 'Username is required';
      if (!connectionDetails.password && !connection.connection_details.password) {
        errors.password = 'Password is required';
      }
      if (!connectionDetails.database) errors.database = 'Database is required';
      if (!connectionDetails.warehouse) errors.warehouse = 'Warehouse is required';
    } else if (connection?.connection_type === 'postgresql') {
      if (!connectionDetails.host) errors.host = 'Host is required';
      if (!connectionDetails.port) errors.port = 'Port is required';
      if (!connectionDetails.database) errors.database = 'Database is required';
      if (!connectionDetails.username) errors.username = 'Username is required';
      if (!connectionDetails.password && !connection.connection_details.password) {
        errors.password = 'Password is required';
      }
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

      // Handle empty password field - don't update it if not changed
      const updatedDetails = { ...connectionDetails };
      if (!updatedDetails.password && connection.connection_details.password) {
        // Use placeholder to indicate we're not changing the password
        updatedDetails.password = undefined;
      }

      const connectionData = {
        name: connectionName,
        connection_type: connection.connection_type,
        connection_details: updatedDetails
      };

      await updateConnection(id, connectionData);

      showNotification('Connection updated successfully!', 'success');
      navigate('/connections');
    } catch (error) {
      console.error('Failed to update connection:', error);
      showNotification(`Failed to update connection: ${error.response?.data?.error || error.message}`, 'error');
    } finally {
      setIsSubmitting(false);
    }
  };

  // If connection not loaded yet, show loading state
  if (!connection) {
    return (
      <div className="py-4">
        <div className="animate-pulse">
          <div className="h-6 bg-secondary-200 rounded w-1/4 mb-4"></div>
          <div className="h-64 bg-secondary-100 rounded"></div>
        </div>
      </div>
    );
  }

  return (
    <div className="py-4">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold text-secondary-900">Edit Connection</h1>
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
              Update your connection settings for {connection.name}.
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
                  />
                  {errors.name && (
                    <p className="mt-2 text-sm text-danger-600">{errors.name}</p>
                  )}
                </div>

                <div className="col-span-6 sm:col-span-3">
                  <label htmlFor="connection-type" className="block text-sm font-medium text-secondary-700">
                    Connection Type
                  </label>
                  <input
                    type="text"
                    name="connection-type"
                    id="connection-type"
                    value={connection.connection_type}
                    disabled
                    className="mt-1 block w-full py-2 px-3 border border-secondary-300 bg-secondary-100 rounded-md shadow-sm focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                  />
                </div>

                <div className="col-span-6">
                  {connection.connection_type === 'snowflake' && (
                    <SnowflakeConnectionForm
                      details={connectionDetails}
                      onChange={handleDetailsChange}
                      errors={errors}
                    />
                  )}

                  {connection.connection_type === 'postgresql' && (
                    <PostgreSQLConnectionForm
                      details={connectionDetails}
                      onChange={handleDetailsChange}
                      errors={errors}
                    />
                  )}
                </div>
              </div>

              <div className="mt-8 flex justify-end space-x-4">
                <button
                    type="button"
                    onClick={handleTestConnection}
                    disabled={testLoading}
                    className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
                >
                  {testLoading ? (
                      <>
                        <ServerIcon className="animate-pulse -ml-1 mr-2 h-5 w-5" aria-hidden="true"/>
                        Testing...
                      </>
                  ) : (
                      <>
                        <ServerIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true"/>
                        Test Connection
                      </>
                  )}
                </button>

                <button
                    type="submit"
                    disabled={isSubmitting}
                    className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50"
                >
                  {isSubmitting ? 'Updating...' : 'Update Connection'}
                </button>
              </div>
            </form>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ConnectionDetailPage;