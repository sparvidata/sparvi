import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  ServerIcon,
  PlusIcon,
  CheckCircleIcon,
  ExclamationCircleIcon,
  PencilIcon,
  TrashIcon,
  StarIcon
} from '@heroicons/react/24/outline';
import { useConnection } from '../../contexts/ConnectionContext';
import { useUI } from '../../contexts/UIContext';

const ConnectionsPage = () => {
  const { connections, refreshConnections, setAsDefaultConnection, deleteConnection, setCurrentConnection } = useConnection();
  const { updateBreadcrumbs, showNotification, setLoading } = useUI();
  const [isDeleting, setIsDeleting] = useState(false);
  const [connectionToDelete, setConnectionToDelete] = useState(null);

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Connections', href: '/connections' }
    ]);
  }, [updateBreadcrumbs]);

  // Load connections
  useEffect(() => {
    const loadConnections = async () => {
      try {
        setLoading('connections', true);
        await refreshConnections();
      } catch (error) {
        console.error('Error loading connections:', error);
        showNotification('Failed to load connections', 'error');
      } finally {
        setLoading('connections', false);
      }
    };

    loadConnections();
  }, [refreshConnections, setLoading, showNotification]);

  // Set connection as default
  const handleSetDefault = async (id) => {
    try {
      await setAsDefaultConnection(id);
      showNotification('Default connection updated', 'success');
    } catch (error) {
      console.error('Error setting default connection:', error);
      showNotification('Failed to set default connection', 'error');
    }
  };

  // Delete connection confirmation
  const confirmDelete = (connection) => {
    setConnectionToDelete(connection);
    setIsDeleting(true);
  };

  // Handle connection deletion
  const handleDelete = async () => {
    if (!connectionToDelete) return;

    try {
      await deleteConnection(connectionToDelete.id);
      showNotification(`Connection "${connectionToDelete.name}" deleted`, 'success');
      setIsDeleting(false);
      setConnectionToDelete(null);
    } catch (error) {
      console.error('Error deleting connection:', error);
      showNotification('Failed to delete connection', 'error');
    }
  };

  // Cancel deletion
  const cancelDelete = () => {
    setIsDeleting(false);
    setConnectionToDelete(null);
  };

  // Set as active connection
  const handleSetActive = (connection) => {
    setCurrentConnection(connection);
    showNotification(`Switched to connection "${connection.name}"`, 'success');
  };

  // If no connections, show empty state
  if (connections.length === 0) {
    return (
      <div className="py-4">
        <h1 className="text-2xl font-semibold text-secondary-900">Connections</h1>
        <div className="mt-4 text-center py-12 bg-white rounded-lg shadow">
          <ServerIcon className="mx-auto h-12 w-12 text-secondary-400" />
          <h3 className="mt-2 text-sm font-medium text-secondary-900">No connections</h3>
          <p className="mt-1 text-sm text-secondary-500">
            Get started by creating a new connection to your data source.
          </p>
          <div className="mt-6">
            <Link
              to="/connections/new"
              className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              <PlusIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
              New Connection
            </Link>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="py-4">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold text-secondary-900">Connections</h1>
        <Link
          to="/connections/new"
          className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
        >
          <PlusIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
          New Connection
        </Link>
      </div>

      <div className="mt-6 overflow-hidden bg-white shadow sm:rounded-md">
        <ul className="divide-y divide-secondary-200">
          {connections.map((connection) => (
            <li key={connection.id}>
              <div className="px-4 py-4 sm:px-6 flex items-center justify-between">
                <div className="flex items-center">
                  <div className="flex-shrink-0">
                    <ServerIcon
                      className={`h-8 w-8 ${connection.is_default ? 'text-primary-600' : 'text-secondary-400'}`}
                      aria-hidden="true"
                    />
                  </div>
                  <div className="ml-4">
                    <div className="flex items-center">
                      <h3 className="text-lg font-medium text-secondary-900">{connection.name}</h3>
                      {connection.is_default && (
                        <span className="ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-primary-100 text-primary-800">
                          Default
                        </span>
                      )}
                    </div>
                    <div className="mt-1 text-sm text-secondary-500">
                      <span className="font-medium">{connection.connection_type}</span>
                      {connection.connection_details?.database && (
                        <span className="ml-2">
                          Database: <span className="font-mono">{connection.connection_details.database}</span>
                        </span>
                      )}
                      {connection.connection_details?.schema && (
                        <span className="ml-2">
                          Schema: <span className="font-mono">{connection.connection_details.schema}</span>
                        </span>
                      )}
                    </div>
                  </div>
                </div>
                <div className="flex items-center space-x-2">
                  <button
                    onClick={() => handleSetActive(connection)}
                    className="inline-flex items-center p-2 border border-secondary-300 rounded-md shadow-sm text-sm font-medium text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                  >
                    <span className="sr-only">Use</span>
                    <CheckCircleIcon className="h-5 w-5" aria-hidden="true" />
                  </button>

                  <button
                    onClick={() => handleSetDefault(connection.id)}
                    disabled={connection.is_default}
                    className={`inline-flex items-center p-2 border border-secondary-300 rounded-md shadow-sm text-sm font-medium ${
                      connection.is_default 
                        ? 'text-primary-400 bg-primary-50 cursor-not-allowed' 
                        : 'text-secondary-700 bg-white hover:bg-secondary-50'
                    } focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500`}
                  >
                    <span className="sr-only">Set as default</span>
                    <StarIcon className="h-5 w-5" aria-hidden="true" />
                  </button>

                  <Link
                    to={`/connections/${connection.id}`}
                    className="inline-flex items-center p-2 border border-secondary-300 rounded-md shadow-sm text-sm font-medium text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                  >
                    <span className="sr-only">Edit</span>
                    <PencilIcon className="h-5 w-5" aria-hidden="true" />
                  </Link>

                  <button
                    onClick={() => confirmDelete(connection)}
                    disabled={connection.is_default}
                    className={`inline-flex items-center p-2 border border-secondary-300 rounded-md shadow-sm text-sm font-medium ${
                      connection.is_default 
                        ? 'text-danger-300 bg-danger-50 cursor-not-allowed' 
                        : 'text-danger-500 bg-white hover:bg-danger-50'
                    } focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-danger-500`}
                  >
                    <span className="sr-only">Delete</span>
                    <TrashIcon className="h-5 w-5" aria-hidden="true" />
                  </button>
                </div>
              </div>

              <div className="px-4 pb-4 sm:px-6">
                <div className="mt-2 flex justify-between">
                  <div className="sm:flex sm:items-center">
                    <div className="text-sm text-secondary-500">
                      Created {new Date(connection.created_at).toLocaleDateString()}
                    </div>
                    {connection.updated_at && (
                      <div className="mt-2 sm:mt-0 sm:ml-6 text-sm text-secondary-500">
                        Updated {new Date(connection.updated_at).toLocaleDateString()}
                      </div>
                    )}
                  </div>

                  <Link
                    to={`/explorer?connection=${connection.id}`}
                    className="text-sm font-medium text-primary-600 hover:text-primary-500"
                  >
                    Explore data with this connection
                  </Link>
                </div>
              </div>
            </li>
          ))}
        </ul>
      </div>

      {/* Delete confirmation modal */}
      {isDeleting && connectionToDelete && (
        <div className="fixed z-50 inset-0 overflow-y-auto">
          <div className="flex items-center justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
            <div className="fixed inset-0 transition-opacity" aria-hidden="true">
              <div className="absolute inset-0 bg-secondary-500 opacity-75"></div>
            </div>

            <span className="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>

            <div className="inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full">
              <div className="bg-white px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
                <div className="sm:flex sm:items-start">
                  <div className="mx-auto flex-shrink-0 flex items-center justify-center h-12 w-12 rounded-full bg-danger-100 sm:mx-0 sm:h-10 sm:w-10">
                    <ExclamationCircleIcon className="h-6 w-6 text-danger-600" aria-hidden="true" />
                  </div>
                  <div className="mt-3 text-center sm:mt-0 sm:ml-4 sm:text-left">
                    <h3 className="text-lg leading-6 font-medium text-secondary-900">Delete connection</h3>
                    <div className="mt-2">
                      <p className="text-sm text-secondary-500">
                        Are you sure you want to delete the connection "{connectionToDelete.name}"? This action cannot be undone.
                      </p>
                    </div>
                  </div>
                </div>
              </div>
              <div className="bg-secondary-50 px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse">
                <button
                  type="button"
                  className="w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-danger-600 text-base font-medium text-white hover:bg-danger-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-danger-500 sm:ml-3 sm:w-auto sm:text-sm"
                  onClick={handleDelete}
                >
                  Delete
                </button>
                <button
                  type="button"
                  className="mt-3 w-full inline-flex justify-center rounded-md border border-secondary-300 shadow-sm px-4 py-2 bg-white text-base font-medium text-secondary-700 hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm"
                  onClick={cancelDelete}
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ConnectionsPage;