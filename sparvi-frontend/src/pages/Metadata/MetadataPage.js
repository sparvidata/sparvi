// src/pages/Metadata/MetadataPage.js - UPDATED VERSION
import React, { useEffect, useState } from 'react';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useMetadataStatus, useRefreshMetadata } from '../../hooks/useMetadataStatus';
import { useSchemaChanges } from '../../hooks/useSchemaChanges'; // Import the new hook
import LoadingSpinner from '../../components/common/LoadingSpinner';
import MetadataStatusPanel from './components/MetadataStatusPanel';
import MetadataTasksList from './components/MetadataTasksList';
import MetadataExplorer from './components/MetadataExplorer';
import SchemaChangesPanel from './components/SchemaChangesPanel'; // Import the new component
import RefreshControls from './components/RefreshControls';
import EmptyState from '../../components/common/EmptyState';
import { ServerIcon, TableCellsIcon, ExclamationTriangleIcon } from '@heroicons/react/24/outline';

const MetadataPage = () => {
  const { activeConnection } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();
  const [selectedMetadataType, setSelectedMetadataType] = useState('tables');
  const [activeTab, setActiveTab] = useState('metadata'); // 'metadata' or 'schema-changes'

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Metadata', href: '/metadata' }
    ]);
  }, [updateBreadcrumbs]);

  // Get the connection ID safely
  const connectionId = activeConnection?.id;

  // Fetch metadata status using the custom hook
  const {
    data: metadataStatus,
    isLoading: isLoadingStatus,
    error: statusError,
    refetch: refetchStatus
  } = useMetadataStatus(connectionId, {
    enabled: !!connectionId,
    refetchInterval: (data) => {
      // Refetch more frequently if there are pending tasks
      return (data?.pending_tasks?.length > 0) ? 5000 : 30000;
    }
  });

  // Use the schema changes hook
  const {
    changes: schemaChanges,
    isLoading: isLoadingChanges,
    detectChanges,
    isDetecting,
    acknowledgeChanges,
    refetch: refetchChanges
  } = useSchemaChanges(connectionId);

  // Use the refresh metadata mutation
  const {
    mutate: refreshMetadata,
    isPending: isRefreshing
  } = useRefreshMetadata(connectionId);

  // Check if there are unacknowledged schema changes
  const hasUnacknowledgedChanges = schemaChanges.some(change => !change.acknowledged);

  // Handle detecting schema changes
  const handleDetectChanges = () => {
    if (!connectionId) {
      showNotification('No connection selected', 'error');
      return;
    }

    detectChanges();
  };

  // If no connection available
  if (!activeConnection) {
    return (
      <EmptyState
        icon={ServerIcon}
        title="No connection selected"
        description="Please select a database connection to view metadata"
        actionText="Manage Connections"
        actionLink="/connections"
      />
    );
  }

  // Handle loading state
  if (isLoadingStatus && !metadataStatus) {
    return (
      <div className="flex justify-center py-12">
        <LoadingSpinner size="lg" />
        <span className="ml-3 text-secondary-600">Loading metadata information...</span>
      </div>
    );
  }

  return (
    <div className="py-4">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold text-secondary-900">Metadata Management</h1>
        <div className="text-sm text-secondary-500">
          Connection: <span className="font-medium text-secondary-700">{activeConnection.name}</span>
        </div>
      </div>

      {/* Tab navigation */}
      <div className="border-b border-secondary-200 mt-4">
        <nav className="-mb-px flex space-x-8" aria-label="Tabs">
          <button
            className={`
              ${activeTab === 'metadata' 
                ? 'border-primary-500 text-primary-600' 
                : 'border-transparent text-secondary-500 hover:border-secondary-300 hover:text-secondary-700'}
              whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm
            `}
            onClick={() => setActiveTab('metadata')}
          >
            Metadata Explorer
          </button>
          <button
            className={`
              ${activeTab === 'schema-changes' 
                ? 'border-primary-500 text-primary-600' 
                : 'border-transparent text-secondary-500 hover:border-secondary-300 hover:text-secondary-700'}
              whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center
            `}
            onClick={() => setActiveTab('schema-changes')}
          >
            Schema Changes
            {hasUnacknowledgedChanges && (
              <span className="ml-2 bg-warning-100 text-warning-800 text-xs px-2 py-0.5 rounded-full">
                {schemaChanges.filter(change => !change.acknowledged).length}
              </span>
            )}
          </button>
        </nav>
      </div>

      <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-12">
        {/* Side panel */}
        <div className="lg:col-span-3">
          <div className="bg-white shadow rounded-lg">
            <div className="px-4 py-3 border-b border-secondary-200">
              <h3 className="text-sm font-medium text-secondary-900">Refresh Controls</h3>
            </div>
            <div className="p-4">
              {activeTab === 'metadata' ? (
                <RefreshControls
                  connectionId={connectionId}
                  onRefresh={refreshMetadata}
                  isRefreshing={isRefreshing}
                  metadataStatus={metadataStatus}
                  onMetadataTypeSelect={setSelectedMetadataType}
                  selectedMetadataType={selectedMetadataType}
                />
              ) : (
                <div className="space-y-4">
                  <button
                    onClick={handleDetectChanges}
                    disabled={isDetecting}
                    className="w-full flex items-center justify-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
                  >
                    {isDetecting ? (
                      <>
                        <LoadingSpinner size="sm" className="mr-2" />
                        Detecting Changes...
                      </>
                    ) : (
                      <>
                        <ExclamationTriangleIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
                        Detect Schema Changes
                      </>
                    )}
                  </button>

                  <div className="border-t border-secondary-200 pt-4">
                    <div className="rounded-md bg-secondary-50 p-4">
                      <div className="flex">
                        <div className="flex-shrink-0">
                          <TableCellsIcon className="h-5 w-5 text-secondary-400" aria-hidden="true" />
                        </div>
                        <div className="ml-3">
                          <h3 className="text-sm font-medium text-secondary-900">About Schema Changes</h3>
                          <div className="mt-2 text-sm text-secondary-700">
                            <p>
                              Schema changes are detected by comparing the current database schema with
                              the previously stored metadata. Detection can be triggered manually or
                              will run automatically during metadata refresh.
                            </p>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>

          <div className="mt-5 bg-white shadow rounded-lg">
            <div className="px-4 py-3 border-b border-secondary-200">
              <h3 className="text-sm font-medium text-secondary-900">Active Tasks</h3>
            </div>
            <div className="p-4">
              <MetadataTasksList
                connectionId={connectionId}
                metadataStatus={metadataStatus}
                isLoading={isLoadingStatus}
              />
            </div>
          </div>
        </div>

        {/* Main content */}
        <div className="lg:col-span-9">
          <div className="bg-white shadow rounded-lg">
            <MetadataStatusPanel
              metadataStatus={metadataStatus}
              isLoading={isLoadingStatus}
              onRefresh={() => refetchStatus()}
            />

            <div className="px-4 py-5 sm:p-6 border-t border-secondary-200">
              {activeTab === 'metadata' ? (
                <MetadataExplorer
                  connectionId={connectionId}
                  metadataType={selectedMetadataType}
                  metadataStatus={metadataStatus}
                />
              ) : (
                <SchemaChangesPanel
                  connectionId={connectionId}
                  schemaChanges={schemaChanges}
                  isLoading={isLoadingChanges}
                  onAcknowledge={acknowledgeChanges}
                />
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default MetadataPage;