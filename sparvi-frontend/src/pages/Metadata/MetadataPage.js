import React, { useEffect, useState } from 'react';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useMetadataStatus, useRefreshMetadata } from '../../hooks/useMetadataStatus';
import { useSchemaChanges } from '../../hooks/useSchemaChanges';
import { useIntegratedMetadata } from '../../hooks/useIntegratedMetadata';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import MetadataStatusPanel from './components/MetadataStatusPanel';
import MetadataTasksList from './components/MetadataTasksList';
import MetadataExplorer from './components/MetadataExplorer';
// import MetadataHistoryPanel from './components/MetadataHistoryPanel'; // TEMPORARILY REMOVED
import SchemaChangesPanel from './components/SchemaChangesPanel';
import RefreshControls from './components/RefreshControls';
import EmptyState from '../../components/common/EmptyState';
import {
  ServerIcon,
  TableCellsIcon,
  ExclamationTriangleIcon,
  ClockIcon,
  CheckCircleIcon
} from '@heroicons/react/24/outline';

const MetadataPage = () => {
  const { activeConnection } = useConnection();
  const { updateBreadcrumbs, showNotification } = useUI();
  const [selectedMetadataType, setSelectedMetadataType] = useState('tables');
  const [activeTab, setActiveTab] = useState('metadata'); // 'metadata', 'history', or 'schema-changes'

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
    refetch: refetchStatus
  } = useMetadataStatus(connectionId, {
    enabled: !!connectionId,
    refetchInterval: (data) => {
      // Refetch more frequently if there are pending tasks
      return (data?.pending_tasks?.length > 0) ? 5000 : 30000;
    }
  });

  // Use integrated metadata loading for better UX
  const {
    data: integratedData,
    isLoading: isLoadingIntegrated,
    refetch: refetchMetadata
  } = useIntegratedMetadata(connectionId, {
    enabled: !!connectionId && activeTab === 'metadata',
    includeColumns: true,
    includeStatistics: true
  });

  // Extract data from integrated response
  const summary = integratedData?.success ? integratedData.data.summary : null;
  const hasColumns = integratedData?.success && integratedData.data.columns.length > 0;
  const hasStatistics = integratedData?.success && integratedData.data.statistics.length > 0;
  const hasBasicTables = integratedData?.success && integratedData.data.tables.length > 0;

  // Use the schema changes hook
  const {
    changes: schemaChanges,
    isLoading: isLoadingChanges,
    detectChanges,
    isDetecting,
    acknowledgeChanges,
    setAcknowledgedFilter,
    acknowledgedFilter
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

  // Enhanced refresh handler that works with integrated metadata
  const handleRefresh = (type, options = {}) => {
    const { onSuccess, onError } = options;

    if (type === 'full') {
      // Refresh all metadata types
      refreshMetadata({
        metadata_type: 'full',
        priority: 'high'
      }, {
        onSuccess: () => {
          // Also refresh our integrated metadata
          refetchMetadata();
          refetchStatus();
          onSuccess?.();
        },
        onError: (error) => {
          console.error('Error in full metadata refresh:', error);
          onError?.(error);
        }
      });
    } else {
      // Refresh specific metadata type
      refreshMetadata({
        metadata_type: type,
        priority: 'high'
      }, {
        onSuccess: () => {
          // Refresh integrated metadata after successful refresh
          refetchMetadata();
          refetchStatus();
          onSuccess?.();
        },
        onError: (error) => {
          console.error(`Error refreshing ${type} metadata:`, error);
          onError?.(error);
        }
      });
    }
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

      {/* Enhanced summary panel when we have data */}
      {summary && activeTab === 'metadata' && (
        <div className="mt-4 bg-white border border-secondary-200 rounded-lg p-4">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="text-center">
              <div className="text-2xl font-bold text-secondary-900">{summary.total_tables || summary.totalTables || 0}</div>
              <div className="text-xs text-secondary-500">Tables</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-accent-600">{(summary.totalRows || summary.total_rows || 0).toLocaleString()}</div>
              <div className="text-xs text-secondary-500">Total Rows</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-primary-600">{summary.total_columns || summary.totalColumns || 0}</div>
              <div className="text-xs text-secondary-500">Columns</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-warning-600">{Math.round(summary.averageHealthScore || summary.average_health_score || 0)}</div>
              <div className="text-xs text-secondary-500">Avg Health Score</div>
            </div>
          </div>

          {/* Loading indicator for progressive data */}
          {isLoadingIntegrated && (
            <div className="mt-3 flex items-center justify-center text-sm text-secondary-500">
              <LoadingSpinner size="sm" className="mr-2" />
              Loading detailed statistics...
            </div>
          )}

          {/* Data availability indicators */}
          <div className="mt-3 flex items-center justify-center space-x-4 text-xs">
            <div className={`flex items-center ${hasBasicTables ? 'text-accent-600' : 'text-secondary-400'}`}>
              <CheckCircleIcon className="h-3 w-3 mr-1" />
              Tables
            </div>
            <div className={`flex items-center ${hasColumns && summary.total_columns > 0 ? 'text-accent-600' : isLoadingIntegrated ? 'text-warning-600' : 'text-secondary-400'}`}>
              {isLoadingIntegrated && !(hasColumns && summary.total_columns > 0) ? (
                <ClockIcon className="h-3 w-3 mr-1" />
              ) : (
                <CheckCircleIcon className="h-3 w-3 mr-1" />
              )}
              Columns
            </div>
            <div className={`flex items-center ${hasStatistics && summary.total_statistics > 0 ? 'text-accent-600' : isLoadingIntegrated ? 'text-warning-600' : 'text-secondary-400'}`}>
              {isLoadingIntegrated && !(hasStatistics && summary.total_statistics > 0) ? (
                <ClockIcon className="h-3 w-3 mr-1" />
              ) : (
                <CheckCircleIcon className="h-3 w-3 mr-1" />
              )}
              Statistics
            </div>
          </div>
        </div>
      )}

      {/* Tab navigation */}
      <div className="border-b border-secondary-200 mt-4">
        <nav className="-mb-px flex space-x-8" aria-label="Tabs">
          <button
            className={`
              ${activeTab === 'metadata' 
                ? 'border-primary-500 text-primary-600' 
                : 'border-transparent text-secondary-500 hover:border-secondary-300 hover:text-secondary-700'}
              whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center
            `}
            onClick={() => setActiveTab('metadata')}
          >
            <TableCellsIcon className="mr-2 h-5 w-5" />
            Metadata Explorer
          </button>

          {/* TEMPORARILY REMOVED: Metadata History tab
          <button
            className={`
              ${activeTab === 'history' 
                ? 'border-primary-500 text-primary-600' 
                : 'border-transparent text-secondary-500 hover:border-secondary-300 hover:text-secondary-700'}
              whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center
            `}
            onClick={() => setActiveTab('history')}
          >
            <ClockIcon className="mr-2 h-5 w-5" />
            Metadata History
          </button>
          */}

          <button
            aria-label="Schema Changes"
            className={`
              ${activeTab === 'schema-changes' 
                ? 'border-primary-500 text-primary-600' 
                : 'border-transparent text-secondary-500 hover:border-secondary-300 hover:text-secondary-700'}
              whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center
            `}
            onClick={() => setActiveTab('schema-changes')}
          >
            <ExclamationTriangleIcon className="mr-2 h-5 w-5" />
            Schema Changes
            {hasUnacknowledgedChanges && (
              <span className="ml-2 bg-warning-100 text-warning-800 text-xs px-2 py-0.5 rounded-full">
                {schemaChanges.filter(change => !change.acknowledged).length}
              </span>
            )}
          </button>
        </nav>
      </div>

      {/* Add persistent banner for unacknowledged changes */}
      {hasUnacknowledgedChanges && activeTab !== 'schema-changes' && (
        <div className="mt-4 mb-6">
          <div className="rounded-md bg-warning-50 p-4">
            <div className="flex">
              <div className="flex-shrink-0">
                <ExclamationTriangleIcon className="h-5 w-5 text-warning-400" aria-hidden="true" />
              </div>
              <div className="ml-3 flex-1 md:flex md:justify-between">
                <p className="text-sm text-warning-700">
                  There are {schemaChanges.filter(change => !change.acknowledged).length} unacknowledged schema changes that require your attention.
                </p>
                <p className="mt-3 text-sm md:mt-0 md:ml-6">
                  <button
                    onClick={() => setActiveTab('schema-changes')}
                    className="whitespace-nowrap font-medium text-warning-700 hover:text-warning-600"
                  >
                    View changes <span aria-hidden="true">&rarr;</span>
                  </button>
                </p>
              </div>
            </div>
          </div>
        </div>
      )}

      <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-12">
        {/* Side panel - only show for metadata tab */}
        {(activeTab === 'metadata' || activeTab === 'schema-changes') && (
          <div className="lg:col-span-3">
            <div className="bg-white shadow rounded-lg">
              <div className="px-4 py-3 border-b border-secondary-200">
                <h3 className="text-sm font-medium text-secondary-900">
                  {activeTab === 'metadata' ? 'Refresh Controls' : 'Change Detection'}
                </h3>
              </div>
              <div className="p-4">
                {activeTab === 'metadata' ? (
                  <RefreshControls
                    connectionId={connectionId}
                    onRefresh={handleRefresh}
                    isRefreshing={isRefreshing}
                    metadataStatus={metadataStatus}
                    onMetadataTypeSelect={setSelectedMetadataType}
                    selectedMetadataType={selectedMetadataType}
                    onViewSchemaChanges={() => setActiveTab('schema-changes')}
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
        )}

        {/* Main content */}
        <div className={`${(activeTab === 'metadata' || activeTab === 'schema-changes') ? 'lg:col-span-9' : 'lg:col-span-12'}`}>
          <div className="bg-white shadow rounded-lg">
            {/* Only show status panel for metadata and schema changes tabs */}
            {(activeTab === 'metadata' || activeTab === 'schema-changes') && (
              <MetadataStatusPanel
                metadataStatus={metadataStatus}
                isLoading={isLoadingStatus}
                onRefresh={() => refetchStatus()}
              />
            )}

            <div className={`px-4 py-5 sm:p-6 ${(activeTab === 'metadata' || activeTab === 'schema-changes') ? 'border-t border-secondary-200' : ''}`}>
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
                  onRefresh={handleDetectChanges}
                  setAcknowledgedFilter={setAcknowledgedFilter}
                  acknowledgedFilter={acknowledgedFilter}
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