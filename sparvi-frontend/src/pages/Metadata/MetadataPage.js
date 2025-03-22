import React, { useEffect, useState } from 'react';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useMetadataStatus, useRefreshMetadata } from '../../hooks/useMetadataStatus';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import MetadataStatusPanel from './components/MetadataStatusPanel';
import MetadataTasksList from './components/MetadataTasksList';
import MetadataExplorer from './components/MetadataExplorer';
import RefreshControls from './components/RefreshControls';
import EmptyState from '../../components/common/EmptyState';
import { ServerIcon } from '@heroicons/react/24/outline';

const MetadataPage = () => {
  const { activeConnection } = useConnection();
  const { updateBreadcrumbs } = useUI();
  const [selectedMetadataType, setSelectedMetadataType] = useState('tables');

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

  // Use the refresh metadata mutation
  const {
    mutate: refreshMetadata,
    isPending: isRefreshing
  } = useRefreshMetadata(connectionId);

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

      <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-12">
        {/* Side panel */}
        <div className="lg:col-span-3">
          <div className="bg-white shadow rounded-lg">
            <div className="px-4 py-3 border-b border-secondary-200">
              <h3 className="text-sm font-medium text-secondary-900">Refresh Controls</h3>
            </div>
            <div className="p-4">
              <RefreshControls
                connectionId={connectionId}
                onRefresh={refreshMetadata}
                isRefreshing={isRefreshing}
                metadataStatus={metadataStatus}
                onMetadataTypeSelect={setSelectedMetadataType}
                selectedMetadataType={selectedMetadataType}
              />
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
              <MetadataExplorer
                connectionId={connectionId}
                metadataType={selectedMetadataType}
                metadataStatus={metadataStatus}
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default MetadataPage;;