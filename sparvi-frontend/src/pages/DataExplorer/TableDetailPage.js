import React, { useState, useEffect } from 'react';
import { Link, useParams, useNavigate } from 'react-router-dom';
import {
  ArrowLeftIcon,
  TableCellsIcon,
  ArrowPathIcon,
  ClipboardDocumentCheckIcon,
  ChartBarIcon,
  DocumentDuplicateIcon
} from '@heroicons/react/24/outline';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { schemaAPI, profilingAPI, validationsAPI } from '../../api/enhancedApiService';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import BatchRequest from '../../components/common/BatchRequest';
import TableHeader from './components/TableHeader';
import TableTabs from './components/TableTabs';
import TableProfile from './components/TableProfile';
import TableColumns from './components/TableColumns';
import TableValidations from './components/TableValidations';
import TableHistory from './components/TableHistory';
import TablePreview from './components/TablePreview';

const TableDetailPage = () => {
  const { connectionId, tableName } = useParams();
  const navigate = useNavigate();
  const { connections, activeConnection, setCurrentConnection } = useConnection();
  const { updateBreadcrumbs, showNotification, setLoading } = useUI();

  const [tableData, setTableData] = useState(null);
  const [profileData, setProfileData] = useState(null);
  const [validations, setValidations] = useState([]);
  const [isLoadingTable, setIsLoadingTable] = useState(true);
  const [isLoadingProfile, setIsLoadingProfile] = useState(true);
  const [activeTab, setActiveTab] = useState('profile');
  const [connection, setConnection] = useState(null);
  const [isRefreshing, setIsRefreshing] = useState(false);

  // Set the active connection based on connectionId from URL
  useEffect(() => {
    if (connectionId && connections.length > 0) {
      const conn = connections.find(c => c.id === connectionId);
      if (conn) {
        setConnection(conn);
        if (!activeConnection || activeConnection.id !== connectionId) {
          setCurrentConnection(conn);
        }
      } else {
        showNotification('Connection not found', 'error');
        navigate('/explorer');
      }
    }
  }, [connectionId, connections, activeConnection, setCurrentConnection, navigate]);

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Data Explorer', href: '/explorer' },
      { name: tableName }
    ]);
  }, [tableName, updateBreadcrumbs]);

  // Batch requests configuration
  const batchRequests = [
    { id: 'columns', path: `/connections/${connectionId}/tables/${tableName}/columns` },
    { id: 'profile', path: '/profile', params: { connection_id: connectionId, table: tableName } },
    { id: 'validations', path: '/validations', params: { table: tableName } }
  ];

  // Initial data loading using batch request
  useEffect(() => {
    if (!connection) return;
    setIsLoadingTable(true);
    setIsLoadingProfile(true);
  }, [connection]);

  // Handle tab change
  const handleTabChange = (tab) => {
    setActiveTab(tab);
  };

  // Handle refresh profile
  const handleRefreshProfile = async () => {
    if (!connection) return;

    try {
      setIsRefreshing(true);
      setIsLoadingProfile(true);
      showNotification('Refreshing profile...', 'info');

      // Get fresh profile data
      const profileResponse = await profilingAPI.getProfile(connectionId, tableName, {
        forceFresh: true,
        requestId: `profile-refresh-${connectionId}-${tableName}`
      });

      setProfileData(profileResponse);
      showNotification('Profile refreshed successfully', 'success');
    } catch (error) {
      console.error(`Error refreshing profile for ${tableName}:`, error);
      showNotification(`Failed to refresh profile for ${tableName}`, 'error');
    } finally {
      setIsRefreshing(false);
      setIsLoadingProfile(false);
    }
  };

  // Handle batch request completion
  const handleBatchComplete = (results) => {
    if (results.columns) {
      setTableData(results.columns);
    }

    if (results.profile) {
      setProfileData(results.profile);
    }

    if (results.validations) {
      setValidations(results.validations.rules || []);
    }

    setIsLoadingTable(false);
    setIsLoadingProfile(false);
  };

  // Handle batch request error
  const handleBatchError = (error) => {
    console.error(`Error loading data for ${tableName}:`, error);
    showNotification(`Failed to load data for ${tableName}`, 'error');
    setIsLoadingTable(false);
    setIsLoadingProfile(false);
  };

  return (
    <div className="py-4">
      {/* Table header */}
      <TableHeader
        tableName={tableName}
        connectionName={connection?.name}
        onRefreshProfile={handleRefreshProfile}
        isRefreshing={isRefreshing}
      />

      {/* Tabs and content */}
      <div className="mt-6 bg-white shadow rounded-lg overflow-hidden">
        <TableTabs
          activeTab={activeTab}
          onChange={handleTabChange}
          validationCount={validations.length}
        />

        {connection ? (
          <BatchRequest
            requests={batchRequests}
            onComplete={handleBatchComplete}
            onError={handleBatchError}
            loadingComponent={
              <div className="flex justify-center py-12">
                <LoadingSpinner size="lg" />
                <span className="ml-3 text-secondary-600">Loading table data...</span>
              </div>
            }
          >
            {() => (
              <div className="p-6">
                {activeTab === 'profile' && (
                  <TableProfile
                    profile={profileData}
                    isLoading={isLoadingProfile}
                    tableName={tableName}
                  />
                )}

                {activeTab === 'columns' && (
                  <TableColumns
                    columns={tableData?.columns}
                    isLoading={isLoadingTable}
                    profile={profileData}
                  />
                )}

                {activeTab === 'validations' && (
                  <TableValidations
                    validations={validations}
                    isLoading={false}
                    connectionId={connectionId}
                    tableName={tableName}
                    onUpdate={(updatedValidations) => setValidations(updatedValidations)}
                  />
                )}

                {activeTab === 'history' && (
                  <TableHistory
                    tableName={tableName}
                    connectionId={connectionId}
                  />
                )}

                {activeTab === 'preview' && (
                  <TablePreview
                    connectionId={connectionId}
                    tableName={tableName}
                  />
                )}
              </div>
            )}
          </BatchRequest>
        ) : (
          <div className="flex justify-center py-12">
            <LoadingSpinner size="lg" />
          </div>
        )}
      </div>
    </div>
  );
};

export default TableDetailPage;