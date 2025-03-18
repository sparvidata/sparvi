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
import { useConnection } from '../../contexts/ConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { schemaAPI, profilingAPI, validationsAPI } from '../../api/apiService';
import LoadingSpinner from '../../components/common/LoadingSpinner';
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
  const [isLoadingValidations, setIsLoadingValidations] = useState(true);
  const [activeTab, setActiveTab] = useState('profile');
  const [connection, setConnection] = useState(null);

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
  }, [connectionId, connections, activeConnection, setCurrentConnection, showNotification, navigate]);

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Data Explorer', href: '/explorer' },
      { name: tableName }
    ]);
  }, [tableName, updateBreadcrumbs]);

  // Load table data
  useEffect(() => {
    const loadData = async () => {
      if (!connection) return;

      try {
        setIsLoadingTable(true);

        // Get table columns
        const columnsResponse = await schemaAPI.getColumns(connectionId, tableName);
        setTableData(columnsResponse.data);

      } catch (error) {
        console.error(`Error loading table data for ${tableName}:`, error);
        showNotification(`Failed to load table data for ${tableName}`, 'error');
      } finally {
        setIsLoadingTable(false);
      }
    };

    loadData();
  }, [connectionId, tableName, connection, showNotification]);

  // Load profile data
  useEffect(() => {
    const loadProfileData = async () => {
      if (!connection) return;

      try {
        setIsLoadingProfile(true);

        // Get profile data
        const profileResponse = await profilingAPI.getProfile(connectionId, tableName);
        setProfileData(profileResponse.data);

      } catch (error) {
        console.error(`Error loading profile data for ${tableName}:`, error);
        showNotification(`Failed to load profile data for ${tableName}`, 'error');
      } finally {
        setIsLoadingProfile(false);
      }
    };

    loadProfileData();
  }, [connectionId, tableName, connection, showNotification]);

  // Load validations
  useEffect(() => {
    const loadValidations = async () => {
      if (!connection) return;

      try {
        setIsLoadingValidations(true);

        // Get validations
        const validationsResponse = await validationsAPI.getRules(tableName);
        setValidations(validationsResponse.data.rules || []);

      } catch (error) {
        console.error(`Error loading validations for ${tableName}:`, error);
        showNotification(`Failed to load validations for ${tableName}`, 'error');
      } finally {
        setIsLoadingValidations(false);
      }
    };

    loadValidations();
  }, [connectionId, tableName, connection, showNotification]);

  // Handle refresh profile
  const handleRefreshProfile = async () => {
    if (!connection) return;

    try {
      setIsLoadingProfile(true);
      showNotification('Refreshing profile...', 'info');

      // Get fresh profile data
      const profileResponse = await profilingAPI.getProfile(connectionId, tableName);
      setProfileData(profileResponse.data);

      showNotification('Profile refreshed successfully', 'success');
    } catch (error) {
      console.error(`Error refreshing profile for ${tableName}:`, error);
      showNotification(`Failed to refresh profile for ${tableName}`, 'error');
    } finally {
      setIsLoadingProfile(false);
    }
  };

  // Handle tab change
  const handleTabChange = (tab) => {
    setActiveTab(tab);
  };

  // If data is still loading, show loading state
  if (isLoadingTable && !tableData) {
    return (
      <div className="py-4">
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-semibold text-secondary-900">{tableName}</h1>
        </div>

        <div className="mt-6 flex justify-center">
          <LoadingSpinner size="lg" />
        </div>
      </div>
    );
  }

  return (
    <div className="py-4">
      {/* Table header */}
      <TableHeader
        tableName={tableName}
        connectionName={connection?.name}
        onRefreshProfile={handleRefreshProfile}
        isRefreshing={isLoadingProfile}
      />

      {/* Tabs */}
      <div className="mt-6 bg-white shadow rounded-lg overflow-hidden">
        <TableTabs
          activeTab={activeTab}
          onChange={handleTabChange}
          validationCount={validations.length}
        />

        {/* Tab content */}
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
              isLoading={isLoadingValidations}
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
      </div>
    </div>
  );
};

export default TableDetailPage;