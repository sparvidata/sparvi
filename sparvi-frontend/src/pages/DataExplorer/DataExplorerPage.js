import React, { useState, useEffect, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import {
  ServerIcon,
  ArrowPathIcon
} from '@heroicons/react/24/outline';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { schemaAPI, metadataAPI } from '../../api/enhancedApiService';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import EmptyState from '../../components/common/EmptyState';
import EnhancedSchemaBrowser from './components/EnhancedSchemaBrowser';

const DataExplorerPage = () => {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const { connections, activeConnection, setCurrentConnection } = useConnection();
  const { updateBreadcrumbs, showNotification, setLoading } = useUI();

  const [tables, setTables] = useState([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [isLoadingTables, setIsLoadingTables] = useState(false);
  const [metadataStatus, setMetadataStatus] = useState({});
  const [expandedNodes, setExpandedNodes] = useState({
    connection: true,
    'schema-public': true
  });
  const [hasAttemptedLoad, setHasAttemptedLoad] = useState(false);

  // Load connection from URL if provided
  useEffect(() => {
    const connectionId = searchParams.get('connection');
    if (connectionId && connections.length > 0) {
      const connection = connections.find(c => c.id === connectionId);
      if (connection && (!activeConnection || activeConnection.id !== connectionId)) {
        setCurrentConnection(connection);
      }
    }
  }, [searchParams, connections, activeConnection, setCurrentConnection]);

  // Set breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Data Explorer', href: '/explorer' }
    ]);
  }, [updateBreadcrumbs]);

  // Load tables for active connection - only when needed
  useEffect(() => {
    // Skip if no active connection
    if (!activeConnection || !activeConnection.id) {
      return;
    }

    // Flag to avoid state updates if component unmounts
    let isMounted = true;
    setIsLoadingTables(true);
    setLoading('explorer', true);

    // Load tables
    const fetchTables = async () => {
      try {
        // Get tables
        console.log("Fetching tables for connection:", activeConnection.id);
        const response = await schemaAPI.getTables(activeConnection.id);
        console.log("Tables response received:", response);

        // Process tables
        let extractedTables = [];
        if (response?.tables) {
          extractedTables = response.tables;
        } else if (response?.data?.tables) {
          extractedTables = response.data.tables;
        } else if (Array.isArray(response)) {
          extractedTables = response;
        }

        console.log("Extracted tables:", extractedTables);

        if (isMounted) {
          setTables(extractedTables || []);
          setIsLoadingTables(false);
          setLoading('explorer', false);
        }

        // Get metadata status in parallel
        try {
          const metaResponse = await metadataAPI.getMetadataStatus(activeConnection.id);
          if (isMounted) {
            setMetadataStatus(metaResponse?.data || {});
          }
        } catch (metaError) {
          console.error('Error loading metadata status:', metaError);
        }
      } catch (error) {
        console.error('Error loading tables:', error);
        if (isMounted) {
          showNotification('Failed to load tables', 'error');
          setIsLoadingTables(false); // Make sure to set loading to false even on error
          setLoading('explorer', false);
        }
      }
    };

    fetchTables();

    // Cleanup function
    return () => {
      isMounted = false;
    };
  }, [activeConnection, setLoading]);

  // Reset load attempt when connection changes
  useEffect(() => {
    setHasAttemptedLoad(false);
  }, [activeConnection?.id]);

  // Memoized handlers to prevent unnecessary rerenders
  const handleSearch = useCallback((query) => {
    setSearchQuery(query);
  }, []);

  const handleTableSelect = useCallback((tableName) => {
    if (!activeConnection) {
      showNotification('Please select a connection first', 'warning');
      return;
    }
    navigate(`/explorer/${activeConnection.id}/tables/${tableName}`);
  }, [activeConnection, navigate]);

  const toggleNodeExpanded = useCallback((nodeId) => {
    setExpandedNodes(prev => ({
      ...prev,
      [nodeId]: !prev[nodeId]
    }));
  }, []);

  // Handle metadata refresh
  const handleRefreshMetadata = async () => {
    if (!activeConnection) {
      showNotification('Please select a connection first', 'warning');
      return;
    }

    try {
      setLoading('refreshMetadata', true);
      await metadataAPI.refreshMetadata(activeConnection.id, 'schema');
      showNotification('Metadata refresh initiated', 'success');

      // Trigger a reload of tables after a short delay
      setTimeout(() => {
        setHasAttemptedLoad(false);
        setLoading('refreshMetadata', false);
      }, 2000);
    } catch (error) {
      console.error('Error refreshing metadata:', error);
      showNotification('Failed to refresh metadata', 'error');
      setLoading('refreshMetadata', false);
    }
  };

  // Format metadata freshness for display
  const getMetadataFreshness = () => {
    const tablesMeta = metadataStatus?.tables?.freshness;
    if (!tablesMeta) return null;

    const statusColors = {
      fresh: 'text-accent-500',
      stale: 'text-warning-500',
      unknown: 'text-secondary-400',
      error: 'text-danger-500'
    };

    return (
      <span className={statusColors[tablesMeta.status] || statusColors.unknown}>
        {tablesMeta.status === 'fresh' ? 'Up to date' :
         tablesMeta.status === 'stale' ? 'Needs refresh' :
         tablesMeta.status === 'error' ? 'Error' : 'Unknown'}
      </span>
    );
  };

  return (
    <div className="py-4">
      <div className="flex justify-between items-center mb-4">
        <h1 className="text-2xl font-semibold text-secondary-900">Data Explorer</h1>

        {activeConnection && (
          <div className="flex items-center space-x-2">
            <button
              onClick={handleRefreshMetadata}
              disabled={isLoadingTables}
              className="inline-flex items-center px-3 py-1.5 border border-secondary-300 shadow-sm text-sm font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
            >
              {isLoadingTables ? (
                <>
                  <LoadingSpinner size="sm" className="mr-2" />
                  Refreshing...
                </>
              ) : (
                <>
                  <ArrowPathIcon className="mr-2 h-4 w-4 text-secondary-500" />
                  Refresh Metadata
                </>
              )}
            </button>
          </div>
        )}
      </div>

      {activeConnection ? (
        <div className="mt-2 flex items-center">
          <ServerIcon className="h-5 w-5 text-secondary-500 mr-2" />
          <span className="font-medium text-secondary-700">{activeConnection.name}</span>
          <span className="mx-2 text-secondary-400">&bull;</span>
          <span className="text-sm text-secondary-500">
            Metadata: {getMetadataFreshness()}
          </span>
        </div>
      ) : (
        <div className="mt-2 p-4 bg-warning-50 border border-warning-200 rounded-md">
          <p className="text-warning-700">Please select a connection to view data.</p>
        </div>
      )}

      {activeConnection ? (
        <div className="mt-6">
          <div className="bg-white shadow rounded-lg">
            <div className="px-4 py-3 border-b border-secondary-200">
              <h3 className="text-lg font-medium text-secondary-900">Database Schema</h3>
            </div>
            <div className="p-4">
              {isLoadingTables ? (
                <div className="flex justify-center items-center h-64">
                  <LoadingSpinner size="lg" />
                </div>
              ) : (
                <EnhancedSchemaBrowser
                  tables={tables}  // Pre-loaded tables passed directly
                  activeConnection={activeConnection}
                  expandedNodes={expandedNodes}
                  toggleNodeExpanded={toggleNodeExpanded}
                  onTableSelect={handleTableSelect}
                  searchQuery={searchQuery}
                  onSearch={handleSearch}
                />
              )}
            </div>
          </div>
        </div>
      ) : (
        <div className="mt-6">
          <EmptyState
            icon={ServerIcon}
            title="No Connection Selected"
            description="Please select a connection to view tables and schema."
            actionText="View Connections"
            actionLink="/connections"
          />
        </div>
      )}
    </div>
  );
};

export default DataExplorerPage;