import React, { useState, useEffect } from 'react';
import { Link, useNavigate, useSearchParams } from 'react-router-dom';
import {
  MagnifyingGlassIcon,
  TableCellsIcon,
  ServerIcon,
  ChevronDownIcon,
  ChevronRightIcon
} from '@heroicons/react/24/outline';
import { useConnection } from '../../contexts/ConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { schemaAPI, metadataAPI } from '../../api/apiService';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import EmptyState from '../../components/common/EmptyState';
import SchemaBrowser from './components/SchemaBrowser';
import TableList from './components/TableList';

const DataExplorerPage = () => {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const { connections, activeConnection, setCurrentConnection } = useConnection();
  const { updateBreadcrumbs, showNotification, setLoading } = useUI();

  const [tables, setTables] = useState([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [isLoadingTables, setIsLoadingTables] = useState(false);
  const [metadataStatus, setMetadataStatus] = useState({});
  const [expandedNodes, setExpandedNodes] = useState({});

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

  // Load tables for active connection
  useEffect(() => {
    // Skip if no active connection
    if (!activeConnection) return;

    const connectionId = activeConnection.id;

    if (!connectionId) return;

    // Flag to avoid state updates if component unmounts
    let isMounted = true;

    // Load tables
    async function fetchTables() {
      try {
        if (isMounted) {
          setIsLoadingTables(true);
          setLoading('explorer', true);
        }

        // Get tables
        const tablesResponse = await schemaAPI.getTables(connectionId);

        // Get metadata status
        const metadataResponse = await metadataAPI.getMetadataStatus(connectionId);

        // Only update state if component is still mounted
        if (isMounted) {
          setTables(tablesResponse.data.tables || []);
          setMetadataStatus(metadataResponse.data);
        }
      } catch (error) {
        console.error('Error loading tables:', error);
        if (isMounted) {
          showNotification('Failed to load tables', 'error');
        }
      } finally {
        if (isMounted) {
          setIsLoadingTables(false);
          setLoading('explorer', false);
        }
      }
    }

    fetchTables();

    // Cleanup function to prevent state updates after unmount
    return () => {
      isMounted = false;
    };

  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeConnection?.id]); // Only depend on connection ID, not functions

  // Handle search
  const handleSearch = (e) => {
    setSearchQuery(e.target.value);
  };

  // Filter tables by search query
  const filteredTables = tables.filter(table =>
    table.toLowerCase().includes(searchQuery.toLowerCase())
  );

  // Handle metadata refresh
  const handleRefreshMetadata = async () => {
    if (!activeConnection) return;

    try {
      setLoading('refreshMetadata', true);
      await metadataAPI.refreshMetadata(activeConnection.id, 'schema');
      showNotification('Metadata refresh initiated', 'success');

      // Refresh tables after a short delay
      setTimeout(async () => {
        try {
          const tablesResponse = await schemaAPI.getTables(activeConnection.id);
          setTables(tablesResponse.data.tables || []);

          const metadataResponse = await metadataAPI.getMetadataStatus(activeConnection.id);
          setMetadataStatus(metadataResponse.data);
        } catch (error) {
          console.error('Error refreshing tables:', error);
        } finally {
          setLoading('refreshMetadata', false);
        }
      }, 2000);
    } catch (error) {
      console.error('Error refreshing metadata:', error);
      showNotification('Failed to refresh metadata', 'error');
      setLoading('refreshMetadata', false);
    }
  };

  // Handle table selection
  const handleTableSelect = (tableName) => {
    if (!activeConnection) return;
    navigate(`/explorer/${activeConnection.id}/tables/${tableName}`);
  };

  // Toggle expanded state for schema browser nodes
  const toggleNodeExpanded = (nodeId) => {
    setExpandedNodes(prev => ({
      ...prev,
      [nodeId]: !prev[nodeId]
    }));
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

  // Rest of your component remains the same...
  // ... (all the rest of the code)

  return (
    <div className="py-4">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold text-secondary-900">Data Explorer</h1>

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
                <svg className="mr-2 h-4 w-4 text-secondary-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
                Refresh Metadata
              </>
            )}
          </button>
        </div>
      </div>

      <div className="mt-2 flex items-center">
        <ServerIcon className="h-5 w-5 text-secondary-500 mr-2" />
        <span className="font-medium text-secondary-700">{activeConnection.name}</span>
        <span className="mx-2 text-secondary-400">&bull;</span>
        <span className="text-sm text-secondary-500">
          Metadata: {getMetadataFreshness()}
        </span>
      </div>

      <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-12">
        {/* Side panel with schema browser */}
        <div className="lg:col-span-3">
          <div className="bg-white shadow rounded-lg">
            <div className="px-4 py-3 border-b border-secondary-200">
              <h3 className="text-sm font-medium text-secondary-900">Schema Browser</h3>
            </div>
            <div className="p-4">
              <SchemaBrowser
                tables={tables}
                activeConnection={activeConnection}
                expandedNodes={expandedNodes}
                toggleNodeExpanded={toggleNodeExpanded}
                onTableSelect={handleTableSelect}
              />
            </div>
          </div>
        </div>

        {/* Main content with table list */}
        <div className="lg:col-span-9">
          <div className="bg-white shadow rounded-lg">
            <div className="px-4 py-3 border-b border-secondary-200">
              <div className="flex items-center justify-between">
                <h3 className="text-lg font-medium text-secondary-900">Tables</h3>

                <div className="relative max-w-xs w-full">
                  <input
                    type="text"
                    className="block w-full pr-10 pl-10 py-2 border border-secondary-300 rounded-md leading-5 bg-white placeholder-secondary-500 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                    placeholder="Search tables..."
                    value={searchQuery}
                    onChange={handleSearch}
                  />
                  <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                    <MagnifyingGlassIcon className="h-5 w-5 text-secondary-400" aria-hidden="true" />
                  </div>
                </div>
              </div>
            </div>

            <div>
              {isLoadingTables ? (
                <div className="flex justify-center items-center h-64">
                  <LoadingSpinner size="lg" />
                </div>
              ) : (
                <TableList
                  tables={filteredTables}
                  searchQuery={searchQuery}
                  onTableSelect={handleTableSelect}
                  connectionId={activeConnection.id}
                />
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DataExplorerPage;