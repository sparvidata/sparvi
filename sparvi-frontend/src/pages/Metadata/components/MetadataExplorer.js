// src/pages/Metadata/components/MetadataExplorer.js
import React, { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { metadataAPI } from '../../../api/enhancedApiService';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import SearchInput from '../../../components/common/SearchInput';
import { MagnifyingGlassIcon, ArrowsUpDownIcon } from '@heroicons/react/24/outline';

const MetadataExplorer = ({ connectionId, metadataType, metadataStatus }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [sortField, setSortField] = useState('name');
  const [sortDirection, setSortDirection] = useState('asc');

  // Fetch metadata using react-query
  const {
    data: metadata,
    isLoading,
    error,
    refetch
  } = useQuery({
    queryKey: ['metadata', connectionId, metadataType],
    queryFn: () => metadataAPI.getMetadata(connectionId, metadataType),
    enabled: !!connectionId,
    select: (data) => {
      // Extract the metadata from the response - handle different response formats
      if (data?.data?.metadata) return data.data.metadata;
      if (data?.metadata) return data.metadata;
      return data;
    }
  });

  // Refetch when metadata type changes
  useEffect(() => {
    if (connectionId) {
      refetch();
    }
  }, [metadataType, connectionId, refetch]);

  // Handle search
  const handleSearch = (query) => {
    setSearchQuery(query);
  };

  // Handle sort
  const handleSort = (field) => {
    if (sortField === field) {
      // Toggle direction
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('asc');
    }
  };

  // Filter and sort metadata
  const filteredAndSortedMetadata = React.useMemo(() => {
    if (!metadata) return [];

    let items = Array.isArray(metadata) ? metadata :
                Object.keys(metadata).map(key => ({
                  name: key,
                  ...metadata[key]
                }));

    // Apply search filter
    if (searchQuery) {
      items = items.filter(item =>
        item.name?.toLowerCase().includes(searchQuery.toLowerCase())
      );
    }

    // Apply sorting
    items.sort((a, b) => {
      const aValue = a[sortField] || '';
      const bValue = b[sortField] || '';

      const comparison = typeof aValue === 'string'
        ? aValue.localeString?.compare(bValue) || aValue.localeCompare(bValue)
        : aValue - bValue;

      return sortDirection === 'asc' ? comparison : -comparison;
    });

    return items;
  }, [metadata, searchQuery, sortField, sortDirection]);

  // Render table based on metadata type
  const renderMetadataTable = () => {
    if (isLoading) {
      return (
        <div className="flex justify-center py-8">
          <LoadingSpinner size="lg" />
        </div>
      );
    }

    if (error) {
      return (
        <div className="text-center py-8">
          <p className="text-danger-500">Error loading metadata: {error.message}</p>
          <button
            onClick={() => refetch()}
            className="mt-2 inline-flex items-center px-3 py-1.5 border border-transparent text-xs font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          >
            Try Again
          </button>
        </div>
      );
    }

    if (!filteredAndSortedMetadata || filteredAndSortedMetadata.length === 0) {
      return (
        <div className="text-center py-8">
          <MagnifyingGlassIcon className="mx-auto h-10 w-10 text-secondary-400" />
          <p className="mt-2 text-secondary-500">No metadata found</p>
          {searchQuery && (
            <p className="text-secondary-400 text-sm">Try clearing your search or refreshing the metadata</p>
          )}
        </div>
      );
    }

    // Render based on metadata type
    switch (metadataType) {
      case 'tables':
        return renderTablesTable();
      case 'columns':
        return renderColumnsTable();
      case 'statistics':
        return renderStatisticsTable();
      default:
        return renderTablesTable();
    }
  };

  // Render tables metadata
  const renderTablesTable = () => {
    return (
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-secondary-200">
          <thead className="bg-secondary-50">
            <tr>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('name')}>
                  Table Name
                  <span className={`ml-2 flex-none rounded ${sortField === 'name' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('schema')}>
                  Schema
                  <span className={`ml-2 flex-none rounded ${sortField === 'schema' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('row_count')}>
                  Row Count
                  <span className={`ml-2 flex-none rounded ${sortField === 'row_count' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('updated_at')}>
                  Last Updated
                  <span className={`ml-2 flex-none rounded ${sortField === 'updated_at' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-secondary-200">
            {filteredAndSortedMetadata.map((table, index) => (
              <tr key={table.name || index} className={index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-secondary-900">{table.name}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">{table.schema || 'default'}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">{table.row_count?.toLocaleString() || '-'}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                  {table.updated_at ? new Date(table.updated_at).toLocaleString() : '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };

  // Render columns metadata
  const renderColumnsTable = () => {
    return (
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-secondary-200">
          <thead className="bg-secondary-50">
            <tr>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('table_name')}>
                  Table
                  <span className={`ml-2 flex-none rounded ${sortField === 'table_name' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('name')}>
                  Column
                  <span className={`ml-2 flex-none rounded ${sortField === 'name' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('data_type')}>
                  Data Type
                  <span className={`ml-2 flex-none rounded ${sortField === 'data_type' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('nullable')}>
                  Nullable
                  <span className={`ml-2 flex-none rounded ${sortField === 'nullable' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('updated_at')}>
                  Last Updated
                  <span className={`ml-2 flex-none rounded ${sortField === 'updated_at' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-secondary-200">
            {filteredAndSortedMetadata.map((column, index) => (
              <tr key={`${column.table_name}-${column.name}` || index} className={index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">{column.table_name}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-secondary-900">{column.name}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                  <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-secondary-100 text-secondary-800">
                    {column.data_type || '-'}
                  </span>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                  {column.nullable ? 'Yes' : 'No'}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                  {column.updated_at ? new Date(column.updated_at).toLocaleString() : '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };

  // Render statistics metadata
  const renderStatisticsTable = () => {
    return (
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-secondary-200">
          <thead className="bg-secondary-50">
            <tr>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('table_name')}>
                  Table
                  <span className={`ml-2 flex-none rounded ${sortField === 'table_name' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('column_name')}>
                  Column
                  <span className={`ml-2 flex-none rounded ${sortField === 'column_name' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('null_count')}>
                  Null %
                  <span className={`ml-2 flex-none rounded ${sortField === 'null_count' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('distinct_count')}>
                  Distinct Values
                  <span className={`ml-2 flex-none rounded ${sortField === 'distinct_count' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('updated_at')}>
                  Last Updated
                  <span className={`ml-2 flex-none rounded ${sortField === 'updated_at' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-secondary-200">
            {filteredAndSortedMetadata.map((stat, index) => (
              <tr key={`${stat.table_name}-${stat.column_name}` || index} className={index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">{stat.table_name}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-secondary-900">{stat.column_name}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                  {stat.null_count !== undefined ? `${((stat.null_count / stat.row_count) * 100).toFixed(1)}%` : '-'}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                  {stat.distinct_count?.toLocaleString() || '-'}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                  {stat.updated_at ? new Date(stat.updated_at).toLocaleString() : '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };

  return (
    <div>
      <div className="mb-4 flex justify-between items-center">
        <h3 className="text-lg font-medium text-secondary-900">
          {metadataType === 'tables' ? 'Tables' :
           metadataType === 'columns' ? 'Columns' :
           metadataType === 'statistics' ? 'Statistics' : 'Metadata'}
        </h3>
        <div className="w-64">
          <SearchInput
            onSearch={handleSearch}
            placeholder={`Search ${metadataType}...`}
            initialValue={searchQuery}
          />
        </div>
      </div>
      {renderMetadataTable()}
    </div>
  );
};

export default MetadataExplorer;