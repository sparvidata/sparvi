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
    data: metadataResponse,
    isLoading,
    error,
    refetch
  } = useQuery({
    queryKey: ['metadata', connectionId, metadataType],
    queryFn: () => metadataAPI.getMetadata(connectionId, metadataType),
    enabled: !!connectionId
  });

  // Refetch when metadata type changes
  useEffect(() => {
    if (connectionId) {
      refetch();
    }
  }, [metadataType, connectionId, refetch]);

  // Extract and process metadata based on the type
  const processedMetadata = React.useMemo(() => {
    if (!metadataResponse) return null;

    console.log('Raw metadata response:', metadataResponse);

    // Extract the metadata from the nested structure
    let extractedData = null;

    // Handle different response formats and extract the relevant data
    if (metadataType === 'tables') {
      if (metadataResponse?.metadata?.metadata?.tables) {
        extractedData = metadataResponse.metadata.metadata.tables;
      } else if (metadataResponse?.metadata?.tables) {
        extractedData = metadataResponse.metadata.tables;
      } else if (metadataResponse?.tables) {
        extractedData = metadataResponse.tables;
      } else if (Array.isArray(metadataResponse?.metadata)) {
        extractedData = metadataResponse.metadata;
      } else if (Array.isArray(metadataResponse)) {
        extractedData = metadataResponse;
      }

      // Tables might be in a special format
      if (metadataResponse?.metadata?.metadata?.metadata?.tables) {
        extractedData = metadataResponse.metadata.metadata.metadata.tables;
      }
    } else if (metadataType === 'columns') {
      // New format with columns organized by table
      if (metadataResponse?.metadata?.metadata?.columns_by_table) {
        const columnsByTable = metadataResponse.metadata.metadata.columns_by_table;
        // Flatten the columns from all tables
        const flattenedColumns = [];

        Object.entries(columnsByTable).forEach(([tableName, columns]) => {
          columns.forEach(column => {
            flattenedColumns.push({
              ...column,
              table_name: tableName
            });
          });
        });

        extractedData = flattenedColumns;
      } else if (metadataResponse?.metadata?.metadata?.columns) {
        extractedData = metadataResponse.metadata.metadata.columns;
      } else if (metadataResponse?.metadata?.columns) {
        extractedData = metadataResponse.metadata.columns;
      } else if (metadataResponse?.columns) {
        extractedData = metadataResponse.columns;
      } else if (Array.isArray(metadataResponse?.metadata)) {
        extractedData = metadataResponse.metadata;
      } else if (Array.isArray(metadataResponse)) {
        extractedData = metadataResponse;
      }
    } else if (metadataType === 'statistics') {
      // Handle the new statistics_by_table structure
      if (metadataResponse?.metadata?.metadata?.statistics_by_table) {
        // New structure: statistics organized by table
        const statsByTable = metadataResponse.metadata.metadata.statistics_by_table;
        const flattenedStats = [];

        // Flatten the structure for display in the table
        Object.entries(statsByTable).forEach(([tableName, tableData]) => {
          // If tableData has column_statistics, process each column
          if (tableData.column_statistics) {
            Object.entries(tableData.column_statistics).forEach(([columnName, columnStats]) => {
              // Extract the basic stats info from the column
              const basicStats = columnStats.basic || {};
              const numericStats = columnStats.numeric || {};
              const stringStats = columnStats.string || {};

              // Create a flattened record with the most relevant information
              flattenedStats.push({
                table_name: tableName,
                column_name: columnName,
                data_type: columnStats.type || 'unknown',
                null_count: basicStats.null_count,
                null_percentage: basicStats.null_percentage,
                distinct_count: basicStats.distinct_count,
                distinct_percentage: basicStats.distinct_percentage,
                is_unique: basicStats.is_unique,
                row_count: tableData.general?.row_count || 0,

                // Include additional stats based on type
                min: numericStats.min || stringStats.min_length,
                max: numericStats.max || stringStats.max_length,
                avg: numericStats.avg || stringStats.avg_length
              });
            });
          }
        });

        extractedData = flattenedStats;
      } else if (metadataResponse?.metadata?.statistics_by_table) {
        // Same logic but with different nesting level
        const statsByTable = metadataResponse.metadata.statistics_by_table;
        const flattenedStats = [];

        Object.entries(statsByTable).forEach(([tableName, tableData]) => {
          if (tableData.column_statistics) {
            Object.entries(tableData.column_statistics).forEach(([columnName, columnStats]) => {
              const basicStats = columnStats.basic || {};
              const numericStats = columnStats.numeric || {};
              const stringStats = columnStats.string || {};

              flattenedStats.push({
                table_name: tableName,
                column_name: columnName,
                data_type: columnStats.type || 'unknown',
                null_count: basicStats.null_count,
                null_percentage: basicStats.null_percentage,
                distinct_count: basicStats.distinct_count,
                distinct_percentage: basicStats.distinct_percentage,
                is_unique: basicStats.is_unique,
                row_count: tableData.general?.row_count || 0,
                min: numericStats.min || stringStats.min_length,
                max: numericStats.max || stringStats.max_length,
                avg: numericStats.avg || stringStats.avg_length
              });
            });
          }
        });

        extractedData = flattenedStats;
      } else if (metadataResponse?.statistics_by_table) {
        // Same logic but with different nesting level
        const statsByTable = metadataResponse.statistics_by_table;
        const flattenedStats = [];

        Object.entries(statsByTable).forEach(([tableName, tableData]) => {
          if (tableData.column_statistics) {
            Object.entries(tableData.column_statistics).forEach(([columnName, columnStats]) => {
              const basicStats = columnStats.basic || {};
              const numericStats = columnStats.numeric || {};
              const stringStats = columnStats.string || {};

              flattenedStats.push({
                table_name: tableName,
                column_name: columnName,
                data_type: columnStats.type || 'unknown',
                null_count: basicStats.null_count,
                null_percentage: basicStats.null_percentage,
                distinct_count: basicStats.distinct_count,
                distinct_percentage: basicStats.distinct_percentage,
                is_unique: basicStats.is_unique,
                row_count: tableData.general?.row_count || 0,
                min: numericStats.min || stringStats.min_length,
                max: numericStats.max || stringStats.max_length,
                avg: numericStats.avg || stringStats.avg_length
              });
            });
          }
        });

        extractedData = flattenedStats;
      } else {
        // Fall back to previous paths just in case
        if (metadataResponse?.metadata?.metadata?.statistics) {
          extractedData = metadataResponse.metadata.metadata.statistics;
        } else if (metadataResponse?.metadata?.statistics) {
          extractedData = metadataResponse.metadata.statistics;
        } else if (metadataResponse?.statistics) {
          extractedData = metadataResponse.statistics;
        } else if (Array.isArray(metadataResponse?.metadata)) {
          extractedData = metadataResponse.metadata;
        } else if (Array.isArray(metadataResponse)) {
          extractedData = metadataResponse;
        }
      }
    }

    // If we've got an object with table/column/stats inside, transform to array
    if (extractedData && !Array.isArray(extractedData)) {
      extractedData = Object.keys(extractedData).map(key => ({
        name: key,
        ...extractedData[key]
      }));
    }

    console.log('Processed metadata:', extractedData);
    return extractedData || [];
  }, [metadataResponse, metadataType]);

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
    if (!processedMetadata) return [];

    let items = [...processedMetadata];

    // Apply search filter
    if (searchQuery) {
      items = items.filter(item => {
        // For tables, search by name
        if (metadataType === 'tables') {
          return (item.name?.toLowerCase() || '').includes(searchQuery.toLowerCase());
        }
        // For columns, search by name and table name
        else if (metadataType === 'columns') {
          return (
            (item.name?.toLowerCase() || '').includes(searchQuery.toLowerCase()) ||
            (item.table_name?.toLowerCase() || '').includes(searchQuery.toLowerCase())
          );
        }
        // For statistics, search by table name and column name
        else if (metadataType === 'statistics') {
          return (
            (item.table_name?.toLowerCase() || '').includes(searchQuery.toLowerCase()) ||
            (item.column_name?.toLowerCase() || '').includes(searchQuery.toLowerCase()) ||
            (item.data_type?.toLowerCase() || '').includes(searchQuery.toLowerCase())
          );
        }
        // Default case
        return (item.name?.toLowerCase() || '').includes(searchQuery.toLowerCase());
      });
    }

    // Apply sorting
    items.sort((a, b) => {
      const aValue = a[sortField] ?? '';
      const bValue = b[sortField] ?? '';

      // Handle different data types for sorting
      if (typeof aValue === 'string' && typeof bValue === 'string') {
        const comparison = aValue.localeCompare(bValue);
        return sortDirection === 'asc' ? comparison : -comparison;
      } else {
        // Numeric comparison
        const comparison = (aValue || 0) - (bValue || 0);
        return sortDirection === 'asc' ? comparison : -comparison;
      }
    });

    return items;
  }, [processedMetadata, searchQuery, sortField, sortDirection, metadataType]);

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
                <button className="group inline-flex items-center" onClick={() => handleSort('column_count')}>
                  Column Count
                  <span className={`ml-2 flex-none rounded ${sortField === 'column_count' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-secondary-200">
            {filteredAndSortedMetadata.map((table, index) => (
              <tr key={table.id || table.name || index} className={index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-secondary-900">{table.name}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">{table.schema || 'default'}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">{table.row_count?.toLocaleString() || '-'}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">{table.column_count?.toLocaleString() || '-'}</td>
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
                <button className="group inline-flex items-center" onClick={() => handleSort('type')}>
                  Data Type
                  <span className={`ml-2 flex-none rounded ${sortField === 'type' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
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
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-secondary-200">
            {filteredAndSortedMetadata.map((column, index) => (
              <tr key={`${column.table_name}-${column.name}` || index} className={index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">{column.table_name}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-secondary-900">{column.name}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                  <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-secondary-100 text-secondary-800">
                    {column.type || column.data_type || '-'}
                  </span>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                  {column.nullable === true ? 'Yes' :
                   column.nullable === false ? 'No' : '-'}
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
                <button className="group inline-flex items-center" onClick={() => handleSort('data_type')}>
                  Data Type
                  <span className={`ml-2 flex-none rounded ${sortField === 'data_type' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('null_percentage')}>
                  Null %
                  <span className={`ml-2 flex-none rounded ${sortField === 'null_percentage' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('distinct_percentage')}>
                  Distinct %
                  <span className={`ml-2 flex-none rounded ${sortField === 'distinct_percentage' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button className="group inline-flex items-center" onClick={() => handleSort('is_unique')}>
                  Unique
                  <span className={`ml-2 flex-none rounded ${sortField === 'is_unique' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
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
                  <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-secondary-100 text-secondary-800">
                    {stat.data_type || '-'}
                  </span>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                  {typeof stat.null_percentage === 'number'
                    ? `${stat.null_percentage.toFixed(2)}%`
                    : '-'}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                  {typeof stat.distinct_percentage === 'number'
                    ? `${stat.distinct_percentage.toFixed(2)}%`
                    : '-'}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                  {stat.is_unique === true ? 'Yes' : stat.is_unique === false ? 'No' : '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };

  // Render metadata explorer with search and table
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