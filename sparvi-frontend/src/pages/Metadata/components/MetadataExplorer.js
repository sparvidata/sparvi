import React, { useState } from 'react';
import { useProgressiveMetadata } from '../../../hooks/useIntegratedMetadata';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import SearchInput from '../../../components/common/SearchInput';
import {
  MagnifyingGlassIcon,
  ArrowsUpDownIcon,
  CheckCircleIcon,
  ExclamationCircleIcon,
  ClockIcon
} from '@heroicons/react/24/outline';

const MetadataExplorer = ({ connectionId, metadataType, metadataStatus }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [sortField, setSortField] = useState('name');
  const [sortDirection, setSortDirection] = useState('asc');

  // Use progressive metadata loading
  const {
    tables,
    columns,
    statistics,
    summary,
    isLoadingInitial,
    isEnhancing,
    hasBasicTables,
    hasColumns,
    hasStatistics,
    error,
    refetch
  } = useProgressiveMetadata(connectionId, {
    enabled: !!connectionId,
    refetchInterval: false
  });

  // Handle search
  const handleSearch = (query) => {
    setSearchQuery(query);
  };

  // Handle sort
  const handleSort = (field) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('asc');
    }
  };

  // Get data based on selected metadata type
  const getDataForType = React.useCallback(() => {
    switch (metadataType) {
      case 'tables':
        return tables || [];
      case 'columns':
        return columns || [];
      case 'statistics':
        return statistics || [];
      default:
        return tables || [];
    }
  }, [metadataType, tables, columns, statistics]);

  // Filter and sort data
  const filteredAndSortedData = React.useMemo(() => {
    const data = getDataForType();
    let items = [...data];

    // Apply search filter
    if (searchQuery) {
      items = items.filter(item => {
        switch (metadataType) {
          case 'tables':
            return (item.name?.toLowerCase() || '').includes(searchQuery.toLowerCase());
          case 'columns':
            return (
              (item.name?.toLowerCase() || '').includes(searchQuery.toLowerCase()) ||
              (item.table_name?.toLowerCase() || '').includes(searchQuery.toLowerCase())
            );
          case 'statistics':
            return (
              (item.table_name?.toLowerCase() || '').includes(searchQuery.toLowerCase()) ||
              (item.column_name?.toLowerCase() || '').includes(searchQuery.toLowerCase()) ||
              (item.data_type?.toLowerCase() || '').includes(searchQuery.toLowerCase())
            );
          default:
            return (item.name?.toLowerCase() || '').includes(searchQuery.toLowerCase());
        }
      });
    }

    // Apply sorting
    items.sort((a, b) => {
      let aValue = a[sortField];
      let bValue = b[sortField];

      // Handle special cases for sorting
      if (sortField === 'row_count' || sortField === 'column_count') {
        aValue = Number(aValue) || 0;
        bValue = Number(bValue) || 0;
      } else if (typeof aValue === 'string' && typeof bValue === 'string') {
        aValue = aValue.toLowerCase();
        bValue = bValue.toLowerCase();
      } else {
        aValue = aValue || '';
        bValue = bValue || '';
      }

      let comparison = 0;
      if (typeof aValue === 'number' && typeof bValue === 'number') {
        comparison = aValue - bValue;
      } else {
        comparison = String(aValue).localeCompare(String(bValue));
      }

      return sortDirection === 'asc' ? comparison : -comparison;
    });

    return items;
  }, [getDataForType, searchQuery, sortField, sortDirection, metadataType]);

  // Render loading state
  const renderLoadingState = () => {
    if (isLoadingInitial) {
      return (
        <div className="flex justify-center py-8">
          <LoadingSpinner size="lg" />
          <span className="ml-3 text-secondary-600">Loading metadata...</span>
        </div>
      );
    }

    if (isEnhancing) {
      return (
        <div className="space-y-4">
          {/* Show basic tables while enhancing */}
          {renderDataTable()}
          <div className="flex justify-center py-4 border-t border-secondary-200">
            <LoadingSpinner size="sm" />
            <span className="ml-2 text-secondary-500">Loading detailed information...</span>
          </div>
        </div>
      );
    }

    return null;
  };

  // Render error state
  const renderErrorState = () => (
    <div className="text-center py-8">
      <ExclamationCircleIcon className="mx-auto h-12 w-12 text-danger-400" />
      <h3 className="mt-2 text-sm font-medium text-secondary-900">Error Loading Metadata</h3>
      <p className="mt-1 text-sm text-secondary-500">{error?.message || 'An unexpected error occurred'}</p>
      <button
        onClick={() => refetch()}
        className="mt-4 inline-flex items-center px-3 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
      >
        Try Again
      </button>
    </div>
  );

  // Render empty state
  const renderEmptyState = () => (
    <div className="text-center py-8">
      <MagnifyingGlassIcon className="mx-auto h-10 w-10 text-secondary-400" />
      <h3 className="mt-2 text-sm font-medium text-secondary-900">No Data Found</h3>
      <p className="mt-1 text-sm text-secondary-500">
        {searchQuery ? 'No items match your search criteria' : `No ${metadataType} found`}
      </p>
      {searchQuery && (
        <button
          onClick={() => setSearchQuery('')}
          className="mt-2 text-sm text-primary-600 hover:text-primary-800"
        >
          Clear search
        </button>
      )}
    </div>
  );

  // Render sort button
  const renderSortButton = (field, label) => (
    <button className="group inline-flex items-center" onClick={() => handleSort(field)}>
      {label}
      <span className={`ml-2 flex-none rounded ${
        sortField === field 
          ? 'bg-secondary-200 text-secondary-900' 
          : 'text-secondary-400 group-hover:bg-secondary-200'
      }`}>
        <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
      </span>
    </button>
  );

  // Format large numbers
  const formatNumber = (num) => {
    if (typeof num !== 'number') return '-';
    if (num === 0) return '0';
    return num.toLocaleString();
  };

  // Format percentage
  const formatPercentage = (num) => {
    if (typeof num !== 'number') return '-';
    return `${num.toFixed(2)}%`;
  };

  // Get data availability indicator
  const getDataAvailabilityIndicator = () => {
    const indicators = [];

    if (hasBasicTables) {
      indicators.push({ label: 'Tables', available: true });
    }
    if (hasColumns) {
      indicators.push({ label: 'Columns', available: true });
    } else if (isEnhancing) {
      indicators.push({ label: 'Columns', available: false, loading: true });
    }
    if (hasStatistics) {
      indicators.push({ label: 'Statistics', available: true });
    } else if (isEnhancing) {
      indicators.push({ label: 'Statistics', available: false, loading: true });
    }

    return indicators;
  };

  // Render tables metadata
  const renderTablesTable = () => (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-secondary-200">
        <thead className="bg-secondary-50">
          <tr>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('name', 'Table Name')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('schema', 'Schema')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('row_count', 'Row Count')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('column_count', 'Column Count')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('health_score', 'Health Score')}
            </th>
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-secondary-200">
          {filteredAndSortedData.map((table, index) => (
            <tr key={table.id || table.name || index} className={index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
              <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-secondary-900">
                <div className="flex items-center">
                  {table.name}
                  {table.has_primary_key && (
                    <CheckCircleIcon className="ml-2 h-4 w-4 text-accent-500" title="Has primary key" />
                  )}
                </div>
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                {table.schema || 'default'}
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                <div className="flex items-center">
                  {formatNumber(table.row_count)}
                  {!hasStatistics && isEnhancing && (
                    <ClockIcon className="ml-1 h-3 w-3 text-secondary-400" title="Loading statistics..." />
                  )}
                </div>
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                <div className="flex items-center">
                  {formatNumber(table.column_count)}
                  {!hasColumns && isEnhancing && (
                    <ClockIcon className="ml-1 h-3 w-3 text-secondary-400" title="Loading columns..." />
                  )}
                </div>
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                {table.health_score ? (
                  <div className="flex items-center">
                    <div className="flex-1 bg-secondary-200 rounded-full h-2 mr-2">
                      <div
                        className={`h-2 rounded-full ${
                          table.health_score >= 80 ? 'bg-accent-500' : 
                          table.health_score >= 60 ? 'bg-warning-500' : 'bg-danger-500'
                        }`}
                        style={{ width: `${table.health_score}%` }}
                      />
                    </div>
                    <span className="text-xs">{table.health_score}</span>
                  </div>
                ) : (
                  '-'
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );

  // Render columns metadata
  const renderColumnsTable = () => (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-secondary-200">
        <thead className="bg-secondary-50">
          <tr>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('table_name', 'Table')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('name', 'Column')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('type', 'Data Type')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('nullable', 'Nullable')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('null_percentage', 'Null %')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('distinct_count', 'Distinct Values')}
            </th>
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-secondary-200">
          {filteredAndSortedData.map((column, index) => (
            <tr key={`${column.table_name}-${column.name}` || index} className={index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                {column.table_name}
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-secondary-900">
                <div className="flex items-center">
                  {column.name}
                  {column.is_unique && (
                    <CheckCircleIcon className="ml-2 h-4 w-4 text-accent-500" title="Unique values" />
                  )}
                </div>
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-secondary-100 text-secondary-800">
                  {column.type || column.data_type || 'unknown'}
                </span>
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                {column.nullable === true ? 'Yes' : column.nullable === false ? 'No' : '-'}
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                {column.null_percentage !== undefined ? (
                  <div className="flex items-center">
                    <div className="flex-1 bg-secondary-200 rounded-full h-2 mr-2 w-12">
                      <div
                        className="h-2 rounded-full bg-danger-400"
                        style={{ width: `${Math.min(100, column.null_percentage)}%` }}
                      />
                    </div>
                    <span className="text-xs">{formatPercentage(column.null_percentage)}</span>
                  </div>
                ) : (
                  '-'
                )}
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                {formatNumber(column.distinct_count)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );

  // Render statistics metadata
  const renderStatisticsTable = () => (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-secondary-200">
        <thead className="bg-secondary-50">
          <tr>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('table_name', 'Table')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('column_name', 'Column')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('data_type', 'Data Type')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('null_percentage', 'Null %')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('distinct_percentage', 'Distinct %')}
            </th>
            <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
              {renderSortButton('is_unique', 'Unique')}
            </th>
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-secondary-200">
          {filteredAndSortedData.map((stat, index) => (
            <tr key={`${stat.table_name}-${stat.column_name}` || index} className={index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                {stat.table_name}
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-secondary-900">
                {stat.column_name}
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-secondary-100 text-secondary-800">
                  {stat.data_type || 'unknown'}
                </span>
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                {stat.null_percentage !== undefined ? (
                  <div className="flex items-center">
                    <div className="flex-1 bg-secondary-200 rounded-full h-2 mr-2 w-12">
                      <div
                        className="h-2 rounded-full bg-danger-400"
                        style={{ width: `${Math.min(100, stat.null_percentage)}%` }}
                      />
                    </div>
                    <span className="text-xs">{formatPercentage(stat.null_percentage)}</span>
                  </div>
                ) : (
                  '-'
                )}
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                {stat.distinct_percentage !== undefined ? (
                  <div className="flex items-center">
                    <div className="flex-1 bg-secondary-200 rounded-full h-2 mr-2 w-12">
                      <div
                        className="h-2 rounded-full bg-accent-400"
                        style={{ width: `${Math.min(100, stat.distinct_percentage)}%` }}
                      />
                    </div>
                    <span className="text-xs">{formatPercentage(stat.distinct_percentage)}</span>
                  </div>
                ) : (
                  '-'
                )}
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                {stat.is_unique === true ? (
                  <CheckCircleIcon className="h-4 w-4 text-accent-500" />
                ) : stat.is_unique === false ? (
                  <span className="text-secondary-400">No</span>
                ) : (
                  '-'
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );

  // Render the appropriate table based on metadata type
  const renderDataTable = () => {
    if (filteredAndSortedData.length === 0) {
      return renderEmptyState();
    }

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

  // Handle error state
  if (error && !hasBasicTables) {
    return renderErrorState();
  }

  // Handle initial loading
  if (isLoadingInitial) {
    return renderLoadingState();
  }

  return (
    <div>
      {/* Header with data availability indicators */}
      <div className="mb-4 flex justify-between items-center">
        <div>
          <h3 className="text-lg font-medium text-secondary-900 flex items-center">
            {metadataType === 'tables' ? 'Tables' :
             metadataType === 'columns' ? 'Columns' :
             metadataType === 'statistics' ? 'Statistics' : 'Metadata'}

            {/* Data availability indicators */}
            <div className="ml-4 flex items-center space-x-2">
              {getDataAvailabilityIndicator().map((indicator, index) => (
                <div key={index} className="flex items-center">
                  {indicator.loading ? (
                    <ClockIcon className="h-4 w-4 text-secondary-400" />
                  ) : indicator.available ? (
                    <CheckCircleIcon className="h-4 w-4 text-accent-500" />
                  ) : (
                    <ExclamationCircleIcon className="h-4 w-4 text-secondary-400" />
                  )}
                  <span className={`ml-1 text-xs ${
                    indicator.available ? 'text-accent-600' : 'text-secondary-500'
                  }`}>
                    {indicator.label}
                  </span>
                </div>
              ))}
            </div>
          </h3>

          {/* Summary information */}
          {summary && (
            <p className="mt-1 text-sm text-secondary-500">
              {summary.totalTables} tables • {formatNumber(summary.totalRows)} total rows • {summary.totalColumns} columns
              {isEnhancing && ' • Loading enhanced data...'}
            </p>
          )}
        </div>

        {/* Search input */}
        <div className="w-64">
          <SearchInput
            onSearch={handleSearch}
            placeholder={`Search ${metadataType}...`}
            initialValue={searchQuery}
          />
        </div>
      </div>

      {/* Data table or loading state */}
      {renderLoadingState() || renderDataTable()}

      {/* Show enhancing indicator if we have basic data but are loading more */}
      {isEnhancing && hasBasicTables && (
        <div className="mt-4 text-center">
          <div className="inline-flex items-center px-3 py-2 border border-secondary-200 rounded-md bg-secondary-50">
            <LoadingSpinner size="sm" className="mr-2" />
            <span className="text-sm text-secondary-600">
              Loading {metadataType === 'tables' ? 'row counts and column details' : 'detailed statistics'}...
            </span>
          </div>
        </div>
      )}
    </div>
  );
};

export default MetadataExplorer;