import React, { useState } from 'react';
import {
  TableCellsIcon,
  MagnifyingGlassIcon,
  ArrowsUpDownIcon,
  InformationCircleIcon
} from '@heroicons/react/24/outline';
import SearchInput from '../../../components/common/SearchInput';

const HistoricalMetadataViewer = ({ data, date, metadataType, tableName }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [sortField, setSortField] = useState(metadataType === 'tables' ? 'name' : 'table_name');
  const [sortDirection, setSortDirection] = useState('asc');

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

  // Process and filter data
  const processedData = React.useMemo(() => {
    if (!data || !data.metadata) return [];

    let items = [];

    if (metadataType === 'tables') {
      // Extract tables from metadata
      if (Array.isArray(data.metadata)) {
        items = data.metadata;
      } else if (data.metadata.tables) {
        items = Object.keys(data.metadata.tables).map(tableName => ({
          name: tableName,
          ...data.metadata.tables[tableName]
        }));
      }
    } else if (metadataType === 'columns') {
      // Extract columns from metadata
      if (data.metadata.columns_by_table) {
        Object.entries(data.metadata.columns_by_table).forEach(([table, columns]) => {
          columns.forEach(column => {
            items.push({
              ...column,
              table_name: table
            });
          });
        });
      } else if (data.metadata.columns) {
        items = data.metadata.columns;
      }
    }

    // Apply search filter
    if (searchQuery) {
      items = items.filter(item => {
        const searchableText = metadataType === 'tables'
          ? item.name?.toLowerCase() || ''
          : `${item.table_name?.toLowerCase() || ''} ${item.name?.toLowerCase() || ''}`;
        return searchableText.includes(searchQuery.toLowerCase());
      });
    }

    // Apply sorting
    items.sort((a, b) => {
      const aValue = a[sortField] ?? '';
      const bValue = b[sortField] ?? '';

      if (typeof aValue === 'string' && typeof bValue === 'string') {
        const comparison = aValue.localeCompare(bValue);
        return sortDirection === 'asc' ? comparison : -comparison;
      } else {
        const comparison = (aValue || 0) - (bValue || 0);
        return sortDirection === 'asc' ? comparison : -comparison;
      }
    });

    return items;
  }, [data, searchQuery, sortField, sortDirection, metadataType]);

  // Render sort button
  const renderSortButton = (field, label) => (
    <button
      className="group inline-flex items-center"
      onClick={() => handleSort(field)}
    >
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

  // Empty state
  if (!data) {
    return (
      <div className="text-center py-8">
        <InformationCircleIcon className="mx-auto h-12 w-12 text-secondary-400" />
        <h3 className="mt-2 text-sm font-medium text-secondary-900">No Historical Data</h3>
        <p className="mt-1 text-sm text-secondary-500">
          No metadata snapshot found for {date}
        </p>
      </div>
    );
  }

  if (processedData.length === 0) {
    return (
      <div className="text-center py-8">
        <MagnifyingGlassIcon className="mx-auto h-10 w-10 text-secondary-400" />
        <p className="mt-2 text-secondary-500">
          {searchQuery ? 'No items match your search' : 'No metadata found'}
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
  }

  return (
    <div className="bg-white shadow rounded-lg">
      {/* Header */}
      <div className="px-6 py-4 border-b border-secondary-200">
        <div className="flex justify-between items-center">
          <div>
            <h3 className="text-lg font-medium text-secondary-900">
              Historical {metadataType === 'tables' ? 'Tables' : 'Columns'}
            </h3>
            <p className="mt-1 text-sm text-secondary-500">
              Snapshot from {new Date(date).toLocaleDateString()}
              {tableName && ` • Table: ${tableName}`}
              • {processedData.length} {processedData.length === 1 ? 'item' : 'items'}
            </p>
          </div>
          <div className="w-64">
            <SearchInput
              onSearch={handleSearch}
              placeholder={`Search ${metadataType}...`}
              initialValue={searchQuery}
            />
          </div>
        </div>
      </div>

      {/* Data Display */}
      <div className="overflow-x-auto">
        {metadataType === 'tables' ? (
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
                  {renderSortButton('size_bytes', 'Size')}
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-secondary-200">
              {processedData.map((table, index) => (
                <tr key={table.name || index} className={index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-secondary-900">
                    {table.name}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                    {table.schema || 'default'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                    {table.row_count?.toLocaleString() || '-'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                    {table.column_count?.toLocaleString() || '-'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                    {table.size_bytes ? formatBytes(table.size_bytes) : '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
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
                  {renderSortButton('data_type', 'Data Type')}
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                  {renderSortButton('nullable', 'Nullable')}
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                  {renderSortButton('ordinal_position', 'Position')}
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                  Default Value
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-secondary-200">
              {processedData.map((column, index) => (
                <tr key={`${column.table_name}-${column.name}` || index} className={index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                    {column.table_name}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-secondary-900">
                    {column.name}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                    <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-secondary-100 text-secondary-800">
                      {column.data_type || column.type || '-'}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                    {column.nullable === true ? 'Yes' : column.nullable === false ? 'No' : '-'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                    {column.ordinal_position || '-'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                    {column.column_default || '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Footer */}
      <div className="px-6 py-3 bg-secondary-50 border-t border-secondary-200">
        <div className="flex justify-between items-center text-sm text-secondary-500">
          <span>
            Collected at: {data.collected_at ? new Date(data.collected_at).toLocaleString() : 'Unknown'}
          </span>
          <span>
            Refresh frequency: {data.refresh_frequency || 'Unknown'}
          </span>
        </div>
      </div>
    </div>
  );
};

// Helper function to format bytes
const formatBytes = (bytes) => {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

export default HistoricalMetadataViewer;