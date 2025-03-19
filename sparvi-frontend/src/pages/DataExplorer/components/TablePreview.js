import React, { useState, useEffect } from 'react';
import {
  EyeIcon,
  ExclamationCircleIcon,
  InformationCircleIcon,
  ArrowsUpDownIcon
} from '@heroicons/react/24/outline';
import { schemaAPI } from '../../../api/enhancedApiService';
import { useUI } from '../../../contexts/UIContext';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

const TablePreview = ({ connectionId, tableName }) => {
  const { showNotification } = useUI();

  const [previewData, setPreviewData] = useState([]);
  const [columns, setColumns] = useState([]);
  const [loading, setLoading] = useState(true);
  const [maxRows, setMaxRows] = useState(50);
  const [restrictedColumns, setRestrictedColumns] = useState([]);
  const [sortColumn, setSortColumn] = useState(null);
  const [sortDirection, setSortDirection] = useState('asc');

  // Load preview data
  useEffect(() => {
    const loadPreview = async () => {
      if (!connectionId || !tableName) return;

      try {
        setLoading(true);

        const response = await schemaAPI.getPreview(connectionId, tableName, maxRows);

        setPreviewData(response.data.preview_data || []);
        setRestrictedColumns(response.data.restricted_columns || []);

        // Extract columns from first row of data or from response
        if (response.data.preview_data && response.data.preview_data.length > 0) {
          setColumns(Object.keys(response.data.preview_data[0]));
        } else if (response.data.all_columns) {
          setColumns(response.data.all_columns);
        } else {
          setColumns([]);
        }
      } catch (error) {
        console.error(`Error loading preview for ${tableName}:`, error);
        showNotification(`Failed to load preview for ${tableName}`, 'error');
      } finally {
        setLoading(false);
      }
    };

    loadPreview();
  }, [connectionId, tableName, maxRows]);

  // Handle max rows change
  const handleMaxRowsChange = (e) => {
    const value = parseInt(e.target.value, 10);
    setMaxRows(value);
  };

  // Handle sort
  const handleSort = (column) => {
    if (sortColumn === column) {
      // Toggle direction if same column
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      // Set new sort column and default to asc
      setSortColumn(column);
      setSortDirection('asc');
    }
  };

  // Sort data
  const sortedData = React.useMemo(() => {
    if (!sortColumn) return previewData;

    return [...previewData].sort((a, b) => {
      let aValue = a[sortColumn];
      let bValue = b[sortColumn];

      // Handle null values
      if (aValue === null) return sortDirection === 'asc' ? -1 : 1;
      if (bValue === null) return sortDirection === 'asc' ? 1 : -1;

      // Compare based on type
      if (typeof aValue === 'string' && typeof bValue === 'string') {
        return sortDirection === 'asc'
          ? aValue.localeCompare(bValue)
          : bValue.localeCompare(aValue);
      } else {
        return sortDirection === 'asc'
          ? (aValue < bValue ? -1 : aValue > bValue ? 1 : 0)
          : (bValue < aValue ? -1 : bValue > aValue ? 1 : 0);
      }
    });
  }, [previewData, sortColumn, sortDirection]);

  // If loading with no data, show loading state
  if (loading && !previewData.length) {
    return (
      <div className="flex justify-center py-10">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  // If no data, show empty state
  if (!previewData.length && !loading) {
    return (
      <div className="text-center py-10">
        <ExclamationCircleIcon className="mx-auto h-12 w-12 text-secondary-400" />
        <h3 className="mt-2 text-sm font-medium text-secondary-900">No preview data available</h3>
        <p className="mt-1 text-sm text-secondary-500">
          This may be due to restricted access or an empty table.
        </p>
      </div>
    );
  }

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <h3 className="text-lg font-medium text-secondary-900">Data Preview</h3>

        <div className="flex items-center space-x-4">
          <div className="flex items-center">
            <label htmlFor="max-rows" className="mr-2 text-sm text-secondary-700">
              Rows:
            </label>
            <select
              id="max-rows"
              name="max-rows"
              className="block w-20 pl-3 pr-10 py-2 text-base border-secondary-300 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm rounded-md"
              value={maxRows}
              onChange={handleMaxRowsChange}
            >
              <option value="10">10</option>
              <option value="25">25</option>
              <option value="50">50</option>
              <option value="100">100</option>
            </select>
          </div>

          {restrictedColumns.length > 0 && (
            <div className="flex items-center text-xs text-warning-600">
              <InformationCircleIcon className="h-4 w-4 mr-1" />
              {restrictedColumns.length} column(s) hidden due to access restrictions
            </div>
          )}
        </div>
      </div>

      {/* Table with horizontal scrolling */}
      <div className="shadow border-b border-secondary-200 rounded-lg overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-secondary-200">
            <thead className="bg-secondary-50">
              <tr>
                {columns.map((column) => (
                  <th
                    key={column}
                    scope="col"
                    className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider cursor-pointer hover:bg-secondary-100"
                    onClick={() => handleSort(column)}
                  >
                    <div className="flex items-center">
                      <span>{column}</span>
                      {sortColumn === column && (
                        <ArrowsUpDownIcon
                          className={`ml-1 h-4 w-4 ${
                            sortDirection === 'asc' ? 'text-primary-500' : 'text-primary-700'
                          }`}
                        />
                      )}
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-secondary-200">
              {sortedData.map((row, rowIndex) => (
                <tr key={rowIndex} className={rowIndex % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
                  {columns.map((column) => (
                    <td key={`${rowIndex}-${column}`} className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                      {formatCellValue(row[column])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="mt-4 flex justify-between items-center text-sm text-secondary-500">
        <div>
          Showing {previewData.length} row{previewData.length !== 1 ? 's' : ''}
        </div>

        {loading && (
          <div className="flex items-center">
            <LoadingSpinner size="sm" className="mr-2" />
            Loading...
          </div>
        )}
      </div>
    </div>
  );
};

// Helper function to format cell values for display
const formatCellValue = (value) => {
  if (value === null || value === undefined) {
    return <span className="text-secondary-400 italic">null</span>;
  }

  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch (e) {
      return String(value);
    }
  }

  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }

  return String(value);
};

export default TablePreview;