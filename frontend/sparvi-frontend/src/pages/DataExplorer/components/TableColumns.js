import React, { useState } from 'react';
import {
  ChevronDownIcon,
  ChevronRightIcon,
  ChartBarIcon,
  CheckCircleIcon,
  XCircleIcon,
  CheckIcon,
  MagnifyingGlassIcon,
  ChartPieIcon,
  ArrowsUpDownIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { formatNumber, formatPercentage } from '../../../utils/formatting';

const TableColumns = ({ columns = [], isLoading, profile }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [expandedColumns, setExpandedColumns] = useState({});
  const [sortField, setSortField] = useState('name');
  const [sortDirection, setSortDirection] = useState('asc');

  // Handle search
  const handleSearch = (e) => {
    setSearchQuery(e.target.value);
  };

  // Toggle column expanded state
  const toggleColumnExpanded = (columnName) => {
    setExpandedColumns(prev => ({
      ...prev,
      [columnName]: !prev[columnName]
    }));
  };

  // Sort columns based on current sort field and direction
  const sortColumns = (columnsToSort) => {
    if (!columnsToSort) return [];

    return [...columnsToSort].sort((a, b) => {
      let aValue, bValue;

      // Handle special sort fields
      switch(sortField) {
        case 'name':
          aValue = a.name || '';
          bValue = b.name || '';
          break;
        case 'type':
          aValue = a.type || '';
          bValue = b.type || '';
          break;
        case 'null_percent':
          // Get null percentages from profile if available
          const nullFractions = profile?.null_fractions || {};
          aValue = nullFractions[a.name] || 0;
          bValue = nullFractions[b.name] || 0;
          break;
        case 'distinct_count':
          // Get distinct counts from profile if available
          const distinctCounts = profile?.distinct_counts || {};
          aValue = distinctCounts[a.name] || 0;
          bValue = distinctCounts[b.name] || 0;
          break;
        default:
          aValue = a[sortField] || '';
          bValue = b[sortField] || '';
      }

      // Compare values (handling string vs number comparisons)
      let comparison;
      if (typeof aValue === 'string' && typeof bValue === 'string') {
        comparison = aValue.localeCompare(bValue);
      } else {
        comparison = aValue < bValue ? -1 : aValue > bValue ? 1 : 0;
      }

      // Apply sort direction
      return sortDirection === 'asc' ? comparison : -comparison;
    });
  };

  // Handle sort click
  const handleSortClick = (field) => {
    if (sortField === field) {
      // Toggle direction if clicking the same field
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      // Set new field and default to ascending
      setSortField(field);
      setSortDirection('asc');
    }
  };

  // Filter columns by search query
  const filteredColumns = columns?.filter(column =>
    column.name.toLowerCase().includes(searchQuery.toLowerCase())
  ) || [];

  // Sort filtered columns
  const sortedColumns = sortColumns(filteredColumns);

  // Get profile data for a column
  const getColumnProfileData = (columnName) => {
    if (!profile) return null;

    const nullFraction = profile.null_fractions?.[columnName] || 0;
    const distinctCount = profile.distinct_counts?.[columnName] || 0;
    const summaryStats = profile.summary_statistics?.[columnName] || {};
    const distributionData = profile.distribution_data?.[columnName] || {};
    const topValues = profile.top_values?.[columnName] || [];

    return {
      nullFraction,
      distinctCount,
      summaryStats,
      distributionData,
      topValues
    };
  };

  // If loading with no columns data, show loading state
  if (isLoading && !columns?.length) {
    return (
      <div className="flex justify-center py-10">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  // If no columns, show empty state
  if (!columns?.length) {
    return (
      <div className="text-center py-10">
        <p className="text-sm text-secondary-500">No column data available</p>
      </div>
    );
  }

  return (
    <div>
      {/* Search and filter */}
      <div className="flex justify-end mb-4">
        <div className="relative max-w-xs w-full">
          <input
            type="text"
            className="block w-full pl-10 pr-3 py-2 border border-secondary-300 rounded-md leading-5 bg-white placeholder-secondary-500 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
            placeholder="Search columns..."
            value={searchQuery}
            onChange={handleSearch}
          />
          <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
            <MagnifyingGlassIcon className="h-5 w-5 text-secondary-400" aria-hidden="true" />
          </div>
        </div>
      </div>

      {/* Columns list */}
      <div className="shadow overflow-hidden border-b border-secondary-200 sm:rounded-lg">
        <table className="min-w-full divide-y divide-secondary-200">
          <thead className="bg-secondary-50">
            <tr>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button
                  className="group inline-flex items-center"
                  onClick={() => handleSortClick('name')}
                >
                  Column
                  <span className={`ml-2 flex-none rounded ${sortField === 'name' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button
                  className="group inline-flex items-center"
                  onClick={() => handleSortClick('type')}
                >
                  Type
                  <span className={`ml-2 flex-none rounded ${sortField === 'type' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button
                  className="group inline-flex items-center"
                  onClick={() => handleSortClick('nullable')}
                >
                  Nullable
                  <span className={`ml-2 flex-none rounded ${sortField === 'nullable' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button
                  className="group inline-flex items-center"
                  onClick={() => handleSortClick('null_percent')}
                >
                  Non-Null %
                  <span className={`ml-2 flex-none rounded ${sortField === 'null_percent' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
              <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                <button
                  className="group inline-flex items-center"
                  onClick={() => handleSortClick('distinct_count')}
                >
                  Distinct Values
                  <span className={`ml-2 flex-none rounded ${sortField === 'distinct_count' ? 'bg-secondary-200 text-secondary-900' : 'text-secondary-400 group-hover:bg-secondary-200'}`}>
                    <ArrowsUpDownIcon className="h-5 w-5" aria-hidden="true" />
                  </span>
                </button>
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-secondary-200">
            {sortedColumns.map((column, index) => {
              const profileData = getColumnProfileData(column.name);
              const isExpanded = expandedColumns[column.name] || false;

              return (
                <React.Fragment key={column.name || index}>
                  {/* Main row */}
                  <tr
                    className={`${index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'} hover:bg-secondary-100 cursor-pointer`}
                    onClick={() => toggleColumnExpanded(column.name)}
                  >
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-secondary-900 flex items-center">
                      {isExpanded ? (
                        <ChevronDownIcon className="h-5 w-5 text-secondary-500 mr-2" />
                      ) : (
                        <ChevronRightIcon className="h-5 w-5 text-secondary-500 mr-2" />
                      )}
                      {column.name}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                      <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-secondary-100 text-secondary-800">
                        {column.type || 'unknown'}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                      {column.nullable ? (
                        <CheckIcon className="h-5 w-5 text-accent-500" aria-hidden="true" />
                      ) : (
                        <XCircleIcon className="h-5 w-5 text-warning-500" aria-hidden="true" />
                      )}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                      {profileData ? formatPercentage(100 - (profileData.nullFraction * 100)) : '-'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                      {profileData ? formatNumber(profileData.distinctCount) : '-'}
                    </td>
                  </tr>

                  {/* Expanded details */}
                  {isExpanded && (
                    <tr className="bg-secondary-50">
                      <td colSpan={5} className="px-6 py-4">
                        <div className="bg-white p-4 rounded-md shadow-sm">
                          <h4 className="text-sm font-medium text-secondary-900 mb-3">Column Details: {column.name}</h4>

                          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
                            {/* Basic info */}
                            <div className="bg-secondary-50 p-3 rounded-md">
                              <h5 className="text-xs font-medium text-secondary-500 uppercase mb-2">Basic Information</h5>
                              <div className="space-y-1">
                                <div className="flex justify-between">
                                  <span className="text-sm text-secondary-500">Data Type:</span>
                                  <span className="text-sm font-medium text-secondary-900">{column.type || 'unknown'}</span>
                                </div>
                                <div className="flex justify-between">
                                  <span className="text-sm text-secondary-500">Nullable:</span>
                                  <span className="text-sm font-medium text-secondary-900">{column.nullable ? 'Yes' : 'No'}</span>
                                </div>
                                {profileData && (
                                  <>
                                    <div className="flex justify-between">
                                      <span className="text-sm text-secondary-500">Non-Null Values:</span>
                                      <span className="text-sm font-medium text-secondary-900">
                                        {formatPercentage(100 - (profileData.nullFraction * 100))}
                                      </span>
                                    </div>
                                    <div className="flex justify-between">
                                      <span className="text-sm text-secondary-500">Distinct Values:</span>
                                      <span className="text-sm font-medium text-secondary-900">
                                        {formatNumber(profileData.distinctCount)}
                                      </span>
                                    </div>
                                  </>
                                )}
                              </div>
                            </div>

                            {/* Statistics */}
                            {profileData && profileData.summaryStats && Object.keys(profileData.summaryStats).length > 0 && (
                              <div className="bg-secondary-50 p-3 rounded-md">
                                <h5 className="text-xs font-medium text-secondary-500 uppercase mb-2">Statistics</h5>
                                <div className="space-y-1">
                                  {profileData.summaryStats.min !== undefined && (
                                    <div className="flex justify-between">
                                      <span className="text-sm text-secondary-500">Min:</span>
                                      <span className="text-sm font-medium text-secondary-900">
                                        {profileData.summaryStats.min}
                                      </span>
                                    </div>
                                  )}
                                  {profileData.summaryStats.max !== undefined && (
                                    <div className="flex justify-between">
                                      <span className="text-sm text-secondary-500">Max:</span>
                                      <span className="text-sm font-medium text-secondary-900">
                                        {profileData.summaryStats.max}
                                      </span>
                                    </div>
                                  )}
                                  {profileData.summaryStats.mean !== undefined && (
                                    <div className="flex justify-between">
                                      <span className="text-sm text-secondary-500">Mean:</span>
                                      <span className="text-sm font-medium text-secondary-900">
                                        {profileData.summaryStats.mean.toFixed(2)}
                                      </span>
                                    </div>
                                  )}
                                  {profileData.summaryStats.median !== undefined && (
                                    <div className="flex justify-between">
                                      <span className="text-sm text-secondary-500">Median:</span>
                                      <span className="text-sm font-medium text-secondary-900">
                                        {profileData.summaryStats.median}
                                      </span>
                                    </div>
                                  )}
                                </div>
                              </div>
                            )}

                            {/* Top values */}
                            {profileData && profileData.topValues && profileData.topValues.length > 0 && (
                              <div className="bg-secondary-50 p-3 rounded-md">
                                <h5 className="text-xs font-medium text-secondary-500 uppercase mb-2">Top Values</h5>
                                <div className="space-y-1">
                                  {profileData.topValues.slice(0, 5).map((valueObj, i) => (
                                    <div key={i} className="flex justify-between">
                                      <span className="text-sm text-secondary-500 truncate max-w-[150px]">
                                        {valueObj.value}:
                                      </span>
                                      <span className="text-sm font-medium text-secondary-900">
                                        {formatNumber(valueObj.count)} ({formatPercentage(valueObj.percentage)})
                                      </span>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}
                          </div>

                          {/* Actions */}
                          <div className="mt-4 flex space-x-2 justify-end">
                            <button
                              className="inline-flex items-center px-2.5 py-1.5 border border-transparent text-xs font-medium rounded text-accent-700 bg-accent-100 hover:bg-accent-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-accent-500"
                            >
                              <ChartBarIcon className="h-4 w-4 mr-1" />
                              View Distribution
                            </button>
                            <button
                              className="inline-flex items-center px-2.5 py-1.5 border border-transparent text-xs font-medium rounded text-primary-700 bg-primary-100 hover:bg-primary-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                            >
                              <CheckCircleIcon className="h-4 w-4 mr-1" />
                              Add Validation
                            </button>
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default TableColumns;