// src/pages/DataExplorer/components/EnhancedSchemaBrowser.js
import React, { useState } from 'react';
import {
  MagnifyingGlassIcon,
  ChevronRightIcon,
  ChevronDownIcon,
  TableCellsIcon,
  FolderIcon
} from '@heroicons/react/24/outline';

const EnhancedSchemaBrowser = ({
  tables = [],
  activeConnection,
  expandedNodes,
  toggleNodeExpanded,
  onTableSelect,
  searchQuery = '',
  onSearch
}) => {
  // Local state for search input
  const [localSearchQuery, setLocalSearchQuery] = useState(searchQuery);

  // Handle search input change
  const handleSearchChange = (e) => {
    const query = e.target.value;
    setLocalSearchQuery(query);
    if (onSearch) {
      onSearch(query);
    }
  };

  // Get schema name
  const schema = activeConnection?.connection_details?.schema || 'public';

  // Filter tables by search
  const filteredTables = searchQuery
    ? tables.filter(table => table.toLowerCase().includes(searchQuery.toLowerCase()))
    : tables;

  return (
    <div>
      {/* Search input */}
      <div className="mb-4">
        <div className="relative">
          <input
            type="text"
            className="block w-full pl-10 pr-3 py-2 border border-secondary-300 rounded-md leading-5 bg-white placeholder-secondary-500 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
            placeholder="Search tables..."
            value={localSearchQuery}
            onChange={handleSearchChange}
          />
          <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
            <MagnifyingGlassIcon className="h-5 w-5 text-secondary-400" aria-hidden="true" />
          </div>
        </div>
      </div>

      {/* Simple table listing */}
      <div className="overflow-auto max-h-[calc(100vh-300px)]">
        {searchQuery ? (
          <div>
            <h4 className="text-sm font-medium text-secondary-500 mb-2">
              {filteredTables.length} {filteredTables.length === 1 ? 'table' : 'tables'} found
            </h4>
            <ul className="space-y-1">
              {filteredTables.map(table => (
                <li
                  key={table}
                  className="flex items-center px-3 py-2 cursor-pointer hover:bg-secondary-50 rounded-md"
                  onClick={() => onTableSelect(table)}
                >
                  <TableCellsIcon className="h-5 w-5 text-secondary-400 mr-2" />
                  <span className="text-primary-600 font-medium">{table}</span>
                </li>
              ))}
            </ul>
          </div>
        ) : (
          <div>
            {/* Simple connection header */}
            <div
              className="flex items-center p-2 cursor-pointer hover:bg-secondary-50 rounded-md"
              onClick={() => toggleNodeExpanded('connection')}
            >
              {expandedNodes['connection'] ? (
                <ChevronDownIcon className="h-5 w-5 text-secondary-500" />
              ) : (
                <ChevronRightIcon className="h-5 w-5 text-secondary-500" />
              )}
              <span className="ml-2 font-medium text-secondary-900">
                {activeConnection.name}
              </span>
            </div>

            {/* Tables listing */}
            {expandedNodes['connection'] && (
              <div className="ml-6 mt-2">
                <div
                  className="flex items-center p-2 cursor-pointer hover:bg-secondary-50 rounded-md"
                  onClick={() => toggleNodeExpanded(`schema-${schema}`)}
                >
                  {expandedNodes[`schema-${schema}`] ? (
                    <ChevronDownIcon className="h-5 w-5 text-secondary-500" />
                  ) : (
                    <ChevronRightIcon className="h-5 w-5 text-secondary-500" />
                  )}
                  <FolderIcon className="h-5 w-5 text-secondary-400 ml-1" />
                  <span className="ml-2 font-medium text-secondary-700">{schema}</span>
                  <span className="ml-2 text-xs font-medium text-secondary-500 bg-secondary-100 px-2 py-0.5 rounded-full">
                    {tables.length}
                  </span>
                </div>

                {expandedNodes[`schema-${schema}`] && (
                  <ul className="ml-8 mt-1 space-y-1">
                    {tables.map(table => (
                      <li
                        key={table}
                        className="flex items-center px-3 py-2 cursor-pointer hover:bg-secondary-50 rounded-md"
                        onClick={() => onTableSelect(table)}
                      >
                        <TableCellsIcon className="h-5 w-5 text-secondary-400 mr-2" />
                        <span className="text-primary-600 font-medium">{table}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default EnhancedSchemaBrowser;