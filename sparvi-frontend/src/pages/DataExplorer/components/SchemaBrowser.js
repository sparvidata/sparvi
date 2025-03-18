import React from 'react';
import { ChevronRightIcon, ChevronDownIcon, TableCellsIcon, FolderIcon } from '@heroicons/react/24/outline';

const SchemaBrowser = ({
  tables,
  activeConnection,
  expandedNodes,
  toggleNodeExpanded,
  onTableSelect
}) => {
  // Group tables by schema if schema information is available
  const tablesBySchema = React.useMemo(() => {
    // For now, we just group all tables under the connection's schema (if available)
    // This could be enhanced later to properly group tables based on their schema
    const schema = activeConnection?.connection_details?.schema || 'public';

    // Create a schema node
    return {
      [schema]: tables
    };
  }, [tables, activeConnection]);

  const schemaNames = Object.keys(tablesBySchema);

  // If no tables, show empty state
  if (tables.length === 0) {
    return (
      <div className="text-center py-4">
        <p className="text-sm text-secondary-500">No tables found</p>
        <button
          className="mt-2 text-sm text-primary-600 hover:text-primary-500"
          onClick={() => {
            // This could trigger a metadata refresh
          }}
        >
          Refresh metadata
        </button>
      </div>
    );
  }

  return (
    <div className="overflow-auto max-h-96">
      <ul className="divide-y divide-secondary-200">
        {/* Connection node */}
        <li className="py-2">
          <div
            className="flex items-center cursor-pointer"
            onClick={() => toggleNodeExpanded('connection')}
          >
            {expandedNodes['connection'] ? (
              <ChevronDownIcon className="h-4 w-4 text-secondary-500" />
            ) : (
              <ChevronRightIcon className="h-4 w-4 text-secondary-500" />
            )}
            <span className="ml-2 text-sm font-medium text-secondary-900">
              {activeConnection.name}
            </span>
          </div>

          {/* Schema nodes */}
          {expandedNodes['connection'] && (
            <ul className="ml-4 mt-1 space-y-1">
              {schemaNames.map(schema => (
                <li key={schema}>
                  <div
                    className="flex items-center cursor-pointer py-1"
                    onClick={() => toggleNodeExpanded(`schema-${schema}`)}
                  >
                    {expandedNodes[`schema-${schema}`] ? (
                      <ChevronDownIcon className="h-4 w-4 text-secondary-500" />
                    ) : (
                      <ChevronRightIcon className="h-4 w-4 text-secondary-500" />
                    )}
                    <FolderIcon className="ml-1 h-4 w-4 text-secondary-400" />
                    <span className="ml-1 text-sm text-secondary-700">
                      {schema}
                    </span>
                  </div>

                  {/* Table nodes */}
                  {expandedNodes[`schema-${schema}`] && (
                    <ul className="ml-5 mt-1 space-y-1">
                      {tablesBySchema[schema].map(table => (
                        <li
                          key={table}
                          className="flex items-center cursor-pointer py-1 hover:bg-secondary-50 rounded pl-2"
                          onClick={() => onTableSelect(table)}
                        >
                          <TableCellsIcon className="h-4 w-4 text-secondary-400" />
                          <span className="ml-1 text-sm text-secondary-700 truncate">
                            {table}
                          </span>
                        </li>
                      ))}
                    </ul>
                  )}
                </li>
              ))}
            </ul>
          )}
        </li>
      </ul>
    </div>
  );
};

export default SchemaBrowser;