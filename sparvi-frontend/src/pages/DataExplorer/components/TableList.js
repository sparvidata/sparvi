import React from 'react';
import { Link } from 'react-router-dom';
import {
  TableCellsIcon,
  ArrowTopRightOnSquareIcon,
  MagnifyingGlassCircleIcon
} from '@heroicons/react/24/outline';

const TableList = ({ tables, searchQuery, onTableSelect, connectionId }) => {
  // If no tables after filtering, show empty state
  if (tables.length === 0) {
    return (
      <div className="text-center py-16">
        <MagnifyingGlassCircleIcon className="mx-auto h-12 w-12 text-secondary-400" />
        {searchQuery ? (
          <>
            <h3 className="mt-2 text-sm font-medium text-secondary-900">No tables match your search</h3>
            <p className="mt-1 text-sm text-secondary-500">
              Try another search term or clear the search.
            </p>
          </>
        ) : (
          <>
            <h3 className="mt-2 text-sm font-medium text-secondary-900">No tables found</h3>
            <p className="mt-1 text-sm text-secondary-500">
              This connection does not have any tables or needs metadata refresh.
            </p>
          </>
        )}
      </div>
    );
  }

  return (
    <div className="overflow-hidden">
      <ul className="divide-y divide-secondary-200">
        {tables.map(table => (
          <li key={table} className="hover:bg-secondary-50">
            <Link
              to={`/explorer/${connectionId}/tables/${table}`}
              className="block px-4 py-4 sm:px-6"
              onClick={(e) => {
                e.preventDefault();
                onTableSelect(table);
              }}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center">
                  <div className="flex-shrink-0">
                    <TableCellsIcon className="h-6 w-6 text-secondary-400" />
                  </div>
                  <div className="ml-4">
                    <div className="text-sm font-medium text-secondary-900">{table}</div>
                    {/* We could add more metadata here when available */}
                  </div>
                </div>
                <div>
                  <ArrowTopRightOnSquareIcon className="h-5 w-5 text-secondary-400" />
                </div>
              </div>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
};

export default TableList;