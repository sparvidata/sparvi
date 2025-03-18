import React from 'react';
import { Link } from 'react-router-dom';
import {
  ArrowLeftIcon,
  ArrowPathIcon,
  ClipboardDocumentCheckIcon,
  DocumentDuplicateIcon,
  ServerIcon
} from '@heroicons/react/24/outline';
import { useUI } from '../../../contexts/UIContext';

const TableHeader = ({
  tableName,
  connectionName,
  onRefreshProfile,
  isRefreshing
}) => {
  const { showNotification } = useUI();

  const handleCopyTableName = () => {
    navigator.clipboard.writeText(tableName);
    showNotification('Table name copied to clipboard', 'success');
  };

  return (
    <div>
      <div className="flex justify-between items-center">
        <div className="flex items-center">
          <Link
            to="/explorer"
            className="inline-flex items-center p-2 rounded-md text-secondary-500 hover:text-secondary-700 hover:bg-secondary-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          >
            <ArrowLeftIcon className="h-5 w-5" aria-hidden="true" />
            <span className="sr-only">Back to data explorer</span>
          </Link>

          <h1 className="ml-2 text-2xl font-semibold text-secondary-900 flex items-center">
            <span>{tableName}</span>
            <button
              type="button"
              onClick={handleCopyTableName}
              className="ml-2 text-secondary-400 hover:text-secondary-600 focus:outline-none"
              title="Copy table name"
            >
              <DocumentDuplicateIcon className="h-5 w-5" aria-hidden="true" />
            </button>
          </h1>
        </div>

        <div className="flex items-center space-x-2">
          <Link
            to={`/validations?table=${tableName}`}
            className="inline-flex items-center px-3 py-1.5 border border-secondary-300 shadow-sm text-sm font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          >
            <ClipboardDocumentCheckIcon className="-ml-1 mr-2 h-5 w-5 text-secondary-500" aria-hidden="true" />
            Validations
          </Link>

          <button
            type="button"
            onClick={onRefreshProfile}
            disabled={isRefreshing}
            className="inline-flex items-center px-3 py-1.5 border border-secondary-300 shadow-sm text-sm font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
          >
            <ArrowPathIcon
              className={`-ml-1 mr-2 h-5 w-5 text-secondary-500 ${isRefreshing ? 'animate-spin' : ''}`}
              aria-hidden="true"
            />
            {isRefreshing ? 'Refreshing...' : 'Refresh Profile'}
          </button>
        </div>
      </div>

      <div className="mt-2 flex items-center text-sm text-secondary-500">
        <ServerIcon className="flex-shrink-0 mr-1.5 h-5 w-5 text-secondary-400" aria-hidden="true" />
        <span>Connection: {connectionName}</span>
      </div>
    </div>
  );
};

export default TableHeader;