import React from 'react';
import { useUI } from '../../../contexts/UIContext';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import ColumnsIcon from '../../../components/icons/ColumnsIcon';
import {
  TableCellsIcon,
  ChartBarIcon,
  ArrowPathIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline';

const RefreshControls = ({
  connectionId,
  onRefresh,
  isRefreshing,
  metadataStatus,
  onMetadataTypeSelect,
  selectedMetadataType,
  onViewSchemaChanges
}) => {
  const { showNotification } = useUI();

  // Refresh a specific type of metadata
  const handleRefresh = (type) => {
    if (!connectionId) {
      showNotification('No connection selected', 'error');
      return;
    }

    onRefresh(type, {
      onSuccess: () => {
        showNotification(`${type} metadata refresh initiated`, 'success');
        onMetadataTypeSelect(type);
      },
      onError: (error) => {
        console.error(`Error refreshing ${type} metadata:`, error);
        showNotification(`Failed to refresh ${type} metadata`, 'error');
      }
    });
  };

  // Refresh all metadata (full refresh)
  const handleRefreshAll = () => {
    if (!connectionId) {
      showNotification('No connection selected', 'error');
      return;
    }

    onRefresh('full', {
      onSuccess: () => {
        showNotification('Full metadata refresh initiated', 'success');
      },
      onError: (error) => {
        console.error('Error refreshing metadata:', error);
        showNotification('Failed to refresh metadata', 'error');
      }
    });
  };

  // Get status indicator class based on freshness
  const getStatusIndicatorClass = (metadataType) => {
    const status = metadataStatus?.[metadataType]?.freshness?.status;

    if (!status) return 'text-secondary-400';

    switch(status) {
      case 'fresh': return 'text-accent-400';
      case 'recent': return 'text-primary-400';
      case 'stale': return 'text-warning-400';
      case 'error': return 'text-danger-400';
      default: return 'text-secondary-400';
    }
  };

  return (
    <div className="space-y-4">
      <div>
        <button
          onClick={handleRefreshAll}
          disabled={isRefreshing}
          className="w-full flex items-center justify-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
        >
          {isRefreshing ? (
            <>
              <LoadingSpinner size="sm" className="mr-2" />
              Refreshing...
            </>
          ) : (
            <>
              <ArrowPathIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
              Refresh All Metadata
            </>
          )}
        </button>
      </div>

      <div className="border-t border-secondary-200 pt-4">
        <h4 className="text-xs font-medium text-secondary-500 uppercase tracking-wider mb-3">
          Metadata Types
        </h4>
        <nav className="space-y-2">
          <button
            onClick={() => {
              onMetadataTypeSelect('tables');
            }}
            className={`w-full flex items-center px-3 py-2 text-sm rounded-md ${
              selectedMetadataType === 'tables' 
                ? 'bg-primary-50 text-primary-700 font-medium' 
                : 'text-secondary-600 hover:bg-secondary-50 hover:text-secondary-900'
            }`}
          >
            <TableCellsIcon className={`mr-3 flex-shrink-0 h-5 w-5 ${getStatusIndicatorClass('tables')}`} aria-hidden="true" />
            <span className="truncate">Tables</span>
            <div className="ml-auto">
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  handleRefresh('tables');
                }}
                disabled={isRefreshing}
                className="p-1 rounded-full text-secondary-400 hover:text-primary-500 focus:outline-none"
              >
                <ArrowPathIcon className="h-4 w-4" aria-hidden="true" />
              </button>
            </div>
          </button>

          <button
            onClick={() => {
              onMetadataTypeSelect('columns');
            }}
            className={`w-full flex items-center px-3 py-2 text-sm rounded-md ${
              selectedMetadataType === 'columns' 
                ? 'bg-primary-50 text-primary-700 font-medium' 
                : 'text-secondary-600 hover:bg-secondary-50 hover:text-secondary-900'
            }`}
          >
            <ColumnsIcon className={`mr-3 flex-shrink-0 h-5 w-5 ${getStatusIndicatorClass('columns')}`} aria-hidden="true" />
            <span className="truncate">Columns</span>
            <div className="ml-auto">
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  handleRefresh('columns');
                }}
                disabled={isRefreshing}
                className="p-1 rounded-full text-secondary-400 hover:text-primary-500 focus:outline-none"
              >
                <ArrowPathIcon className="h-4 w-4" aria-hidden="true" />
              </button>
            </div>
          </button>

          <button
            onClick={() => {
              onMetadataTypeSelect('statistics');
            }}
            className={`w-full flex items-center px-3 py-2 text-sm rounded-md ${
              selectedMetadataType === 'statistics' 
                ? 'bg-primary-50 text-primary-700 font-medium' 
                : 'text-secondary-600 hover:bg-secondary-50 hover:text-secondary-900'
            }`}
          >
            <ChartBarIcon className={`mr-3 flex-shrink-0 h-5 w-5 ${getStatusIndicatorClass('statistics')}`} aria-hidden="true" />
            <span className="truncate">Statistics</span>
            <div className="ml-auto">
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  handleRefresh('statistics');
                }}
                disabled={isRefreshing}
                className="p-1 rounded-full text-secondary-400 hover:text-primary-500 focus:outline-none"
              >
                <ArrowPathIcon className="h-4 w-4" aria-hidden="true" />
              </button>
            </div>
          </button>
        </nav>
      </div>

      {/* Schema change detection */}
      {metadataStatus?.changes_detected > 0 && (
        <div className="mt-4 border-t border-secondary-200 pt-4">
          <div className="rounded-md bg-warning-50 p-4">
            <div className="flex">
              <div className="flex-shrink-0">
                <ExclamationTriangleIcon className="h-5 w-5 text-warning-400" aria-hidden="true" />
              </div>
              <div className="ml-3">
                <h3 className="text-sm font-medium text-warning-800">Schema Changes Detected</h3>
                <div className="mt-2 text-sm text-warning-700">
                  <p>
                    {metadataStatus.changes_detected} schema {metadataStatus.changes_detected === 1 ? 'change has' : 'changes have'} been detected.
                  </p>

                  {/* Add more actionable info */}
                  <div className="mt-3">
                    <button
                      onClick={() => {
                        // Use the callback prop to switch tabs
                        if (onViewSchemaChanges) {
                          onViewSchemaChanges();
                        }
                      }}
                      className="text-sm font-medium text-warning-800 hover:text-warning-900"
                    >
                      View and acknowledge changes <span aria-hidden="true">→</span>
                    </button>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default RefreshControls;