import React from 'react';
import { useUI } from '../../../contexts/UIContext';
import { useAutomationStatus } from '../../../hooks/useAutomationStatus';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import ColumnsIcon from '../../../components/icons/ColumnsIcon';
import {
  TableCellsIcon,
  ChartBarIcon,
  ArrowPathIcon,
  ExclamationTriangleIcon,
  ClockIcon,
  PlayIcon,
  PauseIcon,
  Cog6ToothIcon
} from '@heroicons/react/24/outline';
import { Link } from 'react-router-dom';

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
  const { status: automationStatus, toggleAutomation, triggerManualRun, loading: statusLoading } = useAutomationStatus(connectionId);

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

  // Toggle automation for metadata refresh
  const handleToggleMetadataAutomation = async () => {
    const success = await toggleAutomation('metadata_refresh', !automationStatus.connection_config?.metadata_refresh?.enabled);
    if (success) {
      showNotification(
        `Metadata automation ${!automationStatus.connection_config?.metadata_refresh?.enabled ? 'enabled' : 'disabled'}`,
        'success'
      );
    } else {
      showNotification('Failed to toggle metadata automation', 'error');
    }
  };

  // Toggle automation for schema change detection
  const handleToggleSchemaAutomation = async () => {
    const success = await toggleAutomation('schema_change_detection', !automationStatus.connection_config?.schema_change_detection?.enabled);
    if (success) {
      showNotification(
        `Schema change detection ${!automationStatus.connection_config?.schema_change_detection?.enabled ? 'enabled' : 'disabled'}`,
        'success'
      );
    } else {
      showNotification('Failed to toggle schema automation', 'error');
    }
  };

  // Trigger automated runs manually
  const handleTriggerAutomatedRun = async (type) => {
    const success = await triggerManualRun(type);
    if (success) {
      showNotification(`${type.replace('_', ' ')} automation triggered`, 'success');
    } else {
      showNotification(`Failed to trigger ${type.replace('_', ' ')} automation`, 'error');
    }
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

  // Get automation status for display
  const getAutomationStatus = (automationType) => {
    if (statusLoading) return { enabled: null, interval: null, lastRun: null, loading: true };

    const config = automationStatus.connection_config?.[automationType];
    const lastRun = automationStatus.last_runs?.[automationType];

    return {
      enabled: config?.enabled || false,
      interval: config?.interval_hours,
      lastRun: lastRun?.completed_at,
      status: lastRun?.status,
      loading: false
    };
  };

  const metadataAutomation = getAutomationStatus('metadata_refresh');
  const schemaAutomation = getAutomationStatus('schema_change_detection');

  return (
    <div className="space-y-4">
      {/* Automation Status Panel */}
      <div className="bg-white border border-secondary-200 rounded-lg p-4">
          <div className="flex items-center justify-between mb-3">
            <h4 className="text-sm font-medium text-secondary-900 flex items-center">
              <ClockIcon className="h-4 w-4 mr-2" />
              Automation Status
            </h4>
            <Link
              to="/settings/automation"
              className="text-xs text-primary-600 hover:text-primary-700 flex items-center"
            >
              <Cog6ToothIcon className="h-3 w-3 mr-1" />
              Configure
            </Link>
          </div>

          <div className="space-y-3">
            {/* Metadata Automation */}
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <span className="text-xs text-secondary-700">Metadata Refresh</span>
                {metadataAutomation.enabled && metadataAutomation.interval && !metadataAutomation.loading && (
                  <span className="ml-2 text-xs text-secondary-500">
                    (Every {metadataAutomation.interval}h)
                  </span>
                )}
                {metadataAutomation.lastRun && !metadataAutomation.loading && (
                  <span className="ml-2 text-xs text-secondary-400">
                    Last: {new Date(metadataAutomation.lastRun).toLocaleDateString()}
                  </span>
                )}
              </div>
              <div className="flex items-center space-x-2">
                {metadataAutomation.enabled && !metadataAutomation.loading && (
                  <button
                    onClick={() => handleTriggerAutomatedRun('metadata_refresh')}
                    className="text-xs text-primary-600 hover:text-primary-700"
                    title="Trigger automated refresh now"
                  >
                    <PlayIcon className="h-3 w-3" />
                  </button>
                )}

                {/* Enhanced Toggle Button with Loading State */}
                {metadataAutomation.loading ? (
                  // Loading skeleton
                  <div className="flex items-center px-2 py-1 rounded text-xs font-medium bg-secondary-100">
                    <div className="flex items-center">
                      <div className="animate-spin h-3 w-3 border border-secondary-400 border-t-transparent rounded-full mr-1"></div>
                      <span className="text-secondary-500">Loading...</span>
                    </div>
                  </div>
                ) : (
                  // Actual toggle
                  <button
                    onClick={handleToggleMetadataAutomation}
                    disabled={statusLoading}
                    className={`flex items-center px-2 py-1 rounded text-xs font-medium transition-all duration-200 ${
                      metadataAutomation.enabled
                        ? 'bg-accent-100 text-accent-800 hover:bg-accent-200'
                        : 'bg-secondary-100 text-secondary-600 hover:bg-secondary-200'
                    }`}
                  >
                    {metadataAutomation.enabled ? (
                      <>
                        <PlayIcon className="h-3 w-3 mr-1" />
                        On
                      </>
                    ) : (
                      <>
                        <PauseIcon className="h-3 w-3 mr-1" />
                        Off
                      </>
                    )}
                  </button>
                )}
              </div>
            </div>

            {/* Schema Change Detection */}
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <span className="text-xs text-secondary-700">Schema Detection</span>
                {schemaAutomation.enabled && schemaAutomation.interval && !schemaAutomation.loading && (
                  <span className="ml-2 text-xs text-secondary-500">
                    (Every {schemaAutomation.interval}h)
                  </span>
                )}
                {schemaAutomation.lastRun && !schemaAutomation.loading && (
                  <span className="ml-2 text-xs text-secondary-400">
                    Last: {new Date(schemaAutomation.lastRun).toLocaleDateString()}
                  </span>
                )}
              </div>
              <div className="flex items-center space-x-2">
                {schemaAutomation.enabled && !schemaAutomation.loading && (
                  <button
                    onClick={() => handleTriggerAutomatedRun('schema_change_detection')}
                    className="text-xs text-primary-600 hover:text-primary-700"
                    title="Trigger schema detection now"
                  >
                    <PlayIcon className="h-3 w-3" />
                  </button>
                )}

                {/* Enhanced Toggle Button with Loading State */}
                {schemaAutomation.loading ? (
                  // Loading skeleton
                  <div className="flex items-center px-2 py-1 rounded text-xs font-medium bg-secondary-100">
                    <div className="flex items-center">
                      <div className="animate-spin h-3 w-3 border border-secondary-400 border-t-transparent rounded-full mr-1"></div>
                      <span className="text-secondary-500">Loading...</span>
                    </div>
                  </div>
                ) : (
                  // Actual toggle
                  <button
                    onClick={handleToggleSchemaAutomation}
                    disabled={statusLoading}
                    className={`flex items-center px-2 py-1 rounded text-xs font-medium transition-all duration-200 ${
                      schemaAutomation.enabled
                        ? 'bg-accent-100 text-accent-800 hover:bg-accent-200'
                        : 'bg-secondary-100 text-secondary-600 hover:bg-secondary-200'
                    }`}
                  >
                    {schemaAutomation.enabled ? (
                      <>
                        <PlayIcon className="h-3 w-3 mr-1" />
                        On
                      </>
                    ) : (
                      <>
                        <PauseIcon className="h-3 w-3 mr-1" />
                        Off
                      </>
                    )}
                  </button>
                )}
              </div>
            </div>

            {/* Global automation disabled warning */}
            {!automationStatus.global_enabled && (
              <div className="mt-2 p-2 bg-warning-50 border border-warning-200 rounded flex items-start">
                <ExclamationTriangleIcon className="h-4 w-4 text-warning-400 mr-2 mt-0.5" />
                <div>
                  <p className="text-xs text-warning-800">
                    Global automation is disabled.
                  </p>
                  <Link
                    to="/settings/automation"
                    className="text-xs text-warning-600 hover:text-warning-700 underline"
                  >
                    Enable in settings →
                  </Link>
                </div>
              </div>
            )}
          </div>
        </div>

      {/* Manual Refresh Controls */}
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
              Manual Refresh All
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

                  <div className="mt-3">
                    <button
                      onClick={() => {
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