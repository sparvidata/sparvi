import React, { useState, useEffect } from 'react';
import { CheckCircleIcon, ExclamationTriangleIcon, ArrowPathIcon } from '@heroicons/react/24/outline';
import { formatDate } from '../../../utils/formatting';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

const SchemaChangesPanel = ({
  connectionId,
  schemaChanges,
  isLoading,
  onAcknowledge,
  onRefresh,
  acknowledgedFilter,
  setAcknowledgedFilter
}) => {
  // Local filter states for client-side filtering
  const [localTypeFilter, setLocalTypeFilter] = useState('all');
  const [localAcknowledgedFilter, setLocalAcknowledgedFilter] = useState('all');

  // Track optimistically acknowledged tables
  const [optimisticAcknowledgements, setOptimisticAcknowledgements] = useState({});
  // Track which acknowledgment requests are in progress
  const [acknowledging, setAcknowledging] = useState({});

  // Initialize local filter to match server filter on component mount
  useEffect(() => {
    setLocalAcknowledgedFilter(acknowledgedFilter);
  }, [acknowledgedFilter]);

  // Handler for type filter change
  const handleTypeFilterChange = (e) => {
    setLocalTypeFilter(e.target.value);
  };

  // Handler for acknowledged filter change
  const handleAcknowledgedFilterChange = (e) => {
    const newValue = e.target.value;
    setLocalAcknowledgedFilter(newValue);
  };

  // Handler for acknowledging changes with optimistic update
  const handleAcknowledge = async (tableName) => {
    // Mark this table as being acknowledged
    setAcknowledging(prev => ({ ...prev, [tableName]: true }));

    // Optimistically mark the table's changes as acknowledged
    setOptimisticAcknowledgements(prev => ({ ...prev, [tableName]: true }));

    try {
      // Call the actual API
      await onAcknowledge(tableName);
      // On success, we don't need to do anything as the API will refresh the data
    } catch (error) {
      // On failure, revert the optimistic update
      console.error('Failed to acknowledge changes:', error);
      setOptimisticAcknowledgements(prev => ({ ...prev, [tableName]: false }));
    } finally {
      // Clear the acknowledging state
      setAcknowledging(prev => ({ ...prev, [tableName]: false }));
    }
  };

  // Apply client-side filters and optimistic updates to schema changes
  const filteredSchemaChanges = React.useMemo(() => {
    if (!schemaChanges) return [];

    // Create a modified list with optimistic acknowledgements applied
    const changesWithOptimisticUpdates = schemaChanges.map(change => {
      // If this change's table is being optimistically acknowledged, mark it as acknowledged
      if (optimisticAcknowledgements[change.table] && !change.acknowledged) {
        return { ...change, acknowledged: true, optimisticallyAcknowledged: true };
      }
      return change;
    });

    return changesWithOptimisticUpdates.filter(change => {
      // Filter by acknowledgement status
      if (localAcknowledgedFilter !== 'all') {
        const isAcknowledged = change.acknowledged ? 'true' : 'false';
        if (isAcknowledged !== localAcknowledgedFilter) {
          return false;
        }
      }

      // Filter by change type
      if (localTypeFilter === 'all') {
        return true;
      } else if (localTypeFilter === 'added') {
        return change.type.includes('added');
      } else if (localTypeFilter === 'removed') {
        return change.type.includes('removed');
      } else if (localTypeFilter === 'changed') {
        return change.type.includes('changed');
      } else {
        // For specific change types
        return change.type === localTypeFilter;
      }
    });
  }, [schemaChanges, localAcknowledgedFilter, localTypeFilter, optimisticAcknowledgements]);

  if (isLoading) {
    return (
      <div className="flex justify-center items-center py-8">
        <LoadingSpinner size="md" />
        <span className="ml-2 text-secondary-500">Loading schema changes...</span>
      </div>
    );
  }

  // Handle empty state
  if (!filteredSchemaChanges || filteredSchemaChanges.length === 0) {
    return (
      <div className="text-center py-8">
        <CheckCircleIcon className="mx-auto h-12 w-12 text-accent-400" />
        <h3 className="mt-2 text-sm font-medium text-secondary-900">No Schema Changes Detected</h3>
        <p className="mt-1 text-sm text-secondary-500">
          {localAcknowledgedFilter === 'false'
            ? 'There are no unacknowledged schema changes.'
            : localTypeFilter !== 'all'
              ? 'No changes match the selected filters.'
              : 'Your database schema is currently in sync with the metadata.'}
        </p>
        {onRefresh && (
          <button
            onClick={onRefresh}
            className="mt-4 inline-flex items-center px-3 py-2 border border-transparent
                     text-sm leading-4 font-medium rounded-md shadow-sm text-white
                     bg-primary-600 hover:bg-primary-700 focus:outline-none"
          >
            <ArrowPathIcon className="-ml-0.5 mr-2 h-4 w-4" aria-hidden="true" />
            Detect Changes
          </button>
        )}

        {(localAcknowledgedFilter !== 'all' || localTypeFilter !== 'all') && (
          <div className="mt-2">
            <button
              onClick={() => {
                setLocalAcknowledgedFilter('all');
                setLocalTypeFilter('all');
              }}
              className="text-sm text-primary-600 hover:text-primary-800"
            >
              Clear filters
            </button>
          </div>
        )}
      </div>
    );
  }

  // Group changes by table - using filtered changes now
  const changesByTable = {};
  filteredSchemaChanges.forEach(change => {
    const tableName = change.table || 'Unknown';
    if (!changesByTable[tableName]) {
      changesByTable[tableName] = [];
    }
    changesByTable[tableName].push(change);
  });

  // Get badge for change type
  const getChangeTypeBadge = (type) => {
    if (type.includes('added')) {
      // Special handling for different object types
      if (type.includes('foreign_key')) {
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
            FK Added
          </span>
        );
      } else if (type.includes('primary_key')) {
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800">
            PK Added
          </span>
        );
      } else if (type.includes('index')) {
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
            Index Added
          </span>
        );
      } else {
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-accent-100 text-accent-800">
            Added
          </span>
        );
      }
    } else if (type.includes('removed')) {
      // Special handling for different object types
      if (type.includes('foreign_key')) {
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
            FK Removed
          </span>
        );
      } else if (type.includes('primary_key')) {
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800">
            PK Removed
          </span>
        );
      } else if (type.includes('index')) {
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
            Index Removed
          </span>
        );
      } else {
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-danger-100 text-danger-800">
            Removed
          </span>
        );
      }
    } else if (type.includes('changed')) {
      // Special handling for different object types
      if (type.includes('foreign_key')) {
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
            FK Modified
          </span>
        );
      } else if (type.includes('primary_key')) {
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800">
            PK Modified
          </span>
        );
      } else if (type.includes('index')) {
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
            Index Modified
          </span>
        );
      } else {
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-warning-100 text-warning-800">
            Modified
          </span>
        );
      }
    } else {
      return (
        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-secondary-100 text-secondary-800">
          Changed
        </span>
      );
    }
  };

  // Get badge for acknowledged status
  const getAcknowledgedBadge = (acknowledged, optimisticallyAcknowledged) => {
    if (acknowledged) {
      return (
        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
          optimisticallyAcknowledged ? 'bg-accent-50 text-accent-600' : 'bg-secondary-100 text-secondary-600'
        }`}>
          <CheckCircleIcon className="mr-1 h-3 w-3" />
          {optimisticallyAcknowledged ? 'Acknowledging...' : 'Acknowledged'}
        </span>
      );
    }
    return null;
  };

  // Get descriptive text for change
  const getChangeDescription = (change) => {
    switch (change.type) {
      case 'table_added':
        return 'Table was added';
      case 'table_removed':
        return 'Table was removed';
      case 'column_added':
        return `Column was added (${change.details?.type || 'unknown type'})`;
      case 'column_removed':
        return `Column was removed (${change.details?.type || 'unknown type'})`;
      case 'column_type_changed':
        return `Column type changed from ${change.details?.previous_type || 'unknown'} to ${change.details?.new_type || 'unknown'}`;
      case 'column_nullability_changed':
        return `Column nullability changed from ${change.details?.previous_nullable ? 'nullable' : 'not nullable'} to ${change.details?.new_nullable ? 'nullable' : 'not nullable'}`;
      case 'primary_key_added':
        return 'Primary key was added';
      case 'primary_key_removed':
        return 'Primary key was removed';
      case 'primary_key_changed':
        return 'Primary key columns were modified';
      // Add foreign key change descriptions
      case 'foreign_key_added':
        return `Foreign key was added ${change.details?.referenced_table ? `(references ${change.details.referenced_table})` : ''}`;
      case 'foreign_key_removed':
        return `Foreign key was removed ${change.details?.referenced_table ? `(referenced ${change.details.referenced_table})` : ''}`;
      case 'foreign_key_changed':
        return 'Foreign key relationship was modified';
      // Add index change descriptions
      case 'index_added':
        return `Index was added ${change.details?.index_name ? `(${change.details.index_name})` : ''}`;
      case 'index_removed':
        return `Index was removed ${change.details?.index_name ? `(${change.details.index_name})` : ''}`;
      case 'index_changed':
        return `Index definition was changed ${change.details?.index_name ? `(${change.details.index_name})` : ''}`;
      default:
        return 'Schema changed';
    }
  };


  return (
    <div>
      <div className="mb-4 flex justify-between items-center">
        <h3 className="text-lg font-medium text-secondary-900">
          Schema Changes ({filteredSchemaChanges.length} of {schemaChanges.length})
        </h3>
        <div className="flex space-x-2">
          {/* Add acknowledged filter - now using local state */}
          <select
            id="acknowledged-filter"
            name="acknowledged-filter"
            className="block pl-3 pr-10 py-2 text-base border-secondary-300 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm rounded-md"
            value={localAcknowledgedFilter}
            onChange={handleAcknowledgedFilterChange}
          >
            <option value="all">All Changes</option>
            <option value="false">Unacknowledged Only</option>
            <option value="true">Acknowledged Only</option>
          </select>

          <select
              id="filter"
              name="filter"
              className="block pl-3 pr-10 py-2 text-base border-secondary-300 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm rounded-md"
              value={localTypeFilter}
              onChange={handleTypeFilterChange}
          >
            <option value="all">All Types</option>
            <option value="added">Added</option>
            <option value="removed">Removed</option>
            <option value="changed">Modified</option>
            <option value="table_added">Added Tables</option>
            <option value="table_removed">Removed Tables</option>
            <option value="column_added">Added Columns</option>
            <option value="column_removed">Removed Columns</option>
            <option value="column_type_changed">Type Changes</option>
            <option value="column_nullability_changed">Nullability Changes</option>
            <option value="primary_key_added">Added Primary Keys</option>
            <option value="primary_key_removed">Removed Primary Keys</option>
            <option value="primary_key_changed">Modified Primary Keys</option>
            {/* Add foreign key options */}
            <option value="foreign_key_added">Added Foreign Keys</option>
            <option value="foreign_key_removed">Removed Foreign Keys</option>
            <option value="foreign_key_changed">Modified Foreign Keys</option>
            {/* Add index options */}
            <option value="index_added">Added Indexes</option>
            <option value="index_removed">Removed Indexes</option>
            <option value="index_changed">Modified Indexes</option>
          </select>
          {onRefresh && (
              <button
                  onClick={() => {
                    // When explicitly refreshing, sync local and server filters
                    setAcknowledgedFilter(localAcknowledgedFilter);
                    onRefresh();
                  }}
                  className="inline-flex items-center px-3 py-2 border border-secondary-300
                       shadow-sm text-sm leading-4 font-medium rounded-md text-secondary-700
                       bg-white hover:bg-secondary-50 focus:outline-none"
              >
                <ArrowPathIcon className="-ml-0.5 mr-2 h-4 w-4" aria-hidden="true"/>
                Refresh
              </button>
          )}
        </div>
      </div>

      <div className="bg-white shadow overflow-hidden sm:rounded-md">
        <ul className="divide-y divide-secondary-200">
          {Object.entries(changesByTable).map(([tableName, tableChanges]) => (
              <li key={tableName} className="px-4 py-4">
                <div className="flex items-center justify-between">
                  <h4 className="text-sm font-medium text-secondary-900">{tableName}</h4>
                  <span className="text-xs text-secondary-500">
                  {tableChanges.length} {tableChanges.length === 1 ? 'change' : 'changes'}
                </span>
                </div>

                <ul className="mt-2 space-y-2">
                  {tableChanges.slice(0, 5).map((change, index) => (
                  <li key={`${change.type}-${change.table}-${change.column || ''}-${index}`}
                      className={`text-sm rounded-md p-2 ${change.acknowledged ? 'bg-secondary-50' : 'bg-yellow-50'}`}>
                    <div className="flex items-start">
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between">
                          <p className="font-medium text-secondary-800">
                            {change.column ? (
                              <span>Column: <span className="font-mono text-xs">{change.column}</span></span>
                            ) : (
                              <span>Table: <span className="font-mono text-xs">{change.table}</span></span>
                            )}
                          </p>
                          <div className="flex space-x-2">
                            {getChangeTypeBadge(change.type)}
                            {getAcknowledgedBadge(change.acknowledged, change.optimisticallyAcknowledged)}
                          </div>
                        </div>

                        <p className="text-secondary-500 mt-1">
                          {getChangeDescription(change)}
                        </p>

                        <div className="mt-1 flex flex-wrap text-xs text-secondary-400 space-x-2">
                          {change.timestamp && (
                            <div>
                              Detected: {formatDate(change.timestamp, true)}
                            </div>
                          )}
                          {change.acknowledged_at && !change.optimisticallyAcknowledged && (
                            <div>
                              Acknowledged: {formatDate(change.acknowledged_at, true)}
                            </div>
                          )}
                          {change.optimisticallyAcknowledged && (
                            <div>
                              Acknowledged: Just now
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  </li>
                ))}
                {tableChanges.length > 5 && (
                  <li className="text-center text-xs text-secondary-500 py-1">
                    + {tableChanges.length - 5} more changes for this table
                  </li>
                )}
              </ul>

              {onAcknowledge && tableChanges.some(change => !change.acknowledged) && (
                <div className="mt-4 flex justify-end">
                  <button
                    onClick={() => handleAcknowledge(tableName)}
                    disabled={acknowledging[tableName]}
                    className="inline-flex items-center px-3 py-1.5 border border-transparent
                              text-xs font-medium rounded-md shadow-sm text-white
                              bg-primary-600 hover:bg-primary-700 focus:outline-none disabled:opacity-50"
                  >
                    {acknowledging[tableName] ? (
                      <>
                        <LoadingSpinner size="xs" className="mr-1" />
                        Acknowledging...
                      </>
                    ) : (
                      'Acknowledge Changes'
                    )}
                  </button>
                </div>
              )}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
};

export default SchemaChangesPanel;