// src/pages/Metadata/components/SchemaChangesPanel.js - UPDATED
import React, { useState } from 'react';
import { ChartBarIcon, CheckCircleIcon, ClockIcon, ExclamationCircleIcon } from '@heroicons/react/24/outline';
import { formatDate } from '../../../utils/formatting';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

const SchemaChangesPanel = ({ connectionId, schemaChanges, isLoading, onAcknowledge }) => {
  const [filter, setFilter] = useState('all');

  if (isLoading) {
    return (
      <div className="flex justify-center items-center py-8">
        <LoadingSpinner size="md" />
        <span className="ml-2 text-secondary-500">Loading schema changes...</span>
      </div>
    );
  }

  // Handle empty state
  if (!schemaChanges || schemaChanges.length === 0) {
    return (
      <div className="text-center py-8">
        <CheckCircleIcon className="mx-auto h-12 w-12 text-accent-400" />
        <h3 className="mt-2 text-sm font-medium text-secondary-900">No Schema Changes Detected</h3>
        <p className="mt-1 text-sm text-secondary-500">
          Your database schema is currently in sync with the metadata.
        </p>
      </div>
    );
  }

  // Normalize change object format
  const normalizeChange = (change) => {
    // Handle the different backend change types to map to our UI display format
    const getChangeType = (type) => {
      if (type.includes('added')) return 'added';
      if (type.includes('removed')) return 'removed';
      if (type.includes('changed')) return 'modified';
      return 'modified';
    };

    const tableName = change.table || '';
    const columnName = change.column || '';
    const changeType = getChangeType(change.type || '');

    let details = '';
    let oldValue = '';
    let newValue = '';

    // Build details based on change type
    if (change.type === 'column_type_changed') {
      details = `Column data type changed`;
      oldValue = change.details?.previous_type || '';
      newValue = change.details?.new_type || '';
    } else if (change.type === 'column_nullability_changed') {
      details = `Column nullability changed`;
      oldValue = change.details?.previous_nullable ? 'NULLABLE' : 'NOT NULL';
      newValue = change.details?.new_nullable ? 'NULLABLE' : 'NOT NULL';
    } else if (change.type === 'column_added') {
      details = `New column added`;
      newValue = change.details?.type || '';
    } else if (change.type === 'column_removed') {
      details = `Column removed`;
      oldValue = change.details?.type || '';
    } else if (change.type === 'table_added') {
      details = `New table added`;
    } else if (change.type === 'table_removed') {
      details = `Table removed`;
    }

    return {
      id: change.id || `${change.type}-${tableName}-${columnName}-${Date.now()}`,
      table_name: tableName,
      column_name: columnName,
      change_type: changeType,
      object_type: change.type?.includes('column') ? 'column' : 'table',
      object_name: change.type?.includes('column') ? columnName : tableName,
      details: details,
      old_value: oldValue,
      new_value: newValue,
      detected_at: change.timestamp || new Date().toISOString(),
      acknowledged: change.acknowledged || false
    };
  };

  // Normalize all changes
  const normalizedChanges = schemaChanges.map(normalizeChange);

  // Filter changes based on selected filter
  const filteredChanges = filter === 'all'
    ? normalizedChanges
    : normalizedChanges.filter(change => change.change_type === filter);

  // Group changes by table
  const changesByTable = {};
  filteredChanges.forEach(change => {
    if (!changesByTable[change.table_name]) {
      changesByTable[change.table_name] = [];
    }
    changesByTable[change.table_name].push(change);
  });

  // Get icon for change type
  const getChangeTypeIcon = (type) => {
    switch (type) {
      case 'added':
        return <span className="text-accent-400">+</span>;
      case 'removed':
        return <span className="text-danger-400">-</span>;
      case 'modified':
        return <span className="text-warning-400">~</span>;
      default:
        return <span className="text-secondary-400">•</span>;
    }
  };

  // Get badge for change type
  const getChangeTypeBadge = (type) => {
    switch (type) {
      case 'added':
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-accent-100 text-accent-800">
            Added
          </span>
        );
      case 'removed':
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-danger-100 text-danger-800">
            Removed
          </span>
        );
      case 'modified':
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-warning-100 text-warning-800">
            Modified
          </span>
        );
      default:
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-secondary-100 text-secondary-800">
            Unknown
          </span>
        );
    }
  };

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <h3 className="text-lg font-medium text-secondary-900">
          Schema Changes
        </h3>
        <div className="flex space-x-2">
          <select
            id="filter"
            name="filter"
            className="block pl-3 pr-10 py-2 text-base border-secondary-300 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm rounded-md"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          >
            <option value="all">All Changes</option>
            <option value="added">Added</option>
            <option value="removed">Removed</option>
            <option value="modified">Modified</option>
          </select>
        </div>
      </div>

      <div className="bg-white shadow overflow-hidden sm:rounded-md">
        {Object.keys(changesByTable).length > 0 ? (
          <ul className="divide-y divide-secondary-200">
            {Object.entries(changesByTable).map(([tableName, changes]) => (
              <li key={tableName} className="px-4 py-4">
                <div className="flex items-center justify-between">
                  <h4 className="text-sm font-medium text-secondary-900">{tableName}</h4>
                  <span className="text-xs text-secondary-500">
                    {changes.length} {changes.length === 1 ? 'change' : 'changes'}
                  </span>
                </div>
                <ul className="mt-2 space-y-2">
                  {changes.map((change, index) => (
                    <li key={change.id || index} className="text-sm">
                      <div className="flex items-start">
                        <div className="flex-shrink-0 mr-2 mt-0.5">
                          {getChangeTypeIcon(change.change_type)}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center justify-between">
                            <p className="font-medium text-secondary-800">
                              {change.object_type === 'column' ? (
                                <span className="italic">{change.column_name}</span>
                              ) : (
                                change.object_name || tableName
                              )}
                            </p>
                            {getChangeTypeBadge(change.change_type)}
                          </div>
                          <p className="text-secondary-500">
                            {change.details ||
                              `${change.change_type} ${change.object_type} ${change.object_name || ''}`}
                          </p>
                          {change.old_value && change.new_value && (
                            <div className="mt-1 text-xs">
                              <span className="text-danger-500">
                                {change.old_value}
                              </span>
                              <span className="mx-2">→</span>
                              <span className="text-accent-500">
                                {change.new_value}
                              </span>
                            </div>
                          )}
                          {change.detected_at && (
                            <div className="mt-1 text-xs text-secondary-400">
                              Detected: {formatDate(change.detected_at, true)}
                            </div>
                          )}
                        </div>
                      </div>
                    </li>
                  ))}
                </ul>
                {!changes[0].acknowledged && (
                  <div className="mt-4 flex justify-end">
                    <button
                      type="button"
                      onClick={() => onAcknowledge(tableName)}
                      className="inline-flex items-center px-3 py-1.5 border border-transparent text-xs font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                    >
                      Acknowledge Changes
                    </button>
                  </div>
                )}
              </li>
            ))}
          </ul>
        ) : (
          <div className="text-center py-6">
            <p className="text-secondary-500">No changes match the selected filter</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default SchemaChangesPanel;