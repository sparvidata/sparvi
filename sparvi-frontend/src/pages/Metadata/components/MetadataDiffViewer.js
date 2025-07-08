import React, { useState } from 'react';
import {
  PlusIcon,
  MinusIcon,
  ExclamationTriangleIcon,
  ArrowRightIcon,
  MagnifyingGlassIcon
} from '@heroicons/react/24/outline';
import SearchInput from '../../../components/common/SearchInput';

const MetadataDiffViewer = ({
  primaryData,
  compareData,
  primaryDate,
  compareDate,
  metadataType,
  tableName
}) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [showChangesOnly, setShowChangesOnly] = useState(false);

  // Calculate differences
  const differences = React.useMemo(() => {
    if (!primaryData || !compareData) return { added: [], removed: [], modified: [], unchanged: [] };

    let primaryItems = [];
    let compareItems = [];

    // Extract items based on metadata type
    if (metadataType === 'tables') {
      // Extract tables
      primaryItems = extractTables(primaryData.metadata);
      compareItems = extractTables(compareData.metadata);
    } else {
      // Extract columns
      primaryItems = extractColumns(primaryData.metadata);
      compareItems = extractColumns(compareData.metadata);
    }

    return calculateDifferences(primaryItems, compareItems, metadataType);
  }, [primaryData, compareData, metadataType]);

  // Extract tables from metadata
  const extractTables = (metadata) => {
    if (!metadata) return [];

    if (Array.isArray(metadata)) {
      return metadata;
    } else if (metadata.tables) {
      return Object.keys(metadata.tables).map(tableName => ({
        name: tableName,
        ...metadata.tables[tableName]
      }));
    }
    return [];
  };

  // Extract columns from metadata
  const extractColumns = (metadata) => {
    if (!metadata) return [];

    let columns = [];
    if (metadata.columns_by_table) {
      Object.entries(metadata.columns_by_table).forEach(([table, cols]) => {
        cols.forEach(column => {
          columns.push({
            ...column,
            table_name: table,
            _key: `${table}.${column.name}`
          });
        });
      });
    } else if (metadata.columns) {
      columns = metadata.columns.map(col => ({
        ...col,
        _key: `${col.table_name}.${col.name}`
      }));
    }
    return columns;
  };

  // Calculate differences between two sets of items
  const calculateDifferences = (primaryItems, compareItems, type) => {
    const getKey = (item) => type === 'tables' ? item.name : item._key;

    const primaryMap = new Map(primaryItems.map(item => [getKey(item), item]));
    const compareMap = new Map(compareItems.map(item => [getKey(item), item]));

    const added = [];
    const removed = [];
    const modified = [];
    const unchanged = [];

    // Find added items (in primary but not in compare)
    for (const [key, item] of primaryMap) {
      if (!compareMap.has(key)) {
        added.push({ type: 'added', item, key });
      }
    }

    // Find removed items (in compare but not in primary)
    for (const [key, item] of compareMap) {
      if (!primaryMap.has(key)) {
        removed.push({ type: 'removed', item, key });
      }
    }

    // Find modified and unchanged items
    for (const [key, primaryItem] of primaryMap) {
      const compareItem = compareMap.get(key);
      if (compareItem) {
        const hasChanges = hasItemChanged(primaryItem, compareItem, type);
        if (hasChanges) {
          modified.push({
            type: 'modified',
            primaryItem,
            compareItem,
            key,
            changes: getItemChanges(primaryItem, compareItem, type)
          });
        } else {
          unchanged.push({ type: 'unchanged', item: primaryItem, key });
        }
      }
    }

    return { added, removed, modified, unchanged };
  };

  // Check if an item has changed between versions
  const hasItemChanged = (primary, compare, type) => {
    if (type === 'tables') {
      return primary.row_count !== compare.row_count ||
             primary.column_count !== compare.column_count ||
             primary.size_bytes !== compare.size_bytes ||
             primary.schema !== compare.schema;
    } else {
      return primary.data_type !== compare.data_type ||
             primary.nullable !== compare.nullable ||
             primary.column_default !== compare.column_default ||
             primary.ordinal_position !== compare.ordinal_position;
    }
  };

  // Get specific changes for an item
  const getItemChanges = (primary, compare, type) => {
    const changes = [];

    if (type === 'tables') {
      if (primary.row_count !== compare.row_count) {
        changes.push({
          field: 'row_count',
          oldValue: compare.row_count,
          newValue: primary.row_count,
          label: 'Row Count'
        });
      }
      if (primary.column_count !== compare.column_count) {
        changes.push({
          field: 'column_count',
          oldValue: compare.column_count,
          newValue: primary.column_count,
          label: 'Column Count'
        });
      }
      if (primary.size_bytes !== compare.size_bytes) {
        changes.push({
          field: 'size_bytes',
          oldValue: compare.size_bytes,
          newValue: primary.size_bytes,
          label: 'Size'
        });
      }
    } else {
      if (primary.data_type !== compare.data_type) {
        changes.push({
          field: 'data_type',
          oldValue: compare.data_type,
          newValue: primary.data_type,
          label: 'Data Type'
        });
      }
      if (primary.nullable !== compare.nullable) {
        changes.push({
          field: 'nullable',
          oldValue: compare.nullable,
          newValue: primary.nullable,
          label: 'Nullable'
        });
      }
      if (primary.column_default !== compare.column_default) {
        changes.push({
          field: 'column_default',
          oldValue: compare.column_default,
          newValue: primary.column_default,
          label: 'Default Value'
        });
      }
    }

    return changes;
  };

  // Filter items based on search and show changes only
  const filteredDifferences = React.useMemo(() => {
    let allItems = [];

    if (showChangesOnly) {
      allItems = [...differences.added, ...differences.removed, ...differences.modified];
    } else {
      allItems = [...differences.added, ...differences.removed, ...differences.modified, ...differences.unchanged];
    }

    if (searchQuery) {
      allItems = allItems.filter(diff => {
        const item = diff.item || diff.primaryItem;
        const searchableText = metadataType === 'tables'
          ? item.name?.toLowerCase() || ''
          : `${item.table_name?.toLowerCase() || ''} ${item.name?.toLowerCase() || ''}`;
        return searchableText.includes(searchQuery.toLowerCase());
      });
    }

    return allItems;
  }, [differences, searchQuery, showChangesOnly, metadataType]);

  // Get badge for change type
  const getChangeBadge = (type) => {
    switch (type) {
      case 'added':
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-accent-100 text-accent-800">
            <PlusIcon className="mr-1 h-3 w-3" />
            Added
          </span>
        );
      case 'removed':
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-danger-100 text-danger-800">
            <MinusIcon className="mr-1 h-3 w-3" />
            Removed
          </span>
        );
      case 'modified':
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-warning-100 text-warning-800">
            <ExclamationTriangleIcon className="mr-1 h-3 w-3" />
            Modified
          </span>
        );
      default:
        return (
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-secondary-100 text-secondary-800">
            Unchanged
          </span>
        );
    }
  };

  // Render change details
  const renderChangeDetails = (changes) => {
    if (!changes || changes.length === 0) return null;

    return (
      <div className="mt-2 space-y-1">
        {changes.map((change, index) => (
          <div key={index} className="text-xs text-secondary-600 flex items-center">
            <span className="font-medium">{change.label}:</span>
            <span className="ml-1 text-danger-600">{formatValue(change.oldValue)}</span>
            <ArrowRightIcon className="mx-1 h-3 w-3 text-secondary-400" />
            <span className="text-accent-600">{formatValue(change.newValue)}</span>
          </div>
        ))}
      </div>
    );
  };

  // Format value for display
  const formatValue = (value) => {
    if (value === null || value === undefined) return 'null';
    if (typeof value === 'boolean') return value ? 'true' : 'false';
    if (typeof value === 'number') return value.toLocaleString();
    return String(value);
  };

  // Summary statistics
  const stats = {
    total: filteredDifferences.length,
    added: differences.added.length,
    removed: differences.removed.length,
    modified: differences.modified.length,
    unchanged: differences.unchanged.length
  };

  if (!primaryData || !compareData) {
    return (
      <div className="text-center py-8">
        <ExclamationTriangleIcon className="mx-auto h-12 w-12 text-secondary-400" />
        <h3 className="mt-2 text-sm font-medium text-secondary-900">Missing Data</h3>
        <p className="mt-1 text-sm text-secondary-500">
          Both dates must have data to perform a comparison
        </p>
      </div>
    );
  }

  return (
    <div className="bg-white shadow rounded-lg">
      {/* Header */}
      <div className="px-6 py-4 border-b border-secondary-200">
        <div className="flex justify-between items-center mb-4">
          <div>
            <h3 className="text-lg font-medium text-secondary-900">
              Metadata Comparison: {metadataType === 'tables' ? 'Tables' : 'Columns'}
            </h3>
            <p className="mt-1 text-sm text-secondary-500">
              Comparing {new Date(compareDate).toLocaleDateString()} → {new Date(primaryDate).toLocaleDateString()}
              {tableName && ` • Table: ${tableName}`}
            </p>
          </div>
          <div className="w-64">
            <SearchInput
              onSearch={setSearchQuery}
              placeholder={`Search ${metadataType}...`}
              initialValue={searchQuery}
            />
          </div>
        </div>

        {/* Summary Stats */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-4">
          <div className="text-center">
            <div className="text-2xl font-bold text-secondary-900">{stats.total}</div>
            <div className="text-xs text-secondary-500">Total</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-accent-600">{stats.added}</div>
            <div className="text-xs text-secondary-500">Added</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-danger-600">{stats.removed}</div>
            <div className="text-xs text-secondary-500">Removed</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-warning-600">{stats.modified}</div>
            <div className="text-xs text-secondary-500">Modified</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-secondary-500">{stats.unchanged}</div>
            <div className="text-xs text-secondary-500">Unchanged</div>
          </div>
        </div>

        {/* Filters */}
        <div className="flex items-center space-x-4">
          <label className="flex items-center">
            <input
              type="checkbox"
              checked={showChangesOnly}
              onChange={(e) => setShowChangesOnly(e.target.checked)}
              className="h-4 w-4 text-primary-600 focus:ring-primary-500 border-secondary-300 rounded"
            />
            <span className="ml-2 text-sm text-secondary-700">Show changes only</span>
          </label>
        </div>
      </div>

      {/* Content */}
      <div className="overflow-x-auto">
        {filteredDifferences.length === 0 ? (
          <div className="text-center py-8">
            <MagnifyingGlassIcon className="mx-auto h-10 w-10 text-secondary-400" />
            <p className="mt-2 text-secondary-500">
              {searchQuery ? 'No items match your search' : showChangesOnly ? 'No changes found' : 'No data to display'}
            </p>
          </div>
        ) : (
          <table className="min-w-full divide-y divide-secondary-200">
            <thead className="bg-secondary-50">
              <tr>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                  Status
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                  {metadataType === 'tables' ? 'Table Name' : 'Table'}
                </th>
                {metadataType === 'columns' && (
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                    Column
                  </th>
                )}
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                  {metadataType === 'tables' ? 'Row Count' : 'Data Type'}
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-secondary-500 uppercase tracking-wider">
                  Changes
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-secondary-200">
              {filteredDifferences.map((diff, index) => {
                const item = diff.item || diff.primaryItem;
                return (
                  <tr key={diff.key || index} className={index % 2 === 0 ? 'bg-white' : 'bg-secondary-50'}>
                    <td className="px-6 py-4 whitespace-nowrap">
                      {getChangeBadge(diff.type)}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-secondary-900">
                      {metadataType === 'tables' ? item.name : item.table_name}
                    </td>
                    {metadataType === 'columns' && (
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                        {item.name}
                      </td>
                    )}
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary-500">
                      {metadataType === 'tables'
                        ? (item.row_count?.toLocaleString() || '-')
                        : (item.data_type || item.type || '-')
                      }
                    </td>
                    <td className="px-6 py-4 text-sm text-secondary-500">
                      {diff.type === 'modified' ? renderChangeDetails(diff.changes) :
                       diff.type === 'added' ? 'New item' :
                       diff.type === 'removed' ? 'Item removed' : 'No changes'}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
};

export default MetadataDiffViewer;