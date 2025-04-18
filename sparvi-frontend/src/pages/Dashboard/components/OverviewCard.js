import React from 'react';
import { Link } from 'react-router-dom';
import { ArrowRightIcon } from '@heroicons/react/20/solid';
import { useUI } from '../../../contexts/UIContext';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { useTablesData } from '../../../hooks/useTablesData';
import { useValidationsSummary } from '../../../hooks/useValidationsData';

const OverviewCard = ({ title, type = 'tables', connectionId }) => {
  const { showNotification } = useUI();

  // Use the appropriate query hook based on the card type
  const tablesQuery = useTablesData(connectionId, {
    enabled: type === 'tables' && !!connectionId
  });

  // For validations, we use the summary query
  const validationsSummaryQuery = useValidationsSummary(connectionId, {
    enabled: type === 'validations' && !!connectionId
  });

  // Choose the appropriate data and query based on type
  const query = type === 'tables' ? tablesQuery : validationsSummaryQuery;

  // For tables, data is just the array of tables
  // For validations, we'll handle different potential response formats
  let data = [];

  if (type === 'tables' && tablesQuery.data) {
    data = tablesQuery.data;
    console.log(`[${type}] Tables data in OverviewCard:`, data);
  } else if (type === 'validations' && validationsSummaryQuery.data) {
    // Extract validation summary data
    const summary = validationsSummaryQuery.data;
    console.log(`[${type}] Validations summary in OverviewCard:`, summary);

    if (summary.validations_by_table) {
      // Transform validations_by_table into an array of table validation info
      data = Object.entries(summary.validations_by_table).map(([tableName, tableData]) => ({
        tableName,
        count: tableData.total || 0,
        passingCount: tableData.passing || 0,
        failingCount: tableData.failing || 0,
        unknownCount: tableData.unknown || 0,
        healthScore: tableData.health_score || 0,
        lastRun: tableData.last_run
      })).sort((a, b) => {
        // Sort by failures first, then by health score
        if (a.failingCount !== b.failingCount) return b.failingCount - a.failingCount;
        return (b.healthScore || 0) - (a.healthScore || 0);
      }).slice(0, 5); // Show top 5 tables
    } else if (summary.total_count > 0) {
      // Second format: has total_count but no validations_by_table
      // We'll create a single entry for all validations
      data = [{
        tableName: 'All Tables',
        count: summary.total_count,
        passingCount: summary.passing_count || 0,
        failingCount: summary.failing_count || 0,
        unknownCount: summary.unknown_count || 0,
        healthScore: summary.overall_health_score || 0,
        tablesWithValidations: summary.tables_with_validations || 0,
        tablesWithFailures: summary.tables_with_failures || 0,
        freshness: summary.freshness_status || 'unknown'
      }];
    }
  }

  // Get the appropriate link for the "View all" button
  const getLink = () => {
    switch (type) {
      case 'tables':
        return connectionId ? `/explorer?connection=${connectionId}` : '/explorer';
      case 'validations':
        return '/validations';
      default:
        return '/dashboard';
    }
  };

  // Determine status class for validation items
  const getStatusClass = (item) => {
    if (item.failingCount > 0) return 'bg-danger-100 text-danger-800';
    if (item.passingCount > 0) return 'bg-accent-100 text-accent-800';
    return 'bg-secondary-100 text-secondary-800';
  };

  // Determine status text for validation items
  const getStatusText = (item) => {
    if (item.failingCount > 0) return `${item.failingCount} failing`;
    if (item.passingCount > 0) return 'All passing';
    return 'Not run';
  };

  // Format the health score as a percentage
  const formatHealthScore = (score) => {
    if (score === undefined || score === null) return '';
    return `${Math.round(score)}%`;
  };

  // Format the date for lastRun
  const formatLastRun = (lastRun) => {
    if (!lastRun) return '';

    try {
      const date = new Date(lastRun);
      return date.toLocaleString(undefined, {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
      });
    } catch (e) {
      return '';
    }
  };

  return (
    <div className="card overflow-hidden">
      <div className="px-4 py-5 sm:px-6 flex justify-between items-center bg-white border-b border-secondary-200">
        <h3 className="text-lg leading-6 font-medium text-secondary-900">{title} Overview</h3>
        <Link
          to={getLink()}
          className="text-sm font-medium text-primary-600 hover:text-primary-500 flex items-center"
        >
          View all
          <ArrowRightIcon className="ml-1 h-4 w-4" aria-hidden="true" />
        </Link>
      </div>
      <div className="bg-white px-4 py-5 sm:p-6">
        {query.isLoading ? (
          <div className="space-y-3">
            {[...Array(3)].map((_, i) => (
              <div key={i} className="animate-pulse flex space-x-4">
                <div className="rounded-full bg-secondary-200 h-10 w-10"></div>
                <div className="flex-1 space-y-2 py-1">
                  <div className="h-4 bg-secondary-200 rounded w-3/4"></div>
                  <div className="h-4 bg-secondary-200 rounded w-1/2"></div>
                </div>
              </div>
            ))}
          </div>
        ) : query.isError ? (
          <div className="text-center py-6">
            <p className="text-sm text-danger-500">Error loading {type}</p>
            <button
              onClick={() => query.refetch()}
              className="mt-2 text-sm text-primary-600 hover:text-primary-500"
            >
              Try again
            </button>
          </div>
        ) : data.length === 0 ? (
          <div className="text-center py-6">
            <p className="text-sm text-secondary-500">No {type} found</p>
            <Link
              to={getLink()}
              className="mt-2 inline-flex items-center px-3 py-1.5 border border-transparent text-xs font-medium rounded-md text-primary-700 bg-primary-100 hover:bg-primary-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              Add {type === 'tables' ? 'a table' : 'a validation'}
            </Link>
          </div>
        ) : (
          <ul className="divide-y divide-secondary-200">
            {data.map((item, index) => (
              <li key={type === 'tables' ? item : `${item.tableName}-${index}`} className="py-3">
                {type === 'tables' ? (
                  <Link
                    to={`/explorer/${connectionId}/tables/${item}`}
                    className="flex justify-between items-center hover:bg-secondary-50 px-2 py-1 rounded-md"
                  >
                    <span className="text-sm font-medium text-secondary-900">{item}</span>
                    <ArrowRightIcon className="h-4 w-4 text-secondary-400" aria-hidden="true" />
                  </Link>
                ) : (
                  <Link
                    to={item.tableName === 'All Tables' ? '/validations' : `/validations?table=${item.tableName}`}
                    className="flex justify-between items-center hover:bg-secondary-50 px-2 py-1 rounded-md"
                  >
                    <div>
                      <span className="text-sm font-medium text-secondary-900">{item.tableName}</span>
                      <p className="text-xs text-secondary-500 truncate">
                        {item.count} validation rule{item.count !== 1 ? 's' : ''}
                        {item.healthScore !== undefined && (
                          <span className="ml-2">
                            Health: <span
                              className={`font-medium ${
                                item.healthScore >= 90 ? 'text-accent-600' : 
                                item.healthScore >= 70 ? 'text-warning-600' : 
                                'text-danger-600'
                              }`}
                            >
                              {formatHealthScore(item.healthScore)}
                            </span>
                          </span>
                        )}
                        {item.lastRun && (
                          <span className="ml-2 text-xs text-secondary-400">
                            {formatLastRun(item.lastRun)}
                          </span>
                        )}
                      </p>
                      {item.tablesWithValidations > 0 && (
                        <p className="text-xs text-secondary-500">
                          {item.tablesWithValidations} table{item.tablesWithValidations !== 1 ? 's' : ''} with validations
                          {item.tablesWithFailures > 0 && (
                            <span className="ml-1 text-danger-600">
                              ({item.tablesWithFailures} with failures)
                            </span>
                          )}
                        </p>
                      )}
                    </div>
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStatusClass(item)}`}>
                      {getStatusText(item)}
                    </span>
                  </Link>
                )}
              </li>
            ))}
          </ul>
        )}

        {/* Optional loading indicator for background refetches */}
        {query.isRefetching && !query.isLoading && (
          <div className="mt-2 flex justify-center">
            <LoadingSpinner size="sm" className="opacity-50" />
            <span className="ml-2 text-xs text-secondary-400">Refreshing...</span>
          </div>
        )}
      </div>
    </div>
  );
};

export default OverviewCard;