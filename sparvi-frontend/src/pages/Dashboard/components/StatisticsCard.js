import React from 'react';
import { Link } from 'react-router-dom';
import { ArrowRightIcon } from '@heroicons/react/20/solid';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

const StatisticCard = ({
  title,
  value,
  icon: Icon,
  href,
  color = 'primary',
  loading = false,
  error = false,
  isRefetching = false,
  healthScore = null,
  freshness = null
}) => {
  // Define color classes based on the color prop
  const colorClasses = {
    primary: {
      icon: 'bg-primary-100 text-primary-600',
      link: 'text-primary-600 hover:text-primary-700',
    },
    accent: {
      icon: 'bg-accent-100 text-accent-600',
      link: 'text-accent-600 hover:text-accent-700',
    },
    warning: {
      icon: 'bg-warning-100 text-warning-600',
      link: 'text-warning-600 hover:text-warning-700',
    },
    danger: {
      icon: 'bg-danger-100 text-danger-600',
      link: 'text-danger-600 hover:text-danger-700',
    },
  };

  // Get status class for health score
  const getHealthScoreClass = (score) => {
    if (score === null || score === undefined) return '';
    if (score >= 90) return 'bg-accent-100 text-accent-800';
    if (score >= 70) return 'bg-warning-100 text-warning-800';
    return 'bg-danger-100 text-danger-800';
  };

  // Get status class for freshness
  const getFreshnessClass = (status) => {
    if (!status) return '';
    switch(status.toLowerCase()) {
      case 'recent':
        return 'bg-accent-100 text-accent-800';
      case 'stale':
        return 'bg-warning-100 text-warning-800';
      default:
        return 'bg-secondary-100 text-secondary-800';
    }
  };

  return (
    <div className="card px-4 py-5 sm:p-6 relative">
      {/* Health Score Badge (if provided) */}
      {healthScore !== null && (
        <div className={`absolute top-2 right-2 px-2 py-0.5 rounded-md text-xs font-medium ${getHealthScoreClass(healthScore)}`}>
          {Math.round(healthScore)}%
        </div>
      )}

      {/* Freshness Badge (if provided) */}
      {freshness && (
        <div className={`absolute top-2 left-2 px-2 py-0.5 rounded-md text-xs font-medium ${getFreshnessClass(freshness)}`}>
          {freshness.charAt(0).toUpperCase() + freshness.slice(1)}
        </div>
      )}

      <div className="flex items-center">
        <div className={`flex-shrink-0 rounded-md p-3 ${colorClasses[color].icon}`}>
          <Icon className="h-6 w-6" aria-hidden="true" />
        </div>
        <div className="ml-5 w-0 flex-1">
          <dl>
            <dt className="text-sm font-medium text-secondary-500 truncate">{title}</dt>
            <dd>
              {loading ? (
                <div className="flex items-center h-8">
                  <LoadingSpinner size="sm" className="mr-2" />
                  <span className="text-secondary-400">Loading...</span>
                </div>
              ) : error ? (
                <div className="text-danger-500 text-sm">Error loading data</div>
              ) : (
                <div className="text-lg font-medium text-secondary-900 flex items-center">
                  {value !== undefined && value !== null ? value : '-'}
                  {isRefetching && <LoadingSpinner size="xs" className="ml-2 opacity-50" />}
                </div>
              )}
            </dd>
          </dl>
        </div>
      </div>
      {href && (
        <div className="mt-4">
          <Link
            to={href}
            className={`w-full flex items-center justify-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium bg-white hover:bg-secondary-50 ${colorClasses[color].link}`}
          >
            View {title.toLowerCase()}
            <ArrowRightIcon className="ml-2 h-4 w-4" aria-hidden="true" />
          </Link>
        </div>
      )}
    </div>
  );
};

export default StatisticCard;