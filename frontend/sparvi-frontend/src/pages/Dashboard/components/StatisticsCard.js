import React from 'react';
import { Link } from 'react-router-dom';
import { ArrowRightIcon } from '@heroicons/react/20/solid';

const StatisticCard = ({ title, value, icon: Icon, href, color = 'primary', loading = false }) => {
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

  return (
    <div className="card px-4 py-5 sm:p-6">
      <div className="flex items-center">
        <div className={`flex-shrink-0 rounded-md p-3 ${colorClasses[color].icon}`}>
          <Icon className="h-6 w-6" aria-hidden="true" />
        </div>
        <div className="ml-5 w-0 flex-1">
          <dl>
            <dt className="text-sm font-medium text-secondary-500 truncate">{title}</dt>
            <dd>
              {loading ? (
                <div className="h-8 w-16 bg-secondary-200 animate-pulse rounded"></div>
              ) : (
                <div className="text-lg font-medium text-secondary-900">{value}</div>
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