import React from 'react';
import { Link } from 'react-router-dom';
import {
  ArrowPathIcon,
  CheckCircleIcon,
  XCircleIcon,
  TableCellsIcon,
  PlusIcon,
  MinusIcon,
  PencilIcon
} from '@heroicons/react/24/outline';

const RecentActivity = ({ recentChanges = [], recentValidations = [] }) => {
  // Combine and sort activities by date
  const activities = [
    ...recentChanges.map(change => ({
      type: 'change',
      ...change,
      date: new Date(change.timestamp)
    })),
    ...recentValidations.map(validation => ({
      type: 'validation',
      ...validation,
      date: new Date(validation.timestamp)
    }))
  ].sort((a, b) => b.date - a.date).slice(0, 10); // Get most recent 10

  // Format date relative to now
  const formatRelativeTime = (date) => {
    const now = new Date();
    const diffMs = now - date;
    const diffSec = Math.round(diffMs / 1000);
    const diffMin = Math.round(diffSec / 60);
    const diffHour = Math.round(diffMin / 60);
    const diffDay = Math.round(diffHour / 24);

    if (diffSec < 60) return `${diffSec}s ago`;
    if (diffMin < 60) return `${diffMin}m ago`;
    if (diffHour < 24) return `${diffHour}h ago`;
    if (diffDay === 1) return 'yesterday';
    return `${diffDay}d ago`;
  };

  // Get icon for activity
  const getActivityIcon = (activity) => {
    if (activity.type === 'validation') {
      return activity.is_valid ? (
        <CheckCircleIcon className="h-5 w-5 text-accent-500" />
      ) : (
        <XCircleIcon className="h-5 w-5 text-danger-500" />
      );
    }

    // For changes
    switch (activity.type) {
      case 'table_added':
        return <PlusIcon className="h-5 w-5 text-primary-500" />;
      case 'table_removed':
        return <MinusIcon className="h-5 w-5 text-danger-500" />;
      case 'column_added':
        return <PlusIcon className="h-5 w-5 text-primary-500" />;
      case 'column_removed':
        return <MinusIcon className="h-5 w-5 text-danger-500" />;
      case 'column_type_changed':
      case 'column_nullability_changed':
        return <PencilIcon className="h-5 w-5 text-warning-500" />;
      default:
        return <ArrowPathIcon className="h-5 w-5 text-secondary-500" />;
    }
  };

  // Get title for activity
  const getActivityTitle = (activity) => {
    if (activity.type === 'validation') {
      return (
        <span>
          Validation {' '}
          <span className="font-medium text-secondary-900">
            {activity.rule_name}
          </span>
          {' '} {activity.is_valid ? 'passed' : 'failed'}
        </span>
      );
    }

    // For changes
    switch (activity.type) {
      case 'table_added':
        return (
          <span>
            Table {' '}
            <span className="font-medium text-secondary-900">
              {activity.table}
            </span>
            {' '} was added
          </span>
        );
      case 'table_removed':
        return (
          <span>
            Table {' '}
            <span className="font-medium text-secondary-900">
              {activity.table}
            </span>
            {' '} was removed
          </span>
        );
      case 'column_added':
        return (
          <span>
            Column {' '}
            <span className="font-medium text-secondary-900">
              {activity.column}
            </span>
            {' '} was added to {' '}
            <span className="font-medium text-secondary-900">
              {activity.table}
            </span>
          </span>
        );
      case 'column_removed':
        return (
          <span>
            Column {' '}
            <span className="font-medium text-secondary-900">
              {activity.column}
            </span>
            {' '} was removed from {' '}
            <span className="font-medium text-secondary-900">
              {activity.table}
            </span>
          </span>
        );
      case 'column_type_changed':
        return (
          <span>
            Column {' '}
            <span className="font-medium text-secondary-900">
              {activity.column}
            </span>
            {' '} type changed from {' '}
            <span className="font-mono text-xs bg-secondary-100 px-1 py-0.5 rounded">
              {activity.details?.previous_type || 'unknown'}
            </span>
            {' '} to {' '}
            <span className="font-mono text-xs bg-secondary-100 px-1 py-0.5 rounded">
              {activity.details?.new_type || 'unknown'}
            </span>
          </span>
        );
      case 'column_nullability_changed':
        return (
          <span>
            Column {' '}
            <span className="font-medium text-secondary-900">
              {activity.column}
            </span>
            {' '} nullability changed to {' '}
            <span className="font-mono text-xs bg-secondary-100 px-1 py-0.5 rounded">
              {activity.details?.new_nullable ? 'nullable' : 'not nullable'}
            </span>
          </span>
        );
      default:
        return (
          <span>
            Schema change detected in {' '}
            <span className="font-medium text-secondary-900">
              {activity.table || 'unknown'}
            </span>
          </span>
        );
    }
  };

  // Get link for activity
  const getActivityLink = (activity) => {
    if (activity.type === 'validation') {
      return `/validations/${activity.rule_id}`;
    }

    if (activity.table) {
      return `/explorer/${activity.connection_id}/tables/${activity.table}`;
    }

    return '/metadata';
  };

  return (
    <div className="card overflow-hidden">
      <div className="px-4 py-5 sm:px-6 bg-white border-b border-secondary-200">
        <h3 className="text-lg leading-6 font-medium text-secondary-900">Recent Activity</h3>
      </div>
      <div className="bg-white px-4 py-5 sm:p-6">
        {activities.length === 0 ? (
          <div className="text-center py-6">
            <TableCellsIcon className="mx-auto h-10 w-10 text-secondary-400" />
            <p className="mt-2 text-sm text-secondary-500">No recent activity</p>
            <p className="mt-1 text-xs text-secondary-400">
              Activity will appear here as you work with your data
            </p>
          </div>
        ) : (
          <ul className="divide-y divide-secondary-200">
            {activities.map((activity, index) => (
              <li key={index} className="py-3">
                <div className="flex items-start space-x-4">
                  <div className="flex-shrink-0 mt-1">
                    {getActivityIcon(activity)}
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="text-sm text-secondary-700">
                      {getActivityTitle(activity)}
                    </div>
                    <div className="mt-1 flex items-center text-xs">
                      <Link
                        to={getActivityLink(activity)}
                        className="text-primary-600 hover:text-primary-900 font-medium"
                      >
                        View details
                      </Link>
                      <span className="ml-auto text-secondary-500">
                        {formatRelativeTime(activity.date)}
                      </span>
                    </div>
                  </div>
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
};

export default RecentActivity;