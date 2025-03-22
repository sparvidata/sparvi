import React from 'react';
import { ClockIcon } from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

const MetadataTasksList = ({ connectionId, metadataStatus, isLoading }) => {
  // Get active tasks from status
  const activeTasks = metadataStatus?.pending_tasks || [];

  // Format relative time
  const formatRelativeTime = (timestamp) => {
    if (!timestamp) return '';

    try {
      const taskDate = new Date(timestamp);
      const now = new Date();
      const diffMs = now - taskDate;
      const diffSec = Math.round(diffMs / 1000);

      if (diffSec < 60) return `${diffSec}s ago`;
      if (diffSec < 3600) return `${Math.round(diffSec / 60)}m ago`;
      if (diffSec < 86400) return `${Math.round(diffSec / 3600)}h ago`;
      return `${Math.round(diffSec / 86400)}d ago`;
    } catch (error) {
      return '';
    }
  };

  // Get status badge class
  const getStatusClass = (status) => {
    switch(status) {
      case 'pending': return 'bg-secondary-100 text-secondary-800';
      case 'running': return 'bg-primary-100 text-primary-800';
      case 'completed': return 'bg-accent-100 text-accent-800';
      case 'failed': return 'bg-danger-100 text-danger-800';
      default: return 'bg-secondary-100 text-secondary-800';
    }
  };

  // If loading and no tasks, show spinner
  if (isLoading && !metadataStatus) {
    return (
      <div className="flex justify-center py-4">
        <LoadingSpinner size="sm" />
        <span className="ml-2 text-secondary-500 text-xs">Loading tasks...</span>
      </div>
    );
  }

  // If no active tasks, show empty state
  if (activeTasks.length === 0) {
    return (
      <div className="text-center py-4">
        <ClockIcon className="mx-auto h-6 w-6 text-secondary-400" />
        <p className="mt-1 text-sm text-secondary-500">No active tasks</p>
      </div>
    );
  }

  return (
    <div className="overflow-y-auto max-h-64">
      <ul className="divide-y divide-secondary-200">
        {activeTasks.map((task, index) => (
          <li key={task.id || index} className="py-2">
            <div className="flex items-center justify-between">
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-secondary-900 truncate">
                  {task.task_type || 'Unknown task'}
                </p>
                {task.object_name && (
                  <p className="text-xs text-secondary-500 truncate">
                    {task.object_name}
                  </p>
                )}
              </div>
              <div className="ml-2 flex-shrink-0 flex">
                <span className={`px-2 inline-flex text-xs leading-5 font-medium rounded-full ${getStatusClass(task.status)}`}>
                  {task.status === 'running' ? (
                    <span className="flex items-center">
                      <LoadingSpinner size="xs" className="mr-1" />
                      {task.status}
                    </span>
                  ) : task.status}
                </span>
              </div>
            </div>
            {task.created_at && (
              <div className="mt-1 text-xs text-secondary-400 flex items-center">
                <ClockIcon className="mr-1 h-3 w-3" />
                {formatRelativeTime(task.created_at)}
              </div>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
};

export default MetadataTasksList;