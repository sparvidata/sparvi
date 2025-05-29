import React from 'react';
import { ChartBarIcon } from '@heroicons/react/24/outline';

const AdminAnalytics = () => {
  return (
    <div className="space-y-6">
      <div className="flex items-center">
        <ChartBarIcon className="h-5 w-5 text-secondary-400 mr-3" />
        <div>
          <h3 className="text-lg font-medium text-secondary-900">Organization Analytics</h3>
          <p className="text-sm text-secondary-500">View usage statistics and system performance metrics</p>
        </div>
      </div>

      <div className="bg-secondary-50 border border-secondary-200 rounded-lg p-8 text-center">
        <ChartBarIcon className="h-12 w-12 text-secondary-400 mx-auto mb-4" />
        <h3 className="text-lg font-medium text-secondary-900 mb-2">Analytics Dashboard</h3>
        <p className="text-secondary-500">
          Organization analytics will be implemented here. This will include user activity,
          system performance metrics, and usage statistics.
        </p>
      </div>
    </div>
  );
};

export default AdminAnalytics;