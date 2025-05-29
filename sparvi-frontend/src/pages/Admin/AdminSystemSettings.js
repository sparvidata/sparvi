import React from 'react';
import { Cog6ToothIcon } from '@heroicons/react/24/outline';

const AdminSystemSettings = () => {
  return (
    <div className="space-y-6">
      <div className="flex items-center">
        <Cog6ToothIcon className="h-5 w-5 text-secondary-400 mr-3" />
        <div>
          <h3 className="text-lg font-medium text-secondary-900">System Settings</h3>
          <p className="text-sm text-secondary-500">Configure system-wide settings and preferences</p>
        </div>
      </div>

      <div className="bg-secondary-50 border border-secondary-200 rounded-lg p-8 text-center">
        <Cog6ToothIcon className="h-12 w-12 text-secondary-400 mx-auto mb-4" />
        <h3 className="text-lg font-medium text-secondary-900 mb-2">System Configuration</h3>
        <p className="text-secondary-500">
          System settings will be implemented here. This will include data retention policies,
          backup configurations, and other system-wide preferences.
        </p>
      </div>
    </div>
  );
};

export default AdminSystemSettings;