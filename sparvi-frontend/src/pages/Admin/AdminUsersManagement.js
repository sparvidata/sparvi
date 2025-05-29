import React from 'react';
import { UsersIcon, PlusIcon } from '@heroicons/react/24/outline';

const AdminUsersManagement = () => {
  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center">
          <UsersIcon className="h-5 w-5 text-secondary-400 mr-3" />
          <div>
            <h3 className="text-lg font-medium text-secondary-900">User Management</h3>
            <p className="text-sm text-secondary-500">Manage organization users and permissions</p>
          </div>
        </div>
        <button
          type="button"
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
        >
          <PlusIcon className="h-4 w-4 mr-2" />
          Invite User
        </button>
      </div>

      <div className="bg-secondary-50 border border-secondary-200 rounded-lg p-8 text-center">
        <UsersIcon className="h-12 w-12 text-secondary-400 mx-auto mb-4" />
        <h3 className="text-lg font-medium text-secondary-900 mb-2">User Management</h3>
        <p className="text-secondary-500">
          User management functionality will be implemented here. This will include user invitations,
          role management, and permission controls.
        </p>
      </div>
    </div>
  );
};

export default AdminUsersManagement;