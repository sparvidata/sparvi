import React from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import {
  UsersIcon,
  BellIcon,
  Cog6ToothIcon,
  ChartBarIcon,
  ShieldCheckIcon
} from '@heroicons/react/24/outline';

const AdminLayout = ({ children, activeTab, onTabChange }) => {
  const location = useLocation();
  const navigate = useNavigate();

  const tabs = [
    {
      id: 'users',
      name: 'Users',
      icon: UsersIcon,
      path: '/admin/users'
    },
    {
      id: 'notifications',
      name: 'Notifications',
      icon: BellIcon,
      path: '/admin/notifications'
    },
    {
      id: 'settings',
      name: 'Settings',
      icon: Cog6ToothIcon,
      path: '/admin/settings'
    },
    {
      id: 'analytics',
      name: 'Analytics',
      icon: ChartBarIcon,
      path: '/admin/analytics'
    }
  ];

  return (
    <div className="max-w-7xl mx-auto">
      <div className="mb-6">
        <div className="flex items-center mb-2">
          <ShieldCheckIcon className="h-6 w-6 text-primary-600 mr-2" />
          <h1 className="text-2xl font-semibold text-secondary-900">Administration</h1>
        </div>
        <p className="text-sm text-secondary-500">
          Manage organization settings, users, and system configuration.
        </p>
      </div>

      <div className="bg-white shadow rounded-lg">
        {/* Tabs */}
        <div className="border-b border-secondary-200">
          <nav className="-mb-px flex space-x-8 px-6" aria-label="Tabs">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              const isActive = activeTab === tab.id;

              return (
                <button
                  key={tab.id}
                  onClick={() => {
                    onTabChange(tab.id);
                    navigate(tab.path);
                  }}
                  className={`${
                    isActive
                      ? 'border-primary-500 text-primary-600'
                      : 'border-transparent text-secondary-500 hover:text-secondary-700 hover:border-secondary-300'
                  } whitespace-nowrap py-4 px-2 border-b-2 font-medium text-sm flex items-center`}
                >
                  <Icon className="h-5 w-5 mr-2" />
                  {tab.name}
                </button>
              );
            })}
          </nav>
        </div>

        {/* Tab Content */}
        <div className="p-6">
          {children}
        </div>
      </div>
    </div>
  );
};

export default AdminLayout;