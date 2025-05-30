import React, { useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import {
  UserCircleIcon,
  BellIcon,
  KeyIcon,
  ShieldCheckIcon,
  ClockIcon
} from '@heroicons/react/24/outline';

const SettingsLayout = ({ children, activeTab, onTabChange }) => {
  const location = useLocation();
  const navigate = useNavigate();

  const tabs = [
    {
      id: 'profile',
      name: 'Profile',
      icon: UserCircleIcon,
      path: '/settings/profile'
    },
    {
      id: 'automation',
      name: 'Automation',
      icon: ClockIcon,
      path: '/settings/automation',
      description: 'Configure automated processes'
    },
    {
      id: 'notifications',
      name: 'Notifications',
      icon: BellIcon,
      path: '/settings/notifications'
    },
    {
      id: 'security',
      name: 'Security',
      icon: KeyIcon,
      path: '/settings/security'
    }
  ];

  const handleTabClick = (tab) => {
    onTabChange(tab.id);
    navigate(tab.path);
  };

  return (
    <div className="max-w-7xl mx-auto">
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-secondary-900">Account Settings</h1>
        <p className="mt-1 text-sm text-secondary-500">
          Manage your account settings, automation, and preferences.
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
                  onClick={() => handleTabClick(tab)}
                  className={`${
                    isActive
                      ? 'border-primary-500 text-primary-600'
                      : 'border-transparent text-secondary-500 hover:text-secondary-700 hover:border-secondary-300'
                  } whitespace-nowrap py-4 px-2 border-b-2 font-medium text-sm flex items-center group`}
                >
                  <Icon className="h-5 w-5 mr-2" />
                  <div className="text-left">
                    <div>{tab.name}</div>
                    {tab.description && (
                      <div className="text-xs text-secondary-400 font-normal">
                        {tab.description}
                      </div>
                    )}
                  </div>
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

export default SettingsLayout;