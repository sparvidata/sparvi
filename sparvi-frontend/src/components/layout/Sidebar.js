import React from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import {
  HomeIcon,
  ServerIcon,
  ClipboardDocumentCheckIcon,
  Cog6ToothIcon,
  ShieldCheckIcon,
  CommandLineIcon,
  // ExclamationTriangleIcon, // TEMPORARILY REMOVED - was used for Anomalies
  ClockIcon
} from '@heroicons/react/24/outline';
import { useUI } from '../../contexts/UIContext';
import { useUserProfile } from '../../hooks/useUserProfile';
import { useAutomationJobCount } from '../../hooks/useAutomationJobCount';
import Logo from '../common/Logo';

const Sidebar = () => {
  const { sidebarOpen, isMobile } = useUI();
  const { isAdmin, loading: profileLoading } = useUserProfile();
  const { activeJobCount, loading: jobCountLoading } = useAutomationJobCount();
  const location = useLocation();

  // Define navigation items
  const navigation = [
    { name: 'Dashboard', icon: HomeIcon, href: '/dashboard', exact: true },
    { name: 'Connections', icon: ServerIcon, href: '/connections' },
    { name: 'Validations', icon: ClipboardDocumentCheckIcon, href: '/validations' },
    { name: 'Metadata', icon: CommandLineIcon, href: '/metadata' },
    // { name: 'Anomalies', icon: ExclamationTriangleIcon, href: '/anomalies' }, // TEMPORARILY REMOVED
    {
      name: 'Automation',
      icon: ClockIcon,
      href: '/automation',
      badge: !jobCountLoading && activeJobCount > 0 ? activeJobCount : null,
      description: 'Manage automated processes'
    },
  ];

  // Add admin section if user is admin (and profile has loaded)
  if (isAdmin && !profileLoading) {
    navigation.push(
      { name: 'Admin', icon: ShieldCheckIcon, href: '/admin', divider: true }
    );
  }

  // Always add settings
  navigation.push(
    { name: 'Settings', icon: Cog6ToothIcon, href: '/settings' }
  );

  // Function to check if a nav item is active
  const isActive = (navItem) => {
    if (navItem.exact) {
      return location.pathname === navItem.href;
    }
    return location.pathname.startsWith(navItem.href);
  };

  // When sidebar is closed in mobile view, don't render anything
  if (isMobile && !sidebarOpen) {
    return null;
  }

  return (
    <div
      className={`${
        sidebarOpen ? 'block' : 'hidden'
      } lg:block lg:flex-shrink-0 lg:w-64 bg-white border-r border-secondary-200 z-30 ${
        isMobile ? 'fixed inset-y-0 left-0 w-64' : ''
      }`}
    >
      <div className="h-full flex flex-col">
        {/* Sidebar header with logo */}
        <div className="h-16 flex items-center px-4 border-b border-secondary-200">
          <Logo />
        </div>

        {/* Navigation */}
        <div className="flex-1 flex flex-col overflow-y-auto">
          <nav className="flex-1 py-4">
            <div className="px-4 space-y-1">
              {navigation.map((item, index) => {
                const Icon = item.icon;
                const active = isActive(item);

                return (
                  <div key={item.name}>
                    {/* Add divider if specified */}
                    {item.divider && (
                      <div className="border-t border-secondary-200 my-3"></div>
                    )}

                    <NavLink
                      to={item.href}
                      className={`sidebar-link ${active ? 'active' : ''} relative`}
                      title={item.description}
                    >
                      <Icon className="mr-3 h-5 w-5" aria-hidden="true" />
                      <span className="flex-1">{item.name}</span>

                      {/* Badge for active job count */}
                      {item.badge && (
                        <span className="ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-primary-100 text-primary-800">
                          {item.badge}
                        </span>
                      )}
                    </NavLink>
                  </div>
                );
              })}
            </div>
          </nav>
        </div>

        {/* Footer */}
        <div className="p-4 border-t border-secondary-200">
          <div className="text-xs text-secondary-500">
            <div>Sparvi Cloud</div>
            <div>Version: 1.0.0</div>
            {!jobCountLoading && activeJobCount > 0 && (
              <div className="mt-1 text-primary-600">
                {activeJobCount} automation job{activeJobCount !== 1 ? 's' : ''} running
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default Sidebar;