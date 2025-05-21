import React from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import {
  HomeIcon,
  ServerIcon,
  TableCellsIcon,
  ClipboardDocumentCheckIcon,
  ChartBarIcon,
  Cog6ToothIcon,
  ShieldCheckIcon,
  CommandLineIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline';
import { useUI } from '../../contexts/UIContext';
import Logo from '../common/Logo';

const Sidebar = () => {
  const { sidebarOpen, isMobile } = useUI();
  const location = useLocation();

  // Define navigation items
  const navigation = [
    { name: 'Dashboard', icon: HomeIcon, href: '/dashboard', exact: true },
    { name: 'Connections', icon: ServerIcon, href: '/connections' },
    // { name: 'Data Explorer', icon: TableCellsIcon, href: '/explorer' },
    { name: 'Validations', icon: ClipboardDocumentCheckIcon, href: '/validations' },
    { name: 'Metadata', icon: CommandLineIcon, href: '/metadata' },
    { name: 'Anomalies', icon: ExclamationTriangleIcon, href: '/anomalies' },

    // { name: 'Analytics', icon: ChartBarIcon, href: '/analytics' },
    // { name: 'Admin', icon: ShieldCheckIcon, href: '/admin' },
    // { name: 'Settings', icon: Cog6ToothIcon, href: '/settings' },
  ];

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
              {navigation.map((item) => (
                <NavLink
                  key={item.name}
                  to={item.href}
                  className={({ isActive }) =>
                    `sidebar-link ${isActive ? 'active' : ''}`
                  }
                >
                  <item.icon className="mr-3 h-5 w-5" aria-hidden="true" />
                  {item.name}
                </NavLink>
              ))}
            </div>
          </nav>
        </div>

        {/* Footer */}
        <div className="p-4 border-t border-secondary-200">
          <div className="flex items-center">
            <div className="text-xs text-secondary-500">
              <div>Sparvi Cloud</div>
              <div>Version: 1.0.0</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Sidebar;