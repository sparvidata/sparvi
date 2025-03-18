import React from 'react';
import { Outlet } from 'react-router-dom';
import { useUI } from '../../contexts/UIContext';
import Header from './Header';
import Sidebar from './Sidebar';
import Notifications from '../common/Notifications';
import Breadcrumbs from '../common/Breadcrumbs';

/**
 * MainLayout component that provides the common layout for authenticated pages
 * Includes header, sidebar, and content area
 */
const MainLayout = () => {
  const { sidebarOpen, isMobile } = useUI();

  return (
    <div className="flex h-screen bg-secondary-50 overflow-hidden">
      {/* Sidebar */}
      <Sidebar />

      {/* Mobile sidebar backdrop */}
      {isMobile && sidebarOpen && (
        <div
          className="fixed inset-0 z-20 bg-secondary-900 bg-opacity-50 transition-opacity lg:hidden"
          aria-hidden="true"
        />
      )}

      {/* Main content area */}
      <div className="flex flex-col flex-1 overflow-hidden">
        {/* Header */}
        <Header />

        {/* Main content */}
        <main className="flex-1 relative overflow-y-auto focus:outline-none">
          <div className="py-6">
            <div className="max-w-7xl mx-auto px-4 sm:px-6 md:px-8">
              {/* Breadcrumbs */}
              <Breadcrumbs />

              {/* Page content */}
              <div className="py-4">
                <Outlet />
              </div>
            </div>
          </div>
        </main>
      </div>

      {/* Toast notifications */}
      <Notifications />
    </div>
  );
};

export default MainLayout;