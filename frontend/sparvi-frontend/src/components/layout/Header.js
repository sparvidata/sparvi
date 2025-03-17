import React, { Fragment } from 'react';
import { Link } from 'react-router-dom';
import { Menu, Transition } from '@headlessui/react';
import {
  Bars3Icon,
  XMarkIcon,
  MagnifyingGlassIcon,
  BellIcon,
  UserCircleIcon,
  Cog6ToothIcon,
  ArrowRightOnRectangleIcon,
  SunIcon,
  MoonIcon
} from '@heroicons/react/24/outline';
import { useAuth } from '../../contexts/AuthContext';
import { useUI } from '../../contexts/UIContext';
import { useConnection } from '../../contexts/ConnectionContext';
import Logo from '../common/Logo';

const Header = () => {
  const { user, logout } = useAuth();
  const { sidebarOpen, toggleSidebar, theme, toggleTheme } = useUI();
  const { connections, activeConnection, setCurrentConnection } = useConnection();

  // Extract first name from user data
  const firstName = user?.user_metadata?.first_name || '';

  // Handle logout
  const handleLogout = async () => {
    try {
      await logout();
    } catch (error) {
      console.error('Logout failed:', error);
    }
  };

  return (
    <header className="sticky top-0 z-10 flex-shrink-0 h-16 bg-white shadow flex">
      {/* Left section with menu button and search */}
      <div className="flex-1 flex items-center justify-between px-4">
        <div className="flex items-center lg:w-64">
          {/* Sidebar toggle button */}
          <button
            type="button"
            className="text-secondary-500 focus:outline-none lg:hidden"
            onClick={toggleSidebar}
            aria-label="Toggle sidebar"
          >
            {sidebarOpen ? (
              <XMarkIcon className="h-6 w-6" aria-hidden="true" />
            ) : (
              <Bars3Icon className="h-6 w-6" aria-hidden="true" />
            )}
          </button>

          {/* Logo for mobile view */}
          <div className="lg:hidden ml-2">
            <Logo size="small" />
          </div>

          {/* Search bar */}
          <div className="hidden sm:flex ml-4 lg:ml-0">
            <div className="flex items-center max-w-lg w-full">
              <label htmlFor="search" className="sr-only">Search</label>
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <MagnifyingGlassIcon className="h-5 w-5 text-secondary-400" aria-hidden="true" />
                </div>
                <input
                  id="search"
                  name="search"
                  className="block w-full pl-10 pr-3 py-2 border border-secondary-300 rounded-md leading-5 bg-white placeholder-secondary-500 focus:outline-none focus:placeholder-secondary-400 focus:ring-1 focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                  placeholder="Search"
                  type="search"
                />
              </div>
            </div>
          </div>
        </div>

        {/* Center section with connection selector */}
        <div className="hidden md:flex items-center justify-center flex-1">
          {connections.length > 0 && (
            <div className="relative inline-block text-left">
              <Menu as="div" className="relative inline-block text-left">
                <div>
                  <Menu.Button className="inline-flex justify-center w-full rounded-md border border-secondary-300 shadow-sm px-4 py-2 bg-white text-sm font-medium text-secondary-700 hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-secondary-100 focus:ring-primary-500">
                    <span className="mr-1 font-medium">Connection:</span>
                    <span className="text-primary-700">{activeConnection?.name || 'Select a connection'}</span>
                    <svg className="-mr-1 ml-2 h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                      <path fillRule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clipRule="evenodd" />
                    </svg>
                  </Menu.Button>
                </div>

                <Transition
                  as={Fragment}
                  enter="transition ease-out duration-100"
                  enterFrom="transform opacity-0 scale-95"
                  enterTo="transform opacity-100 scale-100"
                  leave="transition ease-in duration-75"
                  leaveFrom="transform opacity-100 scale-100"
                  leaveTo="transform opacity-0 scale-95"
                >
                  <Menu.Items className="origin-top-right absolute right-0 mt-2 w-56 rounded-md shadow-lg bg-white ring-1 ring-black ring-opacity-5 focus:outline-none">
                    <div className="py-1">
                      {connections.map((connection) => (
                        <Menu.Item key={connection.id}>
                          {({ active }) => (
                            <button
                              onClick={() => setCurrentConnection(connection)}
                              className={`${
                                active ? 'bg-secondary-100 text-secondary-900' : 'text-secondary-700'
                              } ${
                                activeConnection?.id === connection.id ? 'bg-primary-50 text-primary-700' : ''
                              } group flex items-center w-full px-4 py-2 text-sm`}
                            >
                              {connection.name}
                              {connection.is_default && (
                                <span className="ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-primary-100 text-primary-800">
                                  Default
                                </span>
                              )}
                            </button>
                          )}
                        </Menu.Item>
                      ))}
                      <div className="border-t border-secondary-200 my-1"></div>
                      <Menu.Item>
                        {({ active }) => (
                          <Link
                            to="/connections"
                            className={`${
                              active ? 'bg-secondary-100 text-secondary-900' : 'text-secondary-700'
                            } group flex items-center w-full px-4 py-2 text-sm`}
                          >
                            Manage connections
                          </Link>
                        )}
                      </Menu.Item>
                      <Menu.Item>
                        {({ active }) => (
                          <Link
                            to="/connections/new"
                            className={`${
                              active ? 'bg-secondary-100 text-secondary-900' : 'text-secondary-700'
                            } group flex items-center w-full px-4 py-2 text-sm`}
                          >
                            Add new connection
                          </Link>
                        )}
                      </Menu.Item>
                    </div>
                  </Menu.Items>
                </Transition>
              </Menu>
            </div>
          )}
        </div>

        {/* Right section with notifications and user menu */}
        <div className="flex items-center">
          {/* Theme toggle */}
          <button
            onClick={toggleTheme}
            className="p-1 rounded-full text-secondary-400 hover:text-secondary-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          >
            {theme === 'dark' ? (
              <SunIcon className="h-6 w-6" aria-hidden="true" />
            ) : (
              <MoonIcon className="h-6 w-6" aria-hidden="true" />
            )}
          </button>

          {/* Notifications */}
          <button
            type="button"
            className="ml-3 p-1 rounded-full text-secondary-400 hover:text-secondary-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          >
            <span className="sr-only">View notifications</span>
            <BellIcon className="h-6 w-6" aria-hidden="true" />
          </button>

          {/* Profile dropdown */}
          <Menu as="div" className="ml-3 relative">
            <div>
              <Menu.Button className="max-w-xs bg-white rounded-full flex items-center text-sm focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500">
                <span className="sr-only">Open user menu</span>
                <UserCircleIcon className="h-8 w-8 rounded-full text-secondary-400" />
              </Menu.Button>
            </div>
            <Transition
              as={Fragment}
              enter="transition ease-out duration-100"
              enterFrom="transform opacity-0 scale-95"
              enterTo="transform opacity-100 scale-100"
              leave="transition ease-in duration-75"
              leaveFrom="transform opacity-100 scale-100"
              leaveTo="transform opacity-0 scale-95"
            >
              <Menu.Items className="origin-top-right absolute right-0 mt-2 w-48 rounded-md shadow-lg py-1 bg-white ring-1 ring-black ring-opacity-5 focus:outline-none">
                <div className="px-4 py-2 border-b border-secondary-200">
                  <p className="text-sm font-medium text-secondary-900">
                    {firstName ? `Hi, ${firstName}` : 'My Account'}
                  </p>
                  <p className="text-xs text-secondary-500 truncate">{user?.email}</p>
                </div>
                <Menu.Item>
                  {({ active }) => (
                    <Link
                      to="/settings/profile"
                      className={`${
                        active ? 'bg-secondary-100' : ''
                      } block px-4 py-2 text-sm text-secondary-700 w-full text-left flex items-center`}
                    >
                      <UserCircleIcon className="mr-3 h-5 w-5 text-secondary-400" aria-hidden="true" />
                      Your Profile
                    </Link>
                  )}
                </Menu.Item>
                <Menu.Item>
                  {({ active }) => (
                    <Link
                      to="/settings"
                      className={`${
                        active ? 'bg-secondary-100' : ''
                      } block px-4 py-2 text-sm text-secondary-700 w-full text-left flex items-center`}
                    >
                      <Cog6ToothIcon className="mr-3 h-5 w-5 text-secondary-400" aria-hidden="true" />
                      Settings
                    </Link>
                  )}
                </Menu.Item>
                <Menu.Item>
                  {({ active }) => (
                    <button
                      onClick={handleLogout}
                      className={`${
                        active ? 'bg-secondary-100' : ''
                      } block px-4 py-2 text-sm text-secondary-700 w-full text-left flex items-center`}
                    >
                      <ArrowRightOnRectangleIcon className="mr-3 h-5 w-5 text-secondary-400" aria-hidden="true" />
                      Sign out
                    </button>
                  )}
                </Menu.Item>
              </Menu.Items>
            </Transition>
          </Menu>
        </div>
      </div>
    </header>
  );
};

export default Header;