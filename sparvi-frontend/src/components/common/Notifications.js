import React from 'react';
import { Transition } from '@headlessui/react';
import {
  CheckCircleIcon,
  ExclamationCircleIcon,
  InformationCircleIcon,
  XMarkIcon
} from '@heroicons/react/24/outline';
import { useUI } from '../../contexts/UIContext';

const Notifications = () => {
  const { notifications, removeNotification } = useUI();

  // If no notifications, don't render anything
  if (notifications.length === 0) {
    return null;
  }

  // Get icons for different notification types
  const getIcon = (type) => {
    switch (type) {
      case 'success':
        return <CheckCircleIcon className="h-6 w-6 text-accent-400" aria-hidden="true" />;
      case 'error':
        return <ExclamationCircleIcon className="h-6 w-6 text-danger-400" aria-hidden="true" />;
      case 'warning':
        return <ExclamationCircleIcon className="h-6 w-6 text-warning-400" aria-hidden="true" />;
      case 'info':
      default:
        return <InformationCircleIcon className="h-6 w-6 text-primary-400" aria-hidden="true" />;
    }
  };

  // Get background color for different notification types
  const getBackgroundColor = (type) => {
    switch (type) {
      case 'success':
        return 'bg-accent-50';
      case 'error':
        return 'bg-danger-50';
      case 'warning':
        return 'bg-warning-50';
      case 'info':
      default:
        return 'bg-primary-50';
    }
  };

  // Get text color for different notification types
  const getTextColor = (type) => {
    switch (type) {
      case 'success':
        return 'text-accent-800';
      case 'error':
        return 'text-danger-800';
      case 'warning':
        return 'text-warning-800';
      case 'info':
      default:
        return 'text-primary-800';
    }
  };

  return (
    <div
      aria-live="assertive"
      className="fixed inset-0 flex items-end px-4 py-6 pointer-events-none sm:p-6 sm:items-start z-50"
    >
      <div className="w-full flex flex-col items-center space-y-4 sm:items-end">
        {notifications.map((notification) => (
          <Transition
            key={notification.id}
            show={true}
            enter="transform ease-out duration-300 transition"
            enterFrom="translate-y-2 opacity-0 sm:translate-y-0 sm:translate-x-2"
            enterTo="translate-y-0 opacity-100 sm:translate-x-0"
            leave="transition ease-in duration-100"
            leaveFrom="opacity-100"
            leaveTo="opacity-0"
          >
            <div
              className={`max-w-sm w-full ${getBackgroundColor(
                notification.type
              )} shadow-lg rounded-lg pointer-events-auto ring-1 ring-black ring-opacity-5 overflow-hidden`}
            >
              <div className="p-4">
                <div className="flex items-start">
                  <div className="flex-shrink-0">
                    {getIcon(notification.type)}
                  </div>
                  <div className="ml-3 w-0 flex-1 pt-0.5">
                    <p className={`text-sm font-medium ${getTextColor(notification.type)}`}>
                      {notification.message}
                    </p>
                  </div>
                  <div className="ml-4 flex-shrink-0 flex">
                    <button
                      className="bg-transparent rounded-md inline-flex text-secondary-400 hover:text-secondary-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                      onClick={() => removeNotification(notification.id)}
                    >
                      <span className="sr-only">Close</span>
                      <XMarkIcon className="h-5 w-5" aria-hidden="true" />
                    </button>
                  </div>
                </div>
              </div>
            </div>
          </Transition>
        ))}
      </div>
    </div>
  );
};

export default Notifications;