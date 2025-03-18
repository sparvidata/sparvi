import React from 'react';
import { Link } from 'react-router-dom';

const EmptyState = ({
  icon: Icon,
  title,
  description,
  actionText,
  actionLink,
  secondaryActionText,
  secondaryActionLink,
  onAction
}) => {
  return (
    <div className="py-4">
      <div className="text-center py-12 bg-white rounded-lg shadow">
        {Icon && <Icon className="mx-auto h-12 w-12 text-secondary-400" />}
        <h3 className="mt-2 text-sm font-medium text-secondary-900">{title}</h3>
        <p className="mt-1 text-sm text-secondary-500">
          {description}
        </p>
        <div className="mt-6 flex justify-center space-x-4">
          {actionLink ? (
            <Link
              to={actionLink}
              className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              {actionText}
            </Link>
          ) : onAction ? (
            <button
              onClick={onAction}
              className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              {actionText}
            </button>
          ) : null}

          {secondaryActionLink && (
            <Link
              to={secondaryActionLink}
              className="inline-flex items-center px-4 py-2 border border-secondary-300 shadow-sm text-sm font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              {secondaryActionText}
            </Link>
          )}
        </div>
      </div>
    </div>
  );
};

export default EmptyState;