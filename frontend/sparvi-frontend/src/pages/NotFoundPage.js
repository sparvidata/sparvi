import React from 'react';
import { Link } from 'react-router-dom';
import { HomeIcon } from '@heroicons/react/24/outline';

const NotFoundPage = () => {
  return (
    <div className="min-h-full flex flex-col justify-center py-12 sm:px-6 lg:px-8">
      <div className="mt-8 sm:mx-auto sm:w-full sm:max-w-md">
        <div className="text-center">
          <h1 className="text-9xl font-extrabold text-primary-600">404</h1>
          <h2 className="mt-4 text-3xl font-bold text-secondary-900">Page not found</h2>
          <p className="mt-2 text-base text-secondary-500">
            Sorry, we couldn't find the page you're looking for.
          </p>
          <div className="mt-6">
            <Link to="/" className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500">
              <HomeIcon className="-ml-1 mr-2 h-5 w-5" aria-hidden="true" />
              Go back home
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
};

export default NotFoundPage;