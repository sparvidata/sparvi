import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { HomeIcon, ChevronRightIcon } from '@heroicons/react/20/solid';
import { useUI } from '../../contexts/UIContext';

const Breadcrumbs = () => {
  const { breadcrumbs } = useUI();
  const location = useLocation();

  // If custom breadcrumbs are provided, use those
  if (breadcrumbs && breadcrumbs.length > 0) {
    return (
      <nav className="flex" aria-label="Breadcrumb">
        <ol className="flex items-center space-x-2">
          <li>
            <div>
              <Link to="/" className="text-secondary-400 hover:text-secondary-500">
                <HomeIcon className="flex-shrink-0 h-5 w-5" aria-hidden="true" />
                <span className="sr-only">Home</span>
              </Link>
            </div>
          </li>
          {breadcrumbs.map((breadcrumb, index) => (
            <li key={breadcrumb.href || index}>
              <div className="flex items-center">
                <ChevronRightIcon
                  className="flex-shrink-0 h-5 w-5 text-secondary-400"
                  aria-hidden="true"
                />
                {breadcrumb.href ? (
                  <Link
                    to={breadcrumb.href}
                    className="ml-2 text-sm font-medium text-secondary-500 hover:text-secondary-700"
                    aria-current={index === breadcrumbs.length - 1 ? 'page' : undefined}
                  >
                    {breadcrumb.name}
                  </Link>
                ) : (
                  <span className="ml-2 text-sm font-medium text-secondary-500">
                    {breadcrumb.name}
                  </span>
                )}
              </div>
            </li>
          ))}
        </ol>
      </nav>
    );
  }

  // Otherwise, generate breadcrumbs based on current path
  const paths = location.pathname.split('/').filter(Boolean);

  // If no paths (we're at root), just show home
  if (paths.length === 0) {
    return (
      <nav className="flex" aria-label="Breadcrumb">
        <ol className="flex items-center space-x-2">
          <li>
            <div>
              <Link to="/" className="text-secondary-400 hover:text-secondary-500">
                <HomeIcon className="flex-shrink-0 h-5 w-5" aria-hidden="true" />
                <span className="sr-only">Home</span>
              </Link>
            </div>
          </li>
        </ol>
      </nav>
    );
  }

  // Generate breadcrumbs based on current path
  return (
    <nav className="flex" aria-label="Breadcrumb">
      <ol className="flex items-center space-x-2">
        <li>
          <div>
            <Link to="/" className="text-secondary-400 hover:text-secondary-500">
              <HomeIcon className="flex-shrink-0 h-5 w-5" aria-hidden="true" />
              <span className="sr-only">Home</span>
            </Link>
          </div>
        </li>
        {paths.map((path, index) => {
          // Convert path to readable format (capitalize, replace hyphens with spaces)
          const readablePath = path
            .split('-')
            .map(word => word.charAt(0).toUpperCase() + word.slice(1))
            .join(' ');

          // Build the href for this breadcrumb
          const href = `/${paths.slice(0, index + 1).join('/')}`;

          // Check if this is the last breadcrumb
          const isLast = index === paths.length - 1;

          return (
            <li key={path}>
              <div className="flex items-center">
                <ChevronRightIcon
                  className="flex-shrink-0 h-5 w-5 text-secondary-400"
                  aria-hidden="true"
                />
                {!isLast ? (
                  <Link
                    to={href}
                    className="ml-2 text-sm font-medium text-secondary-500 hover:text-secondary-700"
                  >
                    {readablePath}
                  </Link>
                ) : (
                  <span
                    className="ml-2 text-sm font-medium text-secondary-500"
                    aria-current="page"
                  >
                    {readablePath}
                  </span>
                )}
              </div>
            </li>
          );
        })}
      </ol>
    </nav>
  );
};

export default Breadcrumbs;