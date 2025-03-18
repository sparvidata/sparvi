import React, { useState } from 'react';
import { EyeIcon, EyeSlashIcon } from '@heroicons/react/24/outline';

const PostgreSQLConnectionForm = ({ details, onChange, errors = {} }) => {
  const [showPassword, setShowPassword] = useState(false);

  // Handle input changes
  const handleChange = (e) => {
    const { name, value } = e.target;
    onChange({
      ...details,
      [name]: value
    });
  };

  // Toggle password visibility
  const togglePasswordVisibility = () => {
    setShowPassword(!showPassword);
  };

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-y-6 gap-x-4 sm:grid-cols-6">
        <div className="sm:col-span-4">
          <label htmlFor="host" className="block text-sm font-medium text-secondary-700">
            Host
          </label>
          <input
            type="text"
            name="host"
            id="host"
            value={details.host || ''}
            onChange={handleChange}
            className={`mt-1 focus:ring-primary-500 focus:border-primary-500 block w-full shadow-sm sm:text-sm border-secondary-300 rounded-md ${
              errors.host ? 'border-danger-300' : ''
            }`}
            placeholder="localhost or db.example.com"
          />
          {errors.host && (
            <p className="mt-2 text-sm text-danger-600">{errors.host}</p>
          )}
        </div>

        <div className="sm:col-span-2">
          <label htmlFor="port" className="block text-sm font-medium text-secondary-700">
            Port
          </label>
          <input
            type="text"
            name="port"
            id="port"
            value={details.port || '5432'}
            onChange={handleChange}
            className={`mt-1 focus:ring-primary-500 focus:border-primary-500 block w-full shadow-sm sm:text-sm border-secondary-300 rounded-md ${
              errors.port ? 'border-danger-300' : ''
            }`}
            placeholder="5432"
          />
          {errors.port && (
            <p className="mt-2 text-sm text-danger-600">{errors.port}</p>
          )}
        </div>

        <div className="sm:col-span-3">
          <label htmlFor="database" className="block text-sm font-medium text-secondary-700">
            Database
          </label>
          <input
            type="text"
            name="database"
            id="database"
            value={details.database || ''}
            onChange={handleChange}
            className={`mt-1 focus:ring-primary-500 focus:border-primary-500 block w-full shadow-sm sm:text-sm border-secondary-300 rounded-md ${
              errors.database ? 'border-danger-300' : ''
            }`}
            placeholder="postgres"
          />
          {errors.database && (
            <p className="mt-2 text-sm text-danger-600">{errors.database}</p>
          )}
        </div>

        <div className="sm:col-span-3">
          <label htmlFor="schema" className="block text-sm font-medium text-secondary-700">
            Schema
          </label>
          <input
            type="text"
            name="schema"
            id="schema"
            value={details.schema || ''}
            onChange={handleChange}
            className={`mt-1 focus:ring-primary-500 focus:border-primary-500 block w-full shadow-sm sm:text-sm border-secondary-300 rounded-md ${
              errors.schema ? 'border-danger-300' : ''
            }`}
            placeholder="public"
          />
          {errors.schema && (
            <p className="mt-2 text-sm text-danger-600">{errors.schema}</p>
          )}
          <p className="mt-2 text-xs text-secondary-500">
            Optional. Defaults to public if not specified
          </p>
        </div>

        <div className="sm:col-span-3">
          <label htmlFor="username" className="block text-sm font-medium text-secondary-700">
            Username
          </label>
          <input
            type="text"
            name="username"
            id="username"
            value={details.username || ''}
            onChange={handleChange}
            className={`mt-1 focus:ring-primary-500 focus:border-primary-500 block w-full shadow-sm sm:text-sm border-secondary-300 rounded-md ${
              errors.username ? 'border-danger-300' : ''
            }`}
            placeholder="postgres"
          />
          {errors.username && (
            <p className="mt-2 text-sm text-danger-600">{errors.username}</p>
          )}
        </div>

        <div className="sm:col-span-3">
          <label htmlFor="password" className="block text-sm font-medium text-secondary-700">
            Password
          </label>
          <div className="mt-1 relative rounded-md shadow-sm">
            <input
              type={showPassword ? 'text' : 'password'}
              name="password"
              id="password"
              value={details.password || ''}
              onChange={handleChange}
              className={`focus:ring-primary-500 focus:border-primary-500 block w-full pr-10 sm:text-sm border-secondary-300 rounded-md ${
                errors.password ? 'border-danger-300' : ''
              }`}
              placeholder="••••••••"
            />
            <button
              type="button"
              onClick={togglePasswordVisibility}
              className="absolute inset-y-0 right-0 pr-3 flex items-center text-secondary-400 hover:text-secondary-500 focus:outline-none"
            >
              {showPassword ? (
                <EyeSlashIcon className="h-5 w-5" aria-hidden="true" />
              ) : (
                <EyeIcon className="h-5 w-5" aria-hidden="true" />
              )}
            </button>
          </div>
          {errors.password && (
            <p className="mt-2 text-sm text-danger-600">{errors.password}</p>
          )}
        </div>

        <div className="sm:col-span-6">
          <div className="flex items-start">
            <div className="flex items-center h-5">
              <input
                id="use_ssl"
                name="useSSL"
                type="checkbox"
                checked={details.useSSL === undefined ? true : details.useSSL}
                onChange={(e) => handleChange({
                  target: {
                    name: 'useSSL',
                    value: e.target.checked
                  }
                })}
                className="focus:ring-primary-500 h-4 w-4 text-primary-600 border-secondary-300 rounded"
              />
            </div>
            <div className="ml-3 text-sm">
              <label htmlFor="use_ssl" className="font-medium text-secondary-700">
                Use SSL
              </label>
              <p className="text-secondary-500">
                Connect to PostgreSQL using SSL (recommended for security)
              </p>
            </div>
          </div>
        </div>

        <div className="sm:col-span-6">
          <div className="flex items-start">
            <div className="flex items-center h-5">
              <input
                id="use_env_vars"
                name="useEnvVars"
                type="checkbox"
                checked={details.useEnvVars || false}
                onChange={(e) => handleChange({
                  target: {
                    name: 'useEnvVars',
                    value: e.target.checked
                  }
                })}
                className="focus:ring-primary-500 h-4 w-4 text-primary-600 border-secondary-300 rounded"
              />
            </div>
            <div className="ml-3 text-sm">
              <label htmlFor="use_env_vars" className="font-medium text-secondary-700">
                Use environment variables
              </label>
              <p className="text-secondary-500">
                Use credentials from environment variables instead of storing them directly.
                Prefix will be "POSTGRES_" by default (e.g., POSTGRES_USERNAME).
              </p>
            </div>
          </div>

          {details.useEnvVars && (
            <div className="mt-3">
              <label htmlFor="env_var_prefix" className="block text-sm font-medium text-secondary-700">
                Environment Variable Prefix
              </label>
              <input
                type="text"
                name="envVarPrefix"
                id="env_var_prefix"
                value={details.envVarPrefix || 'POSTGRES'}
                onChange={handleChange}
                className="mt-1 focus:ring-primary-500 focus:border-primary-500 block w-full shadow-sm sm:text-sm border-secondary-300 rounded-md"
                placeholder="POSTGRES"
              />
              <p className="mt-2 text-xs text-secondary-500">
                The prefix for environment variables (e.g., POSTGRES_USERNAME, POSTGRES_PASSWORD)
              </p>
            </div>
          )}
        </div>
      </div>

      <div className="pt-5 border-t border-secondary-200">
        <h4 className="text-sm font-medium text-secondary-900">Advanced Options</h4>

        <div className="mt-4 grid grid-cols-1 gap-y-6 gap-x-4 sm:grid-cols-6">
          <div className="sm:col-span-3">
            <label htmlFor="connectionTimeout" className="block text-sm font-medium text-secondary-700">
              Connection Timeout (Seconds)
            </label>
            <input
              type="number"
              name="connectionTimeout"
              id="connectionTimeout"
              value={details.connectionTimeout || 10}
              onChange={handleChange}
              className="mt-1 focus:ring-primary-500 focus:border-primary-500 block w-full shadow-sm sm:text-sm border-secondary-300 rounded-md"
              placeholder="10"
              min="1"
            />
          </div>

          <div className="sm:col-span-3">
            <label htmlFor="statementTimeout" className="block text-sm font-medium text-secondary-700">
              Statement Timeout (Seconds)
            </label>
            <input
              type="number"
              name="statementTimeout"
              id="statementTimeout"
              value={details.statementTimeout || 300}
              onChange={handleChange}
              className="mt-1 focus:ring-primary-500 focus:border-primary-500 block w-full shadow-sm sm:text-sm border-secondary-300 rounded-md"
              placeholder="300"
              min="0"
            />
            <p className="mt-2 text-xs text-secondary-500">
              Maximum time in seconds a query can run before timing out
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default PostgreSQLConnectionForm;