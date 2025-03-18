import React, { useState } from 'react';
import { EyeIcon, EyeSlashIcon } from '@heroicons/react/24/outline';

const SnowflakeConnectionForm = ({ details, onChange, errors = {} }) => {
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
        <div className="sm:col-span-3">
          <label htmlFor="account" className="block text-sm font-medium text-secondary-700">
            Account
          </label>
          <input
            type="text"
            name="account"
            id="account"
            value={details.account || ''}
            onChange={handleChange}
            className={`mt-1 focus:ring-primary-500 focus:border-primary-500 block w-full shadow-sm sm:text-sm border-secondary-300 rounded-md ${
              errors.account ? 'border-danger-300' : ''
            }`}
            placeholder="your-account"
          />
          {errors.account && (
            <p className="mt-2 text-sm text-danger-600">{errors.account}</p>
          )}
          <p className="mt-2 text-xs text-secondary-500">
            Your Snowflake account identifier (e.g., xy12345.us-east-1)
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
            placeholder="username"
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

        <div className="sm:col-span-3">
          <label htmlFor="warehouse" className="block text-sm font-medium text-secondary-700">
            Warehouse
          </label>
          <input
            type="text"
            name="warehouse"
            id="warehouse"
            value={details.warehouse || ''}
            onChange={handleChange}
            className={`mt-1 focus:ring-primary-500 focus:border-primary-500 block w-full shadow-sm sm:text-sm border-secondary-300 rounded-md ${
              errors.warehouse ? 'border-danger-300' : ''
            }`}
            placeholder="COMPUTE_WH"
          />
          {errors.warehouse && (
            <p className="mt-2 text-sm text-danger-600">{errors.warehouse}</p>
          )}
          <p className="mt-2 text-xs text-secondary-500">
            The Snowflake warehouse to use for queries
          </p>
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
            placeholder="ANALYTICS"
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
            placeholder="PUBLIC"
          />
          {errors.schema && (
            <p className="mt-2 text-sm text-danger-600">{errors.schema}</p>
          )}
          <p className="mt-2 text-xs text-secondary-500">
            Optional. Defaults to PUBLIC if not specified
          </p>
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
                Prefix will be "SNOWFLAKE_" by default (e.g., SNOWFLAKE_USERNAME).
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
                value={details.envVarPrefix || 'SNOWFLAKE'}
                onChange={handleChange}
                className="mt-1 focus:ring-primary-500 focus:border-primary-500 block w-full shadow-sm sm:text-sm border-secondary-300 rounded-md"
                placeholder="SNOWFLAKE"
              />
              <p className="mt-2 text-xs text-secondary-500">
                The prefix for environment variables (e.g., SNOWFLAKE_USERNAME, SNOWFLAKE_PASSWORD)
              </p>
            </div>
          )}
        </div>
      </div>

      <div className="pt-5 border-t border-secondary-200">
        <h4 className="text-sm font-medium text-secondary-900">Advanced Options</h4>

        <div className="mt-4 grid grid-cols-1 gap-y-6 gap-x-4 sm:grid-cols-6">
          <div className="sm:col-span-3">
            <label htmlFor="role" className="block text-sm font-medium text-secondary-700">
              Role (Optional)
            </label>
            <input
              type="text"
              name="role"
              id="role"
              value={details.role || ''}
              onChange={handleChange}
              className="mt-1 focus:ring-primary-500 focus:border-primary-500 block w-full shadow-sm sm:text-sm border-secondary-300 rounded-md"
              placeholder="ACCOUNTADMIN"
            />
          </div>

          <div className="sm:col-span-3">
            <label htmlFor="timeout" className="block text-sm font-medium text-secondary-700">
              Query Timeout (Seconds)
            </label>
            <input
              type="number"
              name="timeout"
              id="timeout"
              value={details.timeout || 300}
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

export default SnowflakeConnectionForm;