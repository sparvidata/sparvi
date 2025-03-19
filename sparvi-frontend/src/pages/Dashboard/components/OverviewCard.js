import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Link } from 'react-router-dom';
import { ArrowRightIcon } from '@heroicons/react/20/solid';
import { useUI } from '../../../contexts/UIContext';
import { apiRequest } from '../../../utils/apiUtils';
import LoadingSpinner from '../../../components/common/LoadingSpinner';

const OverviewCard = ({ title, type = 'tables', connectionId }) => {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const { showNotification } = useUI();

  // Use refs to track component state and prevent unnecessary fetches
  const isMountedRef = useRef(true);
  const lastFetchRef = useRef(0);
  const dataLoadedRef = useRef(false);
  const FETCH_INTERVAL_MS = 30000; // 30 seconds minimum between fetches

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      isMountedRef.current = false;
    };
  }, []);

  // Create memoized load function with fetch throttling
  const fetchData = useCallback(async (force = false) => {
    if (!connectionId) return;

    // Check if we need to fetch again - avoid too frequent refreshes
    const now = Date.now();
    if (!force && now - lastFetchRef.current < FETCH_INTERVAL_MS) {
      console.log(`Skipping ${type} overview fetch, too soon since last fetch`);
      return;
    }

    // Skip if already loaded unless forced
    if (!force && dataLoadedRef.current) {
      return;
    }

    try {
      setLoading(true);
      setError(null);
      lastFetchRef.current = now;
      console.log(`Loading ${type} overview data for connection`, connectionId);

      // Add a small delay to prevent request conflicts
      await new Promise(resolve => setTimeout(resolve, 100));

      if (!isMountedRef.current) return;

      if (type === 'tables') {
        const response = await apiRequest(`connections/${connectionId}/tables`, {
          skipThrottle: force // Skip throttling for manual refreshes
        });

        if (isMountedRef.current) {
          console.log(`Setting ${type} overview data:`, response);
          // Only show first 5 tables
          setData((response?.tables || []).slice(0, 5));
          dataLoadedRef.current = true;
        }
      } else if (type === 'validations') {
        // First get tables
        const tablesResponse = await apiRequest(`connections/${connectionId}/tables`, {
          skipThrottle: force // Skip throttling for manual refreshes
        });
        const tables = tablesResponse?.tables || [];

        if (!isMountedRef.current) return;

        if (tables.length > 0) {
          // Get validation rules for the first table
          const validationResponse = await apiRequest('validations', {
            params: { table: tables[0] },
            skipThrottle: force // Skip throttling for manual refreshes
          });

          if (isMountedRef.current) {
            console.log(`Setting ${type} overview data:`, validationResponse);
            setData((validationResponse?.rules || []).slice(0, 5));
            dataLoadedRef.current = true;
          }
        } else {
          if (isMountedRef.current) {
            setData([]);
            dataLoadedRef.current = true;
          }
        }
      }
    } catch (err) {
      if (!isMountedRef.current) return;

      if (err.throttled) {
        console.log(`${type} overview request throttled`);
        return;
      }

      if (err.cancelled) return;

      console.error(`Error fetching ${type}:`, err);
      setError(err);
      showNotification(`Failed to load ${type} data`, 'error');
    } finally {
      if (isMountedRef.current) {
        setLoading(false);
      }
    }
  }, [connectionId, type, showNotification]);

  // Load data only when dependencies change and not loaded yet
  useEffect(() => {
    if (connectionId && !dataLoadedRef.current) {
      fetchData();
    } else if (!connectionId) {
      setData([]);
      setLoading(false);
      dataLoadedRef.current = false;
    }

    // Reset loaded state when connection changes
    if (connectionId) {
      return () => {
        dataLoadedRef.current = false;
      };
    }
  }, [connectionId, fetchData]);

  const getLink = () => {
    switch (type) {
      case 'tables':
        return connectionId ? `/explorer?connection=${connectionId}` : '/explorer';
      case 'validations':
        return '/validations';
      default:
        return '/dashboard';
    }
  };

  return (
    <div className="card overflow-hidden">
      <div className="px-4 py-5 sm:px-6 flex justify-between items-center bg-white border-b border-secondary-200">
        <h3 className="text-lg leading-6 font-medium text-secondary-900">{title} Overview</h3>
        <Link
          to={getLink()}
          className="text-sm font-medium text-primary-600 hover:text-primary-500 flex items-center"
        >
          View all
          <ArrowRightIcon className="ml-1 h-4 w-4" aria-hidden="true" />
        </Link>
      </div>
      <div className="bg-white px-4 py-5 sm:p-6">
        {loading ? (
          <div className="space-y-3">
            {[...Array(3)].map((_, i) => (
              <div key={i} className="animate-pulse flex space-x-4">
                <div className="rounded-full bg-secondary-200 h-10 w-10"></div>
                <div className="flex-1 space-y-2 py-1">
                  <div className="h-4 bg-secondary-200 rounded w-3/4"></div>
                  <div className="h-4 bg-secondary-200 rounded w-1/2"></div>
                </div>
              </div>
            ))}
          </div>
        ) : data.length === 0 ? (
          <div className="text-center py-6">
            <p className="text-sm text-secondary-500">No {type} found</p>
            <Link
              to={getLink()}
              className="mt-2 inline-flex items-center px-3 py-1.5 border border-transparent text-xs font-medium rounded-md text-primary-700 bg-primary-100 hover:bg-primary-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              Add {type === 'tables' ? 'a table' : 'a validation'}
            </Link>
          </div>
        ) : (
          <ul className="divide-y divide-secondary-200">
            {data.map((item, index) => (
              <li key={index} className="py-3">
                {type === 'tables' ? (
                  <Link
                    to={`/explorer/${connectionId}/tables/${item}`}
                    className="flex justify-between items-center hover:bg-secondary-50 px-2 py-1 rounded-md"
                  >
                    <span className="text-sm font-medium text-secondary-900">{item}</span>
                    <ArrowRightIcon className="h-4 w-4 text-secondary-400" aria-hidden="true" />
                  </Link>
                ) : (
                  <Link
                    to={`/validations/${item.id}`}
                    className="flex justify-between items-center hover:bg-secondary-50 px-2 py-1 rounded-md"
                  >
                    <div>
                      <span className="text-sm font-medium text-secondary-900">{item.rule_name}</span>
                      <p className="text-xs text-secondary-500 truncate">{item.description}</p>
                    </div>
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      item.last_result ? 'bg-accent-100 text-accent-800' : 'bg-danger-100 text-danger-800'
                    }`}>
                      {item.last_result ? 'Passed' : 'Failed'}
                    </span>
                  </Link>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
};

export default OverviewCard;