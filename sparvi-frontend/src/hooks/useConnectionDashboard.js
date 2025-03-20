// src/hooks/useConnectionDashboard.js
import { useQuery } from '@tanstack/react-query';
import { connectionsAPI } from '../api/enhancedApiService';

/**
 * Custom hook to fetch connection dashboard data
 * @param {string} connectionId - The connection ID to fetch data for
 * @param {Object} options - Additional options for the query
 * @returns {Object} Query result object
 */
export const useConnectionDashboard = (connectionId, options = {}) => {
  const {
    enabled = !!connectionId,
    refetchInterval = false,
    ...queryOptions
  } = options;

  return useQuery({
    queryKey: ['connection-dashboard', connectionId],
    queryFn: () => connectionsAPI.getConnectionDashboard(connectionId, { forceFresh: false }),
    enabled: enabled,
    refetchInterval: refetchInterval,
    ...queryOptions,
    select: (data) => {
      // Transform the API response if needed
      // This is where you can normalize or restructure data
      return data;
    },
    // Don't refetch on window focus for dashboard data - it doesn't change that frequently
    refetchOnWindowFocus: false,
    // Keep the data for 30 minutes
    cacheTime: 30 * 60 * 1000,
    // Consider it stale after 10 minutes, meaning new mounts will refetch
    staleTime: 10 * 60 * 1000,
  });
};