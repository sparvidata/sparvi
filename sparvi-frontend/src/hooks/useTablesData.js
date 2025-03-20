// src/hooks/useTablesData.js
import { useQuery } from '@tanstack/react-query';
import { schemaAPI } from '../api/enhancedApiService';

/**
 * Custom hook to fetch tables data for a connection
 * @param {string} connectionId - The connection ID to fetch tables for
 * @param {Object} options - Additional options for the query
 * @returns {Object} Query result object
 */
export const useTablesData = (connectionId, options = {}) => {
  const {
    enabled = !!connectionId,
    refetchInterval = false,
    ...queryOptions
  } = options;

  return useQuery({
    queryKey: ['schema-tables', connectionId],
    queryFn: () => schemaAPI.getTables(connectionId, { forceFresh: false }),
    enabled: enabled,
    refetchInterval: refetchInterval,
    ...queryOptions,
    select: (data) => {
      // Handle various response formats from the API
      console.log("Tables data received:", data);

      // Handle different response structures
      if (data?.data?.tables) {
        return data.data.tables;
      }
      else if (data?.tables) {
        return data.tables;
      }
      else if (Array.isArray(data)) {
        return data;
      }
      // Last resort - look through the entire object for an array
      else if (data && typeof data === 'object') {
        // Find the first array property
        for (const key in data) {
          if (Array.isArray(data[key])) {
            return data[key];
          }
        }
      }

      // Default empty array
      return [];
    },
    // Keep the data for 30 minutes
    cacheTime: 30 * 60 * 1000,
    // Consider it stale after 5 minutes
    staleTime: 5 * 60 * 1000,
  });
};