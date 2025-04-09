import { useQuery } from '@tanstack/react-query';
import { schemaAPI } from '../api/enhancedApiService';

let hasLogged = false; // This ensures the log only runs once per session

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
      if (!hasLogged) {
        console.log("Tables data received:", data);
        hasLogged = true;
      }

      // Handle different response structures
      if (data?.data?.tables) {
        return data.data.tables;
      } else if (data?.tables) {
        return data.tables;
      } else if (Array.isArray(data)) {
        return data;
      } else if (data && typeof data === 'object') {
        // Find the first array property
        for (const key in data) {
          if (Array.isArray(data[key])) {
            return data[key];
          }
        }
      }

      // Default empty array
      return [];
    }
  });
};
