import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { metadataAPI } from '../api/enhancedApiService';

/**
 * Custom hook to fetch metadata status
 * @param {string} connectionId - The connection ID to fetch status for
 * @param {Object} options - Additional options for the query
 * @returns {Object} Query result object
 */
export const useMetadataStatus = (connectionId, options = {}) => {
  const {
    enabled = !!connectionId,
    refetchInterval = connectionId ? 30000 : false, // Poll every 30 seconds if we have a connectionId
    ...queryOptions
  } = options;

  return useQuery({
    queryKey: ['metadata-status', connectionId],
    queryFn: () => metadataAPI.getMetadataStatus(connectionId, { forceFresh: true }),
    enabled: enabled,
    refetchInterval: refetchInterval,
    ...queryOptions,
    // If tasks are pending, we want to poll more frequently
    refetchIntervalInBackground: true,
    // Transform the data to handle different API response formats
    select: (data) => {
      // console.log("Metadata status received:", data);

      // If data has a nested data property, use that
      const responseData = data?.data || data;

      return {
        tables: responseData?.tables || {},
        columns: responseData?.columns || {},
        statistics: responseData?.statistics || {},
        pending_tasks: responseData?.pending_tasks || [],
        changes: responseData?.changes || [],
        changes_detected: responseData?.changes_detected || 0
      };
    }
  });
};

/**
 * Custom hook to refresh metadata
 * @param {string} connectionId - The connection ID to refresh metadata for
 * @returns {Object} Mutation result object
 */
export const useRefreshMetadata = (connectionId) => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (metadataType = 'full') =>
      metadataAPI.refreshMetadata(connectionId, metadataType),
    onSuccess: () => {
      // Invalidate metadata status query to trigger a refetch
      queryClient.invalidateQueries(['metadata-status', connectionId]);

      // After a short delay, also invalidate tables and related queries
      // This gives the backend time to process the initial request
      setTimeout(() => {
        queryClient.invalidateQueries(['schema-tables', connectionId]);
        queryClient.invalidateQueries(['connection-dashboard', connectionId]);
      }, 2000);
    }
  });
};