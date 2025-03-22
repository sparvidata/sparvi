import { useMutation } from '@tanstack/react-query';
import { metadataAPI } from '../api/enhancedApiService';
import { queryClient } from '../api/queryClient';

/**
 * Custom hook for metadata operations (scheduling tasks, etc)
 */
export const useMetadataOperations = (connectionId) => {
  // Schedule metadata task
  const scheduleTask = useMutation({
    mutationFn: ({ taskType, tableName, priority = 'medium' }) =>
      metadataAPI.scheduleTask(connectionId, taskType, tableName, priority),
    onSuccess: () => {
      // Invalidate metadata status query to get updated task list
      queryClient.invalidateQueries(['metadata-status', connectionId]);
    }
  });

  // Detect schema changes
  const detectChanges = useMutation({
    mutationFn: () => metadataAPI.detectChanges(connectionId),
    onSuccess: () => {
      // Invalidate metadata status query
      queryClient.invalidateQueries(['metadata-status', connectionId]);
      // Invalidate tables query as schema may have changed
      queryClient.invalidateQueries(['schema-tables', connectionId]);
    }
  });

  // Schedule full collection
  const collectMetadata = useMutation({
    mutationFn: (options) => metadataAPI.collectMetadata(connectionId, options),
    onSuccess: () => {
      // Invalidate metadata status query
      queryClient.invalidateQueries(['metadata-status', connectionId]);
    }
  });

  return {
    scheduleTask,
    detectChanges,
    collectMetadata
  };
};