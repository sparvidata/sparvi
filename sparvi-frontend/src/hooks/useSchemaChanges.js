import { useState, useEffect, useCallback } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { schemaAPI } from '../api/enhancedApiService';
import { useUI } from '../contexts/UIContext';

export const useSchemaChanges = (connectionId, options = {}) => {
  const { showNotification } = useUI();
  const queryClient = useQueryClient();
  const [acknowledgedFilter, setAcknowledgedFilter] = useState('all'); // Default to 'all'
  const { enabled = !!connectionId } = options;

  // Fetch schema changes from the database
  const {
    data: changes,
    isLoading,
    error,
    refetch
  } = useQuery({
    queryKey: ['schema-changes', connectionId, acknowledgedFilter], // Keep this to refresh when filter changes
    queryFn: () => schemaAPI.getChanges(connectionId, { acknowledged: acknowledgedFilter }),
    enabled: enabled,
    select: (data) => {
      // Normalize the data format
      if (data?.changes) {
        return data.changes;
      }
      if (Array.isArray(data)) {
        return data;
      }
      return [];
    },
    refetchInterval: 300000, // Refetch every 5 minutes
  });

  // Detect schema changes
  const detectChanges = useMutation({
    mutationFn: () => schemaAPI.detectChanges(connectionId),
    onSuccess: (data) => {
      // Invalidate schema changes query to refresh data
      queryClient.invalidateQueries(['schema-changes', connectionId]);

      if (data.changes_detected > 0) {
        showNotification(
          `Detected ${data.changes_detected} schema ${data.changes_detected === 1 ? 'change' : 'changes'}`,
          'warning'
        );
      } else {
        showNotification('No schema changes detected', 'info');
      }
    },
    onError: (error) => {
      console.error('Error detecting schema changes:', error);
      showNotification('Failed to detect schema changes', 'error');
    }
  });

  // Acknowledge changes
  const acknowledgeChanges = useMutation({
    mutationFn: (tableName) => schemaAPI.acknowledgeChanges(connectionId, tableName),
    onSuccess: (data) => {
      queryClient.invalidateQueries(['schema-changes', connectionId]);
      showNotification(
        `Acknowledged ${data.acknowledged_count} schema ${data.acknowledged_count === 1 ? 'change' : 'changes'}`,
        'success'
      );
    },
    onError: (error) => {
      console.error('Error acknowledging schema changes:', error);
      showNotification('Failed to acknowledge changes', 'error');
    }
  });

  return {
    changes: changes || [],
    isLoading,
    error,
    detectChanges: detectChanges.mutate,
    isDetecting: detectChanges.isPending,
    acknowledgeChanges: (tableName) => acknowledgeChanges.mutate(tableName),
    isAcknowledging: acknowledgeChanges.isPending,
    refetch,
    setAcknowledgedFilter, // Export this function to control the filter
    acknowledgedFilter     // Export current filter state
  };
};