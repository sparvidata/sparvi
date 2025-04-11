import { useState, useEffect, useCallback } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { schemaAPI } from '../api/enhancedApiService';
import { useUI } from '../contexts/UIContext';

export const useSchemaChanges = (connectionId, options = {}) => {
  const { showNotification } = useUI();
  const queryClient = useQueryClient();
  const { days = 30, enabled = !!connectionId } = options;

  // Format the date to ISO string (what the API expects)
  const getFormattedDate = (daysAgo) => {
    const date = new Date();
    date.setDate(date.getDate() - daysAgo);
    return date.toISOString();
  };

  // Fetch schema changes
  const {
    data: changes,
    isLoading,
    error,
    refetch
  } = useQuery({
    queryKey: ['schema-changes', connectionId, days],
    queryFn: () => {
      // Calculate the date from X days ago in the format the API expects
      const sinceDate = getFormattedDate(days);
      return schemaAPI.getChanges(connectionId, sinceDate);
    },
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
    mutationFn: (tableNames) => {
      // In a real implementation, this would call an API
      // For MVP, we'll just simulate acknowledging changes
      return Promise.resolve({ success: true, acknowledged: tableNames });
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries(['schema-changes', connectionId]);
      showNotification('Changes acknowledged successfully', 'success');
    }
  });

  // Helper to handle acknowledgement
  const handleAcknowledge = useCallback((tableName) => {
    acknowledgeChanges.mutate(tableName);
  }, [acknowledgeChanges]);

  return {
    changes: changes || [],
    isLoading,
    error,
    detectChanges: detectChanges.mutate,
    isDetecting: detectChanges.isPending,
    acknowledgeChanges: handleAcknowledge,
    isAcknowledging: acknowledgeChanges.isPending,
    refetch
  };
};