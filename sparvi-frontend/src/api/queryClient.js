// src/api/queryClient.js
import { QueryClient } from '@tanstack/react-query';

// Create a client with default settings
export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Default settings for all queries
      staleTime: 5 * 60 * 1000, // 5 minutes before data is considered stale
      cacheTime: 30 * 60 * 1000, // Cache data for 30 minutes (even after component unmounts)
      refetchOnWindowFocus: false, // Don't refetch when window regains focus
      retry: 1, // Only retry failed queries once
      retryDelay: 3000, // Wait 3 seconds between retries
    },
  },
});

// Helper to invalidate queries by prefix
export const invalidateQueries = (queryKeyPrefix) => {
  return queryClient.invalidateQueries({ queryKey: [queryKeyPrefix] });
};

// Helper to reset the cache for the entire application
export const resetQueryCache = () => {
  return queryClient.resetQueries();
};