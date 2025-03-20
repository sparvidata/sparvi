// src/api/queryClient.js
import { QueryClient } from '@tanstack/react-query';

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 10 * 60 * 1000, // Increase to 10 minutes
      cacheTime: 60 * 60 * 1000, // Cache for 1 hour even after unmounting
      refetchOnWindowFocus: false,
      retry: 1,
      retryDelay: 3000,
      refetchOnMount: false, // Don't refetch when component mounts if data exists
      refetchOnReconnect: false, // Don't refetch on reconnection
    },
  },
});

// Helper to prefetch query data (use for critical paths)
export const prefetchQuery = async (queryKey, queryFn) => {
  return queryClient.prefetchQuery({
    queryKey,
    queryFn,
    staleTime: 10 * 60 * 1000,
  });
};

// Helper to invalidate queries by prefix
export const invalidateQueries = (queryKeyPrefix) => {
  return queryClient.invalidateQueries({ queryKey: [queryKeyPrefix] });
};

// Helper to reset the cache for the entire application
export const resetQueryCache = () => {
  return queryClient.resetQueries();
};