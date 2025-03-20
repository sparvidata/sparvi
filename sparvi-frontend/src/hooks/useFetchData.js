import { useQuery } from '@tanstack/react-query';

/**
 * Generic data fetching hook with standardized error handling and response normalization
 */
export function useFetchData(queryKey, fetchFn, options = {}) {
  const {
    // Default options with longer staleTime
    enabled = true,
    staleTime = 10 * 60 * 1000, // 10 minutes
    cacheTime = 60 * 60 * 1000, // 1 hour
    normalizer = (data) => data,
    onSuccess,
    onError,
    ...restOptions
  } = options;

  return useQuery({
    queryKey,
    queryFn: async () => {
      try {
        const response = await fetchFn();
        return normalizer(response);
      } catch (error) {
        console.error(`Error fetching ${queryKey.join('.')}:`, error);
        throw error;
      }
    },
    enabled,
    staleTime,
    cacheTime,
    onSuccess,
    onError,
    ...restOptions,
  });
}