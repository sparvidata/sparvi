/**
 * Utility functions for API request management
 */
import axios from 'axios';
import { waitForAuth } from '../api/enhancedApiService';

/**
 * Map to store active request AbortControllers
 * Used to cancel in-flight requests when needed
 */
const activeRequestControllers = new Map();

/**
 * Create an AbortController and token for a request
 * @param {string} requestId - Unique identifier for the request
 * @returns {Object} Object containing signal for the request
 */
export const getRequestAbortController = (requestId) => {
  // Cancel any existing request with the same ID
  cancelRequest(requestId);

  // Create a new AbortController
  const controller = new AbortController();
  activeRequestControllers.set(requestId, controller);

  return {
    signal: controller.signal
  };
};

/**
 * Cancel a specific request
 * @param {string} requestId - ID of the request to cancel
 */
export const cancelRequest = (requestId) => {
  const controller = activeRequestControllers.get(requestId);
  if (controller) {
    controller.abort();
    activeRequestControllers.delete(requestId);
  }
};

/**
 * Cancel all active requests or those matching a prefix
 * @param {string} prefix - Optional prefix to limit cancellation to specific request IDs
 */
export const cancelRequests = (prefix = '') => {
  for (const [requestId, controller] of activeRequestControllers.entries()) {
    if (!prefix || requestId.startsWith(prefix)) {
      controller.abort();
      activeRequestControllers.delete(requestId);
    }
  }
};

/**
 * Clean up completed request controllers
 * @param {string} requestId - ID of the completed request
 */
export const requestCompleted = (requestId) => {
  activeRequestControllers.delete(requestId);
};

/**
 * Create a debounced function that delays invoking func until after wait milliseconds
 * @param {Function} func - The function to debounce
 * @param {number} wait - The number of milliseconds to delay
 * @returns {Function} The debounced function
 */
export const debounce = (func, wait = 300) => {
  let timeout;

  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };

    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
};

/**
 * Create a throttled function that only invokes func at most once per every wait milliseconds
 * @param {Function} func - The function to throttle
 * @param {number} wait - The number of milliseconds to throttle invocations to
 * @returns {Function} The throttled function
 */
export const throttle = (func, wait = 300) => {
  let waiting = false;

  return function executedFunction(...args) {
    if (waiting) return;

    func(...args);
    waiting = true;
    setTimeout(() => {
      waiting = false;
    }, wait);
  };
};

/**
 * Batch multiple API requests into a single call with retry logic
 * @param {Array} requests - Array of request configurations
 * @param {Object} options - Options for batch request
 * @returns {Promise} Promise that resolves with all responses
 */
export const batchRequests = async (requests, options = {}) => {
  const {
    endpoint = '/batch',
    retries = 2,
    retryDelay = 1000,
    waitForAuthentication = true
  } = options;

  // If requested, wait for authentication to be ready
  if (waitForAuthentication) {
    try {
      await waitForAuth();
    } catch (error) {
      console.warn('Proceeding with batch request without waiting for auth');
    }
  }

  let attempt = 0;

  while (attempt <= retries) {
    try {
      const apiUrl = process.env.NODE_ENV === 'development'
        ? 'http://127.0.0.1:5000/api'
        : '';

      // Use the full URL in development, relative in production
      const batchEndpoint = `${apiUrl}${endpoint}`;
      console.log("Making batch request to:", batchEndpoint);

      const response = await axios.post(batchEndpoint, { requests });
      return response.data.results;
    } catch (error) {
      // Don't retry canceled requests
      if (axios.isCancel(error)) {
        console.log("Batch request was canceled");
        throw { cancelled: true };
      }

      attempt++;

      // If we've exhausted retries, throw the error
      if (attempt > retries || error.response?.status !== 401) {
        console.error('Error in batch request:', error);
        throw error;
      }

      // If error is 401 and we have retries left, wait and retry
      console.log(`Auth error in batch request, retrying (${attempt}/${retries})...`);
      await new Promise(resolve => setTimeout(resolve, retryDelay));
    }
  }
};

/**
 * Create a paginated data fetcher
 * @param {Function} fetchFunction - The function to call for fetching data
 * @param {Object} options - Options for pagination
 * @returns {Object} Object with pagination methods and state
 */
export const createPagination = (fetchFunction, options = {}) => {
  const {
    initialPage = 1,
    pageSize = 20,
    initialData = [],
  } = options;

  let currentPage = initialPage;
  let hasMore = true;
  let isLoading = false;
  let data = [...initialData];
  let totalCount = 0;

  // Function to fetch a specific page
  const fetchPage = async (page) => {
    if (isLoading) return;

    isLoading = true;
    try {
      const result = await fetchFunction({
        page,
        pageSize,
      });

      // If result is array, use it directly, otherwise extract data
      const newData = Array.isArray(result) ? result : (result.data || []);
      totalCount = result.totalCount || newData.length;

      // If this is the first page, replace data, otherwise append
      if (page === 1) {
        data = newData;
      } else {
        data = [...data, ...newData];
      }

      // Check if there are more pages
      hasMore = newData.length === pageSize;
      currentPage = page;

      return {
        data,
        page: currentPage,
        hasMore,
        totalCount,
      };
    } catch (error) {
      console.error('Error fetching page:', error);
      throw error;
    } finally {
      isLoading = false;
    }
  };

  // Function to fetch the next page
  const fetchNextPage = async () => {
    if (!hasMore || isLoading) return null;
    return await fetchPage(currentPage + 1);
  };

  // Function to reset and fetch the first page
  const reset = async () => {
    currentPage = 0;
    hasMore = true;
    data = [];
    return await fetchNextPage();
  };

  return {
    fetchPage,
    fetchNextPage,
    reset,
    getCurrentPage: () => currentPage,
    getData: () => data,
    hasMore: () => hasMore,
    isLoading: () => isLoading,
    getTotalCount: () => totalCount,
  };
};