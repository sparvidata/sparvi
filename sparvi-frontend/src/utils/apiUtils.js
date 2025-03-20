import axios from 'axios';
import { getSession } from '../api/supabase';

const apiCallTracker = {};
const THROTTLE_WINDOW_MS = 15000; // 15 second throttle window

// Create an authenticated API client
export const createApiClient = async () => {
  try {
    // Get auth token
    const session = await getSession();
    const token = session?.access_token;

    // Determine the API URL, prioritizing environment variables
    const apiUrl = process.env.REACT_APP_API_BASE_URL ||
      (process.env.NODE_ENV === 'development'
        ? 'http://127.0.0.1:5000/api'
        : '/api');

    console.log("API Client Creation Details:", {
      token: token ? 'Token Present' : 'No Token',
      apiUrl,
      nodeEnv: process.env.NODE_ENV,
      envApiBaseUrl: process.env.REACT_APP_API_BASE_URL
    });

    if (!token) {
      console.error('No authentication token available for API request');
      throw new Error('Authentication required');
    }

    // Create and return axios instance with auth
    return axios.create({
      baseURL: apiUrl,
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      timeout: 60000
    });
  } catch (error) {
    console.error('Error creating API client:', error);
    throw error;
  }
};

/**
 * Make an authenticated API request
 * @param {string} endpoint - API endpoint (without leading slash)
 * @param {Object} options - Request options
 * @returns {Promise} Promise resolving to response data
 */
export const apiRequest = async (endpoint, options = {}) => {
  const {
    method = 'GET',
    data = null,
    params = null,
    timeout = 60000, // 60 seconds default timeout
    skipThrottle = false // Add option to skip throttle
  } = options;

  // Check throttling unless explicitly skipped
  if (!skipThrottle) {
    const now = Date.now();
    const key = `${method}:${endpoint}`;
    const lastCall = apiCallTracker[key];

    if (lastCall && now - lastCall < THROTTLE_WINDOW_MS) {
      console.log(`Throttling request to ${endpoint} - too recent`);
      return Promise.reject({
        throttled: true,
        message: 'Request throttled - too frequent'
      });
    }

    // Track this call
    apiCallTracker[key] = now;
  }

  try {
    console.log(`Making ${method} request to ${endpoint}`);
    const client = await createApiClient();

    // Add these new debug logging lines
    const session = await getSession();
    const token = session?.access_token;
    console.log(`Auth token available for ${endpoint}:`, !!token);

    // Override client timeout if specified
    if (timeout !== 60000) {
      client.defaults.timeout = timeout;
    }

    const response = await client({
      method,
      url: endpoint,
      data,
      params
    });

    console.log(`Response from ${endpoint}:`, response.data);
    return response.data;
  } catch (error) {
    console.error(`API request failed for ${endpoint}:`, error);
    throw error;
  }
};

export { apiRequest };  // Named export
export default apiRequest;  // Default export