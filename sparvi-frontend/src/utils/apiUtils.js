import axios from 'axios';
import { getSession } from '../api/supabase';

// Create an authenticated API client
export const createApiClient = async () => {
  try {
    // Get auth token
    const session = await getSession();
    const token = session?.access_token;

    if (!token) {
      console.error('No authentication token available for API request');
      throw new Error('Authentication required');
    }

    // Define API URL
    const apiUrl = process.env.NODE_ENV === 'development'
      ? 'http://127.0.0.1:5000/api'
      : '/api';

    // Create and return axios instance with auth
    return axios.create({
      baseURL: apiUrl,
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      timeout: 60000 // 60 seconds timeout - doubled from previous 30s
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
// src/utils/apiUtils.js - Add debug logging
export const apiRequest = async (endpoint, options = {}) => {
  const {
    method = 'GET',
    data = null,
    params = null,
    timeout = 60000 // 60 seconds default timeout
  } = options;

  try {
    console.log(`Making ${method} request to ${endpoint}`); // Add debug log
    const client = await createApiClient();

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

    console.log(`Response from ${endpoint}:`, response.data); // Add debug log
    return response.data;
  } catch (error) {
    console.error(`API request failed for ${endpoint}:`, error);
    throw error;
  }
};