import axios from 'axios';
import { getSession } from '../api/supabase';

/**
 * A simplified function for batching multiple API requests
 * @param {Array} requests - Array of request objects with id and path properties
 * @param {Object} options - Optional configuration
 * @returns {Object} Object with results keyed by request id
 */
export const simpleBatchRequest = async (requests, options = {}) => {
  const { timeout = 30000 } = options;

  try {
    // Get auth token
    const session = await getSession();
    const token = session?.access_token;

    if (!token) {
      console.error('No authentication token available for batch request');
      throw new Error('Authentication required');
    }

    // Define API URL
    const apiUrl = process.env.NODE_ENV === 'development'
      ? 'http://127.0.0.1:5000/api'
      : '/api';

    console.log(`Making batch request with ${requests.length} requests`);

    // Make the batch request
    const response = await axios.post(
      `${apiUrl}/batch`,
      { requests },
      {
        headers: { 'Authorization': `Bearer ${token}` },
        timeout
      }
    );

    return response.data.results || {};
  } catch (error) {
    console.error('Batch request failed:', error);
    throw error;
  }
};

/**
 * Simplified component for loading data that depends on connections
 * @param {string} connectionId - The ID of the current connection
 * @param {Array} requests - Request configurations to batch
 * @returns {Promise} Promise resolving to batch results
 */
export const loadConnectionData = async (connectionId, requests = []) => {
  if (!connectionId) {
    console.warn('No connection ID provided for loadConnectionData');
    return {};
  }

  // Add connectionId to any requests that need it
  const preparedRequests = requests.map(req => {
    // Clone the request to avoid mutating the original
    const newReq = { ...req };

    // Add params object if it doesn't exist
    if (!newReq.params) {
      newReq.params = {};
    }

    // Add connectionId to params if not already there
    if (!newReq.params.connection_id && !newReq.params.connectionId) {
      newReq.params.connection_id = connectionId;
    }

    return newReq;
  });

  return simpleBatchRequest(preparedRequests);
};