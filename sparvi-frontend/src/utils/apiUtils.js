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

// Rest of the file remains the same...