// frontend/src/profile-api.js - updated to handle Snowflake connections
import axios from 'axios';
import { supabase } from './lib/supabase';

// Set the API base URL
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:5000';

// Get the current session token
const getToken = async () => {
  try {
    console.log("Getting auth session...");
    const { data } = await supabase.auth.getSession();
    const session = data.session;

    if (!session) {
      console.log("No session found, attempting to refresh...");
      // Try to refresh the session
      const { data: refreshData } = await supabase.auth.refreshSession();
      const refreshedSession = refreshData.session;

      if (refreshedSession) {
        console.log("Session refreshed successfully");
        return refreshedSession.access_token;
      } else {
        console.log("No session after refresh attempt");
        return null;
      }
    }

    // Check if token is close to expiry (within 5 minutes)
    const expiresAt = session.expires_at * 1000; // Convert to milliseconds
    const now = Date.now();
    const fiveMinutesInMs = 5 * 60 * 1000;

    if (expiresAt - now < fiveMinutesInMs) {
      console.log("Token expiring soon, refreshing...");
      const { data: refreshData } = await supabase.auth.refreshSession();
      const refreshedSession = refreshData.session;

      if (refreshedSession) {
        console.log("Session refreshed successfully");
        return refreshedSession.access_token;
      }
    }

    console.log("Using existing token");
    return session.access_token;
  } catch (error) {
    console.error("Error getting token:", error);
    return null;
  }
};

// Sanitize connection string for logging - don't display passwords
const sanitizeConnectionString = (connectionString) => {
  if (!connectionString) return '';

  try {
    // If it contains a password, replace it with asterisks
    if (connectionString.includes('@') && connectionString.includes(':')) {
      // Extract parts of the connection string
      const [protocol, rest] = connectionString.split('://');
      const [auth, hostPart] = rest.split('@');

      // If auth contains username:password
      if (auth.includes(':')) {
        const [username, password] = auth.split(':');
        // Replace password with asterisks
        return `${protocol}://${username}:******@${hostPart}`;
      }
    }
    return connectionString;
  } catch (e) {
    // If any error occurs during parsing, return masked version
    return connectionString.replace(/:[^@:]+@/, ':******@');
  }
};

// Export a direct fetch profile function that doesn't rely on interceptors
export const directFetchProfile = async (connectionString, tableName) => {
  const safeConnString = sanitizeConnectionString(connectionString);
  console.log('directFetchProfile called with:', { connectionString: safeConnString, tableName });

  try {
    // Get the token directly
    const token = await getToken();

    console.log(`Token available: ${!!token}`);
    if (!token) {
      console.error('No authentication token available');
      throw new Error('Authentication required');
    }

    // Make the request with explicit token
    console.log(`Making GET request to ${API_BASE_URL}/api/profile`, {
      params: {
        connection_string: connectionString, // Original connection string for backend
        table: tableName
      },
      headers: { Authorization: `Bearer ${token}` }
    });

    const response = await axios.get(`${API_BASE_URL}/api/profile`, {
      params: {
        connection_string: connectionString,
        table: tableName
      },
      headers: { Authorization: `Bearer ${token}` }
    });

    console.log('Profile API response received');
    return response.data;
  } catch (error) {
    console.error('Profile API error:', error);
    if (error.response) {
      // The request was made and the server responded with a status code
      // that falls out of the range of 2xx
      console.error('Error data:', error.response.data);
      console.error('Error status:', error.response.status);
      console.error('Error headers:', error.response.headers);
    } else if (error.request) {
      // The request was made but no response was received
      console.error('No response received:', error.request);
    } else {
      // Something happened in setting up the request that triggered an Error
      console.error('Request setup error:', error.message);
    }
    throw error;
  }
};

// Export a direct fetch tables function with Snowflake support
export const directFetchTables = async (connectionString) => {
  const safeConnString = sanitizeConnectionString(connectionString);
  console.log('directFetchTables called with:', safeConnString);

  try {
    // Get the token directly
    const token = await getToken();

    console.log(`Token available for tables request: ${!!token}`);
    if (!token) {
      console.error('No authentication token available');
      throw new Error('Authentication required');
    }

    // Make the request with explicit token
    console.log(`Making GET request to ${API_BASE_URL}/api/tables`, {
      params: { connection_string: connectionString }, // Original connection string for backend
      headers: { Authorization: `Bearer ${token}` }
    });

    const response = await axios.get(`${API_BASE_URL}/api/tables`, {
      params: { connection_string: connectionString },
      headers: { Authorization: `Bearer ${token}` }
    });

    console.log('Tables API response:', response.data);
    return response.data;
  } catch (error) {
    console.error('Tables API error:', error);
    if (error.response) {
      console.error('Error data:', error.response.data);
      console.error('Error status:', error.response.status);
    } else if (error.request) {
      console.error('No response received:', error.request);
    } else {
      console.error('Request setup error:', error.message);
    }
    throw error;
  }
};