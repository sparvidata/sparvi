// frontend/core/api.js
import axios from 'axios';
import AuthHandler from './auth/AuthHandler';

// Set the API base URL to your backend, e.g., http://localhost:5000
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:5000';

// Create an axios instance
const apiClient = axios.create({
  baseURL: API_BASE_URL
});

// Add a request interceptor to automatically add the token to requests
apiClient.interceptors.request.use(
  async (config) => {
    // Get the current token
    const token = await AuthHandler.getAccessToken();

    // If we have a token, add it to the request
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Add a response interceptor to handle token expiration
apiClient.interceptors.response.use(
  (response) => {
    return response;
  },
  async (error) => {
    const originalRequest = error.config;

    // If the error is a 401 (Unauthorized) and we haven't already tried to refresh
    if (error.response?.status === 401 && !originalRequest._retry) {
      originalRequest._retry = true;

      try {
        // Try to refresh the token
        const { data, error: refreshError } = await AuthHandler.refreshSession();

        if (refreshError) {
          // If refresh fails, redirect to login
          window.location.href = '/login';
          return Promise.reject(error);
        }

        // If refresh succeeds, retry the original request
        return apiClient(originalRequest);
      } catch (refreshError) {
        // If refresh fails, redirect to login
        window.location.href = '/login';
        return Promise.reject(error);
      }
    }

    // For other errors, just reject the promise
    return Promise.reject(error);
  }
);

// Export the loginUser function
export const loginUser = async (email, password) => {
  return await AuthHandler.signIn(email, password);
};

// Export the fetchProfile function
export const fetchProfile = async (connectionString, table) => {
  const response = await apiClient.get('/api/profile', {
    params: { connection_string: connectionString, table }
  });
  return response.data;
};

// Fetch all tables for a given connection string
export const fetchTables = async (connectionString) => {
  const response = await apiClient.get('/api/tables', {
    params: { connection_string: connectionString }
  });
  return response.data;
};

// Fetch validation rules for a table
export const fetchValidations = async (table) => {
  console.log("fetchValidations called with table:", table);
  try {
    const response = await apiClient.get('/api/validations', {
      params: { table }
    });
    console.log("fetchValidations response:", response.data);
    return response.data;
  } catch (error) {
    console.error("Error in fetchValidations:", error);
    throw error;
  }
};

// Add a new validation rule
export const addValidationRule = async (table, rule) => {
  const response = await apiClient.post('/api/validations', rule, {
    params: { table }
  });
  return response.data;
};

// Delete a validation rule
export const deleteValidationRule = async (table, ruleName) => {
  const response = await apiClient.delete('/api/validations', {
    params: { table, rule_name: ruleName }
  });
  return response.data;
};

// Run all validation rules for a table
export const runValidations = async (connectionString, table) => {
  console.log("API runValidations called with:", { connectionString, table });

  try {
    const response = await apiClient.post('/api/run-validations', {
      connection_string: connectionString,
      table
    });
    console.log("runValidations API response:", response.data);
    return response.data;
  } catch (error) {
    console.error("runValidations API error:", error);
    throw error;
  }
};

// Get validation history for a table
export const fetchValidationHistory = async (table, limit = 10) => {
  const response = await apiClient.get('/api/validation-history', {
    params: { table, limit }
  });
  return response.data;
};

// Generate and add default validation rules for a table
export const generateDefaultValidations = async (connectionString, table) => {
  console.log("API call with:", { connectionString, table });
  const response = await apiClient.post('/api/generate-default-validations', {
    connection_string: connectionString, // Make sure this matches what your backend expects
    table: table
  });
  return response.data;
};

export const fetchProfileHistory = async (table, limit = 10) => {
  const response = await apiClient.get('/api/profile-history', {
    params: { table, limit }
  });
  return response.data;
};

// Export the signOut function
export const signOut = async () => {
  return await AuthHandler.signOut();
};

// Export the default functions
export default {
  fetchProfile,
  fetchTables,
  fetchValidations,
  addValidationRule,
  deleteValidationRule,
  runValidations,
  fetchValidationHistory,
  generateDefaultValidations,
  fetchProfileHistory,
  loginUser,
  signOut
};