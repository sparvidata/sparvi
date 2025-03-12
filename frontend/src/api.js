import axios from 'axios';
import AuthHandler from './auth/AuthHandler';

const HOST = process.env.REACT_APP_API_BASE_URL || 'localhost:5000';
const API_BASE_URL = HOST.includes('://') ? HOST : `https://${HOST}`;

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
export const runValidations = async (connectionString, table, profileHistoryId = null) => {
  console.log("API runValidations called with:", {
    connectionString: connectionString ? "[CONNECTION STRING PRESENT]" : "[MISSING]",
    table,
    profileHistoryId
  });

  try {
    const response = await apiClient.post('/api/run-validations', {
      connection_string: connectionString,
      table,
      profile_history_id: profileHistoryId
    });
    console.log("runValidations API response:", response.data);
    return response.data;
  } catch (err) {
    console.error("runValidations API error:", err);
    throw err;
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

export const fetchProfileHistory = async (tableName, limit = 15) => {
  console.log(`[API] fetchProfileHistory called with table: ${tableName}, limit: ${limit}`);

  try {
    // Use the correct endpoint with query parameters
    const response = await apiClient.get(`/api/profile-history`, {
      params: {
        table: tableName,
        limit: limit
      }
    });

    console.log(`[API] fetchProfileHistory response:`, response.data);
    return response.data;
  } catch (error) {
    console.error(`[API] fetchProfileHistory error:`, error);
    throw error;
  }
};

// Export the signOut function
export const signOut = async () => {
  return await AuthHandler.signOut();
};

// Update an existing validation rule
export const updateValidationRule = async (table, ruleId, rule) => {
  const response = await apiClient.put(`/api/validations/${ruleId}`, rule, {
    params: { table }
  });
  return response.data;
};

// Get all users in the organization
export const fetchAdminUsers = async () => {
  const response = await apiClient.get('/api/admin/users');
  return response.data.users;
};

// Update a user's profile
export const updateAdminUser = async (userId, userData) => {
  const response = await apiClient.put(`/api/admin/users/${userId}`, userData);
  return response.data;
};

// Invite a new user to the organization
export const inviteUser = async (inviteData) => {
  const response = await apiClient.post('/api/admin/users', inviteData);
  return response.data;
};

// Remove a user from the organization
export const removeUser = async (userId) => {
  const response = await apiClient.delete(`/api/admin/users/${userId}`);
  return response.data;
};

// Get organization details
export const fetchOrganization = async () => {
  const response = await apiClient.get('/api/admin/organization');
  return response.data.organization;
};

// Update organization details
export const updateOrganization = async (orgData) => {
  const response = await apiClient.put('/api/admin/organization', orgData);
  return response.data;
};

export const fetchDataPreview = async (connectionString, tableName, maxRows = 50) => {
  console.log('Fetching data preview for:', { connectionString, tableName, maxRows });

  try {
    const response = await apiClient.get('/api/preview', {
      params: {
        connection_string: connectionString,
        table: tableName,
        max_rows: maxRows
      }
    });

    console.log('Preview data response:', response.data);
    return response.data;
  } catch (error) {
    console.error('Error fetching preview data:', error);
    if (error.response) {
      console.error('Error details:', error.response.data);
    }
    throw error;
  }
};

// Update the organization settings function
export const updateOrganizationSettings = async (orgData) => {
  const response = await apiClient.put('/api/admin/organization', orgData);
  return response.data;
};

// Add a function to get preview settings
export const getPreviewSettings = async () => {
  const response = await apiClient.get('/api/admin/organization/preview-settings');
  return response.data;
};

// Add a function to update preview settings
export const updatePreviewSettings = async (previewSettings) => {
  const response = await apiClient.put('/api/admin/organization/preview-settings', previewSettings);
  return response.data;
};

// Add these functions to your existing api.js file

// Get all connections for current organization
export const fetchConnections = async () => {
  const response = await apiClient.get('/api/connections');
  return response.data;
};

// Create a new connection
export const createConnection = async (connectionData) => {
  const response = await apiClient.post('/api/connections', connectionData);
  return response.data;
};

// Update an existing connection
export const updateConnection = async (id, connectionData) => {
  const response = await apiClient.put(`/api/connections/${id}`, connectionData);
  return response.data;
};

// Delete a connection
export const deleteConnection = async (id) => {
  const response = await apiClient.delete(`/api/connections/${id}`);
  return response.data;
};

// Test a connection
export const testConnection = async (connectionData) => {
  const response = await apiClient.post('/api/connections/test', connectionData);
  return response.data;
};

// Set a connection as default
export const setDefaultConnection = async (id) => {
  const response = await apiClient.put(`/api/connections/${id}/default`);
  return response.data;
};

export const fetchConnectionById = async (connectionId) => {
  console.log(`[API] Fetching connection details for ID: ${connectionId}`);
  try {
    const response = await apiClient.get(`/api/connections/${connectionId}`);
    console.log(`[API] Connection details response:`, response.data);
    return response.data;
  } catch (error) {
    console.error(`[API] Error fetching connection details:`, error);
    throw error;
  }
};

// Export the default functions
export default {
  fetchProfile,
  fetchTables,
  fetchValidations,
  addValidationRule,
  deleteValidationRule,
  runValidations,
  updateValidationRule,
  fetchValidationHistory,
  generateDefaultValidations,
  fetchProfileHistory,
  loginUser,
  signOut,
  fetchAdminUsers,
  updateAdminUser,
  inviteUser,
  removeUser,
  fetchOrganization,
  updateOrganization,
  fetchDataPreview,
  updateOrganizationSettings,
  getPreviewSettings,
  updatePreviewSettings,
  fetchConnections,
  createConnection,
  updateConnection,
  deleteConnection,
  testConnection,
  setDefaultConnection
};