import axios from 'axios';
import { getSession } from './supabase';

// Create a base API client with defaults
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:5000/api';

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add request interceptor to include auth token
apiClient.interceptors.request.use(
  async (config) => {
    const session = await getSession();
    if (session?.access_token) {
      config.headers.Authorization = `Bearer ${session.access_token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Add response interceptor for error handling
apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    // Handle specific error codes
    if (error.response) {
      if (error.response.status === 401) {
        // Handle unauthorized errors (e.g., redirect to login)
        console.error('Unauthorized access. Redirecting to login...');
        // You might want to dispatch an action or use a context here
      }

      if (error.response.status === 403) {
        console.error('Forbidden access. You do not have permission to access this resource.');
      }
    }

    return Promise.reject(error);
  }
);

// Connections API
export const connectionsAPI = {
  getAll: () => apiClient.get('/connections'),
  getById: (id) => apiClient.get(`/connections/${id}`),
  create: (data) => apiClient.post('/connections', data),
  update: (id, data) => apiClient.put(`/connections/${id}`, data),
  delete: (id) => apiClient.delete(`/connections/${id}`),
  test: (data) => apiClient.post('/connections/test', data),
  setDefault: (id) => apiClient.put(`/connections/${id}/default`),
};

// Tables and Schema API
export const schemaAPI = {
  getTables: (connectionId) => apiClient.get(`/connections/${connectionId}/tables`),
  getColumns: (connectionId, tableName) =>
    apiClient.get(`/connections/${connectionId}/tables/${tableName}/columns`),
  getPreview: (connectionId, tableName, maxRows = 50) =>
    apiClient.get(`/connections/${connectionId}/tables/${tableName}/preview`, { params: { max_rows: maxRows }}),
  getStatistics: (connectionId, tableName) =>
    apiClient.get(`/connections/${connectionId}/tables/${tableName}/statistics`),
  detectChanges: (connectionId) =>
    apiClient.post(`/connections/${connectionId}/schema/detect-changes`),
  getChanges: (connectionId, since) =>
    apiClient.get(`/connections/${connectionId}/changes`, { params: { since }}),
};

// Profiling API
export const profilingAPI = {
  getProfile: (connectionId, tableName) =>
    apiClient.get('/profile', { params: { connection_id: connectionId, table: tableName }}),
  getHistory: (tableName, limit = 10) =>
    apiClient.get('/profile-history', { params: { table: tableName, limit }}),
  getTrends: (connectionId, tableName, days = 30) =>
    apiClient.get(`/connections/${connectionId}/tables/${tableName}/trends`, { params: { days }}),
};

// Validations API
export const validationsAPI = {
  getRules: (tableName) => apiClient.get('/validations', { params: { table: tableName }}),
  createRule: (tableName, rule) => apiClient.post('/validations', rule, { params: { table: tableName }}),
  updateRule: (ruleId, tableName, rule) =>
    apiClient.put(`/validations/${ruleId}`, rule, { params: { table: tableName }}),
  deleteRule: (tableName, ruleName) =>
    apiClient.delete('/validations', { params: { table: tableName, rule_name: ruleName }}),
  runValidations: (connectionId, tableName, connectionString) =>
    apiClient.post('/run-validations', {
      connection_id: connectionId,
      table: tableName,
      connection_string: connectionString
    }),
  getValidationHistory: (profileId) => apiClient.get(`/validation-history/${profileId}`),
  generateDefaultValidations: (connectionId, tableName, connectionString) =>
    apiClient.post('/generate-default-validations', {
      connection_id: connectionId,
      table: tableName,
      connection_string: connectionString
    }),
};

// Metadata API
export const metadataAPI = {
  getMetadata: (connectionId, type = 'tables') =>
    apiClient.get(`/connections/${connectionId}/metadata`, { params: { type }}),
  collectMetadata: (connectionId, options = {}) =>
    apiClient.post(`/connections/${connectionId}/metadata/collect`, options),
  refreshMetadata: (connectionId, metadataType, tableName) =>
    apiClient.post(`/connections/${connectionId}/metadata/refresh`, {
      metadata_type: metadataType,
      table_name: tableName
    }),
  getMetadataStatus: (connectionId) =>
    apiClient.get(`/connections/${connectionId}/metadata/status`),
  getTasks: (connectionId, limit = 10) =>
    apiClient.get(`/connections/${connectionId}/metadata/tasks`, { params: { limit }}),
  scheduleTask: (connectionId, taskType, tableName, priority = 'medium') =>
    apiClient.post(`/connections/${connectionId}/metadata/tasks`, {
      task_type: taskType,
      table_name: tableName,
      priority
    }),
  getTaskStatus: (connectionId, taskId) =>
    apiClient.get(`/connections/${connectionId}/metadata/tasks/${taskId}`),
};

// Admin API
export const adminAPI = {
  getUsers: () => apiClient.get('/admin/users'),
  updateUser: (userId, userData) => apiClient.put(`/admin/users/${userId}`, userData),
  inviteUser: (userData) => apiClient.post('/admin/users', userData),
  removeUser: (userId) => apiClient.delete(`/admin/users/${userId}`),
  getOrganization: () => apiClient.get('/admin/organization'),
  updateOrganization: (orgData) => apiClient.put('/admin/organization', orgData),
  getWorkerStats: () => apiClient.get('/metadata/worker/stats'),
};

// User Setup API
export const userAPI = {
  setupUser: (userData) => apiClient.post('/setup-user', userData),
};

// Analytics API
export const analyticsAPI = {
  getChangeFrequency: (connectionId, objectType, objectName, days = 30) =>
    apiClient.get(`/connections/${connectionId}/analytics/change-frequency`, {
      params: { object_type: objectType, object_name: objectName, days }
    }),
  getRefreshSuggestion: (connectionId, objectType, objectName, currentInterval = 24) =>
    apiClient.get(`/connections/${connectionId}/analytics/refresh-suggestion`, {
      params: {
        object_type: objectType,
        object_name: objectName,
        current_interval: currentInterval
      }
    }),
  getHighImpactObjects: (connectionId, limit = 10) =>
    apiClient.get(`/connections/${connectionId}/analytics/high-impact`, {
      params: { limit }
    }),
  getDashboard: (connectionId) =>
    apiClient.get(`/connections/${connectionId}/analytics/dashboard`),
};

export default apiClient;