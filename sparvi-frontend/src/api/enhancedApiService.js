import axios from 'axios';
import { getSession } from './supabase';
import { getCacheItem, setCacheItem, clearCacheItem } from '../utils/cacheUtils';
import { getRequestAbortController, requestCompleted } from '../utils/requestUtils';

(async () => {
  try {
    console.log("Checking auth session structure...");
    const session = await getSession();
    console.log("Session available:", !!session);
    if (session) {
      // Log a safe version of the session without exposing full tokens
      const safeSession = { ...session };
      if (safeSession.access_token) safeSession.access_token = safeSession.access_token.substring(0, 10) + '...';
      if (safeSession.token) safeSession.token = safeSession.token.substring(0, 10) + '...';
      if (safeSession.accessToken) safeSession.accessToken = safeSession.accessToken.substring(0, 10) + '...';
      if (safeSession.user?.token) safeSession.user.token = safeSession.user.token.substring(0, 10) + '...';
      if (safeSession.data?.access_token) safeSession.data.access_token = safeSession.data.access_token.substring(0, 10) + '...';

      console.log("Session structure:", safeSession);
      console.log("Token property paths to check:",
        "access_token:", !!session.access_token,
        "token:", !!session.token,
        "accessToken:", !!session.accessToken,
        "user.token:", !!(session.user && session.user.token),
        "data.access_token:", !!(session.data && session.data.access_token)
      );
    }
  } catch (e) {
    console.error("Error checking session:", e);
  }
})();

// Create a base API client with defaults
const API_BASE_URL = process.env.NODE_ENV === 'development'
  ? 'http://localhost:5000/api'  // Local development
  : process.env.REACT_APP_API_BASE_URL || 'https://sparvi-backend.onrender.com/api';

console.log("Full environment details:", {
  NODE_ENV: process.env.NODE_ENV,
  REACT_APP_API_BASE_URL: process.env.REACT_APP_API_BASE_URL,
  COMPUTED_API_BASE_URL: API_BASE_URL
});

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Create an auth ready state
let authReady = false;
let authReadyPromise = null;
let authReadyResolve = null;

// Initialize the auth ready promise
const initAuthReadyPromise = () => {
  if (!authReadyPromise) {
    authReadyPromise = new Promise(resolve => {
      authReadyResolve = resolve;
    });
  }
  return authReadyPromise;
};

// Function to check if auth is ready
export const waitForAuth = async (timeoutMs = 5000) => {
  if (authReady) return true;

  // Initialize the promise if it doesn't exist
  const promise = initAuthReadyPromise();

  // Add timeout
  const timeoutPromise = new Promise((_, reject) => {
    setTimeout(() => reject(new Error('Auth ready timeout')), timeoutMs);
  });

  try {
    await Promise.race([promise, timeoutPromise]);
    return true;
  } catch (error) {
    console.warn('Timed out waiting for auth to be ready');
    return false;
  }
};

// Function to set auth as ready
export const setAuthReady = () => {
  authReady = true;
  if (authReadyResolve) {
    authReadyResolve(true);
  }
};

// Check auth on module load and set it ready if possible
(async () => {
  try {
    const session = await getSession();
    if (session?.access_token) {
      setAuthReady();
      console.log("Auth initialized with valid token");
    } else {
      console.log("No valid auth token found on initialization");
    }
  } catch (e) {
    console.error("Error initializing auth:", e);
  }
})();

const debugAuth = async () => {
  try {
    const session = await getSession();
    console.log("Auth check: Session exists:", !!session);

    // Check different possible token locations
    const token = session?.access_token ||
                 session?.token ||
                 session?.accessToken ||
                 (session?.user?.token) ||
                 (session?.data?.access_token);

    console.log("Auth check: Token exists:", !!token);
    if (token) {
      console.log("Auth check: Token preview:", `${token.substring(0, 10)}...`);
    } else {
      // If no token found, log the session structure to help debug
      console.log("Auth check: Session structure:", JSON.stringify(session, null, 2));
    }
    return !!token;
  } catch (e) {
    console.error("Error in debugAuth:", e);
    return false;
  }
};

apiClient.interceptors.request.use(
  async (config) => {
    try {
      const session = await getSession();
      // The token is likely in session.access_token
      if (session?.access_token) {
        config.headers.Authorization = `Bearer ${session.access_token}`;
      }
    } catch (err) {
      console.error('Error adding auth token to request:', err);
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Add response interceptor for error handling
apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    // Don't treat cancelled requests as errors
    if (axios.isCancel(error)) {
    // Silently handle canceled requests by default
    // Use a debug flag for logging if needed
    const isDevelopment = (typeof process !== 'undefined') &&
                         (process.env) &&
                         (process.env.NODE_ENV === 'development');

    if (isDevelopment) {
      console.debug('Request cancelled:', error.message);
    }
    return Promise.reject({ cancelled: true });
  }

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

/**
 * Enhanced API request function with caching, abort control, and error handling
 * @param {Object} options - Request options
 * @returns {Promise} Promise resolving to the response data
 */
const enhancedRequest = async (options) => {
  const {
    method = 'GET',
    url,
    data,
    params,
    cacheKey,
    cacheTTL = 5 * 60 * 1000, // 5 minutes default
    forceFresh = false,
    requestId,
    abortSignal,
    onUploadProgress,
    onDownloadProgress,
    timeout = 60000 // 60 seconds timeout
  } = options;

  // Check cache first if this is a GET request and caching is enabled
  if (method === 'GET' && cacheKey && !forceFresh) {
    const cachedData = getCacheItem(cacheKey);
    if (cachedData) {
      return cachedData;
    }
  }

  // Setup abort controller if needed
  const controller = requestId ? getRequestAbortController(requestId) : new AbortController();
  const signal = abortSignal || (controller ? controller.signal : undefined);

  // Set a timeout
  const timeoutId = setTimeout(() => {
    if (controller && !controller.signal.aborted) {
      console.log(`Request timeout (${timeout}ms) for: ${url}`, { requestId });
      controller.abort();
    }
  }, timeout);

  try {
    // Log when making batch requests
    if (url === '/batch') {
      console.log(`Making batch request to: ${API_BASE_URL}${url}`, {
        method,
        requestId
      });
    }

    const requestConfig = {
      method,
      url,
      data,
      params,
      signal,
      onUploadProgress,
      onDownloadProgress,
      timeout // Add explicit timeout to axios request
    };

    const response = await apiClient(requestConfig);

    // Cache the response if this is a GET request and caching is enabled
    if (method === 'GET' && cacheKey) {
      setCacheItem(cacheKey, response.data, { ttl: cacheTTL });
    }

    // Clear the abort controller
    if (requestId) {
      requestCompleted(requestId);
    }

    return response.data;
  } catch (error) {
    // If it's a cancelled request, just rethrow
    if (axios.isCancel(error) || error.cancelled) {
      throw { cancelled: true, originalError: error };
    }

    // Log details about the error for debugging
    console.error(`Error in ${method} request to ${url}:`, {
      message: error.message,
      status: error.response?.status,
      data: error.response?.data,
      requestId
    });

    // Clear the abort controller
    if (requestId) {
      requestCompleted(requestId);
    }

    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
};

// Enhanced Connections API
export const connectionsAPI = {
  getAll: (options = {}) => {
    const { forceFresh = false, requestId = 'connections.getAll' } = options;
    return enhancedRequest({
      url: '/connections',
      cacheKey: 'connections.list',
      requestId,
      forceFresh
    }).then(response => {
      // Handle different response formats
      if (response && response.data && response.data.connections) {
        return response; // Already in correct format
      } else if (response && response.connections) {
        // Wrap in data property for consistency
        return { data: { connections: response.connections } };
      } else if (Array.isArray(response)) {
        // Response is array, wrap in expected format
        return { data: { connections: response } };
      } else {
        // Fallback for unexpected response format
        console.warn('Unexpected API response format from connections endpoint', response);
        return { data: { connections: [] } };
      }
    }).catch(error => {
      // Handle canceled requests
      if (error && error.cancelled) {
        throw error; // Just re-throw cancelled errors
      }

      console.error('Error fetching connections:', error);
      // Return valid but empty response
      return { data: { connections: [] } };
    });
  },

  getById: (id, options = {}) => {
    const { forceFresh = false, requestId = `connections.get.${id}` } = options;
    return enhancedRequest({
      url: `/connections/${id}`,
      cacheKey: `connections.${id}`,
      requestId,
      forceFresh
    });
  },

  create: (data) => {
    return enhancedRequest({
      method: 'POST',
      url: '/connections',
      data,
      requestId: 'connections.create'
    }).then(response => {
      // Invalidate connections list cache on create
      clearCacheItem('connections.list');
      return response;
    });
  },

  update: (id, data) => {
    return enhancedRequest({
      method: 'PUT',
      url: `/connections/${id}`,
      data,
      requestId: `connections.update.${id}`
    }).then(response => {
      // Invalidate affected cache items
      clearCacheItem(`connections.${id}`);
      clearCacheItem('connections.list');
      return response;
    });
  },

  delete: (id) => {
    return enhancedRequest({
      method: 'DELETE',
      url: `/connections/${id}`,
      requestId: `connections.delete.${id}`
    }).then(response => {
      // Invalidate affected cache items
      clearCacheItem(`connections.${id}`);
      clearCacheItem('connections.list');
      return response;
    });
  },

  test: (data) => {
    return enhancedRequest({
      method: 'POST',
      url: '/connections/test',
      data,
      requestId: 'connections.test'
    });
  },

  setDefault: (id) => {
    return enhancedRequest({
      method: 'PUT',
      url: `/connections/${id}/default`,
      requestId: `connections.setDefault.${id}`
    }).then(response => {
      // Invalidate connections list cache
      clearCacheItem('connections.list');
      return response;
    });
  },

  getConnectionDashboard: (connectionId, options = {}) => {
    const { forceFresh = false } = options;

    return enhancedRequest({
      method: 'POST',
      url: '/batch',
      data: {
        requests: [
          { id: 'connection', path: `/connections/${connectionId}` },
          { id: 'tables', path: `/connections/${connectionId}/tables` },
          { id: 'metadata', path: `/connections/${connectionId}/metadata/status` }
        ]
      },
      cacheKey: `connections.dashboard.${connectionId}`,
      cacheTTL: 2 * 60 * 1000,
      requestId: `connections.dashboard.${connectionId}`,
      forceFresh
    }).catch(error => {
      // Check if it's an auth error
      if (error.response && error.response.status === 401) {
        console.error("Authentication error in dashboard batch request");
        // You might want to trigger a login redirect here
      } else {
        console.error("Dashboard batch request failed:", error);
      }
      throw error;
    });
  }
};

// Enhanced Tables and Schema API with pagination
export const schemaAPI = {
  getTables: (connectionId, options = {}) => {
    const {
      forceFresh = false,
      requestId = `schema.tables.${connectionId}`,
      page = null,
      pageSize = null
    } = options;

    let params = {};
    if (page !== null && pageSize !== null) {
      params = { page, pageSize };
    }

    return enhancedRequest({
      url: `/connections/${connectionId}/tables`,
      params,
      cacheKey: `schema.tables.${connectionId}${page !== null ? `.page${page}.size${pageSize}` : ''}`,
      cacheTTL: 10 * 60 * 1000, // 10 minutes for schema data
      requestId,
      forceFresh
    });
  },

  getColumns: (connectionId, tableName, options = {}) => {
    const { forceFresh = false, requestId = `schema.columns.${connectionId}.${tableName}` } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/tables/${tableName}/columns`,
      cacheKey: `schema.columns.${connectionId}.${tableName}`,
      cacheTTL: 10 * 60 * 1000, // 10 minutes for schema data
      requestId,
      forceFresh
    });
  },

  getPreview: (connectionId, tableName, maxRows = 50, options = {}) => {
    const { requestId = `schema.preview.${connectionId}.${tableName}` } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/tables/${tableName}/preview`,
      params: { max_rows: maxRows },
      requestId,
      // Don't cache previews - they should always be fresh
    });
  },

  getStatistics: (connectionId, tableName, options = {}) => {
    const { forceFresh = false, requestId = `schema.stats.${connectionId}.${tableName}` } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/tables/${tableName}/statistics`,
      cacheKey: `schema.statistics.${connectionId}.${tableName}`,
      cacheTTL: 30 * 60 * 1000, // 30 minutes for statistics which change less frequently
      requestId,
      forceFresh
    });
  },

  detectChanges: (connectionId) => {
    return enhancedRequest({
      method: 'POST',
      url: `/connections/${connectionId}/schema/detect-changes`,
      requestId: `schema.detectChanges.${connectionId}`
    }).then(response => {
      // If changes were detected, invalidate schema caches
      if (response.changes_detected > 0) {
        clearCacheItem(`schema.tables.${connectionId}`);
      }
      return response;
    });
  },

  getChanges: (connectionId, since) => {
    return enhancedRequest({
      url: `/connections/${connectionId}/changes`,
      params: { since },
      requestId: `schema.changes.${connectionId}`
      // Don't cache changes - they should always be fresh
    });
  },

  getTableDashboard: (connectionId, tableName, options = {}) => {
    const { forceFresh = false } = options;

    return enhancedRequest({
      method: 'POST',
      url: '/batch',
      data: {
        requests: [
          { id: 'columns', path: `/connections/${connectionId}/tables/${tableName}/columns` },
          { id: 'statistics', path: `/connections/${connectionId}/tables/${tableName}/statistics` },
          { id: 'profile', path: '/profile', params: { connection_id: connectionId, table: tableName } },
          { id: 'validations', path: '/validations', params: { table: tableName } }
        ]
      },
      cacheKey: `table.dashboard.${connectionId}.${tableName}`,
      cacheTTL: 5 * 60 * 1000, // 5 minutes
      requestId: `table.dashboard.${connectionId}.${tableName}`,
      forceFresh
    }).catch(error => {
      // Check if it's an auth error
      if (error.response && error.response.status === 401) {
        console.error("Authentication error in table dashboard batch request");
        // You might want to trigger a login redirect here
      } else {
        console.error("Table dashboard batch request failed:", error);
      }
      throw error;
    });
  }
};

// Enhanced Profiling API
export const profilingAPI = {
  getProfile: (connectionId, tableName, options = {}) => {
    const { forceFresh = false, requestId = `profile.${connectionId}.${tableName}` } = options;
    return enhancedRequest({
      url: '/profile',
      params: { connection_id: connectionId, table: tableName },
      cacheKey: `profile.${connectionId}.${tableName}`,
      cacheTTL: 10 * 60 * 1000, // 10 minutes
      requestId,
      forceFresh
    });
  },

  getHistory: (tableName, limit = 10, options = {}) => {
    const { forceFresh = false, requestId = `profile.history.${tableName}` } = options;
    return enhancedRequest({
      url: '/profile-history',
      params: { table: tableName, limit },
      cacheKey: `profile.history.${tableName}.limit${limit}`,
      cacheTTL: 10 * 60 * 1000, // 10 minutes
      requestId,
      forceFresh
    });
  },

  getTrends: (connectionId, tableName, days = 30, options = {}) => {
    const { forceFresh = false, requestId = `profile.trends.${connectionId}.${tableName}` } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/tables/${tableName}/trends`,
      params: { days },
      cacheKey: `profile.trends.${connectionId}.${tableName}.days${days}`,
      cacheTTL: 30 * 60 * 1000, // 30 minutes since trends don't change frequently
      requestId,
      forceFresh
    });
  }
};

// Enhanced Validations API
export const validationsAPI = {
  getRules: (tableName, options = {}) => {
    const { forceFresh = false, requestId = `validations.rules.${tableName}` } = options;
    return enhancedRequest({
      url: '/validations',
      params: { table: tableName },
      cacheKey: `validations.rules.${tableName}`,
      cacheTTL: 10 * 60 * 1000, // 10 minutes
      requestId,
      forceFresh
    });
  },

  createRule: (tableName, rule) => {
    return enhancedRequest({
      method: 'POST',
      url: '/validations',
      params: { table: tableName },
      data: rule,
      requestId: `validations.create.${tableName}`
    }).then(response => {
      // Invalidate rules cache for this table
      clearCacheItem(`validations.rules.${tableName}`);
      return response;
    });
  },

  updateRule: (ruleId, tableName, rule) => {
    return enhancedRequest({
      method: 'PUT',
      url: `/validations/${ruleId}`,
      params: { table: tableName },
      data: rule,
      requestId: `validations.update.${tableName}.${ruleId}`
    }).then(response => {
      // Invalidate rules cache for this table
      clearCacheItem(`validations.rules.${tableName}`);
      return response;
    });
  },

  deleteRule: (tableName, ruleName) => {
    return enhancedRequest({
      method: 'DELETE',
      url: '/validations',
      params: { table: tableName, rule_name: ruleName },
      requestId: `validations.delete.${tableName}.${ruleName}`
    }).then(response => {
      // Invalidate rules cache for this table
      clearCacheItem(`validations.rules.${tableName}`);
      return response;
    });
  },

  getSummary: (connectionId, options = {}) => {
    const { forceFresh = false, requestId = `validations.summary.${connectionId}` } = options;
    return enhancedRequest({
      url: '/validations/summary',
      params: { connection_id: connectionId },
      cacheKey: `validations.summary.${connectionId}`,
      cacheTTL: 5 * 60 * 1000, // 5 minutes
      requestId,
      forceFresh
    });
  },

  runValidations: (connectionId, tableName, connectionString, options = {}) => {
    const {
      timeout = 600000, // Default to 10 minutes, but allow override
      requestId = `validations.run.${connectionId}.${tableName}`
    } = options;

    return enhancedRequest({
      method: 'POST',
      url: '/run-validations',
      data: {
        connection_id: connectionId,
        table: tableName,
        connection_string: connectionString
      },
      timeout, // Add custom timeout
      requestId
    }).then(response => {
      // Invalidate validation rules cache to reflect new execution results
      clearCacheItem(`validations.rules.${tableName}`);
      return response;
    });
  },

  getValidationHistory: (profileId, options = {}) => {
    const { forceFresh = false, requestId = `validations.history.${profileId}` } = options;
    return enhancedRequest({
      url: `/validation-history/${profileId}`,
      cacheKey: `validations.history.${profileId}`,
      cacheTTL: 30 * 60 * 1000, // 30 minutes (historical data changes rarely)
      requestId,
      forceFresh
    });
  },

  generateDefaultValidations: (connectionId, tableName, connectionString) => {
    return enhancedRequest({
      method: 'POST',
      url: '/generate-default-validations',
      data: {
        connection_id: connectionId,
        table: tableName,
        connection_string: connectionString
      },
      requestId: `validations.generate.${connectionId}.${tableName}`
    }).then(response => {
      // Invalidate rules cache for this table since new rules have been created
      clearCacheItem(`validations.rules.${tableName}`);
      return response;
    });
  }
};

// Enhanced Metadata API
export const metadataAPI = {
  getMetadata: (connectionId, type = 'tables', options = {}) => {
    const { forceFresh = false, requestId = `metadata.${connectionId}.${type}` } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/metadata`,
      params: { type },
      cacheKey: `metadata.${connectionId}.${type}`,
      cacheTTL: 10 * 60 * 1000, // 10 minutes
      requestId,
      forceFresh
    });
  },

  collectMetadata: (connectionId, options = {}) => {
    const requestBody = { ...options };
    return enhancedRequest({
      method: 'POST',
      url: `/connections/${connectionId}/metadata/collect`,
      data: requestBody,
      requestId: `metadata.collect.${connectionId}`
    }).then(response => {
      // Invalidate metadata caches for this connection
      clearCacheItem(`metadata.${connectionId}.tables`);
      clearCacheItem(`metadata.${connectionId}.columns`);
      clearCacheItem(`metadata.${connectionId}.statistics`);
      return response;
    });
  },

  refreshMetadata: (connectionId, metadataType, tableName) => {
    return enhancedRequest({
      method: 'POST',
      url: `/connections/${connectionId}/metadata/refresh`,
      data: {
        metadata_type: metadataType,
        table_name: tableName
      },
      requestId: `metadata.refresh.${connectionId}.${metadataType}`
    }).then(response => {
      // Invalidate affected metadata caches
      clearCacheItem(`metadata.${connectionId}.${metadataType}`);
      if (tableName) {
        clearCacheItem(`schema.columns.${connectionId}.${tableName}`);
        clearCacheItem(`schema.statistics.${connectionId}.${tableName}`);
      }
      return response;
    });
  },

  getMetadataStatus: (connectionId, options = {}) => {
    const { forceFresh = false, requestId = `metadata.status.${connectionId}` } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/metadata/status`,
      cacheKey: `metadata.status.${connectionId}`,
      cacheTTL: 2 * 60 * 1000, // 2 minutes (status changes frequently)
      requestId,
      forceFresh
    });
  },

  getTasks: (connectionId, limit = 10, options = {}) => {
    const { forceFresh = false, requestId = `metadata.tasks.${connectionId}` } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/metadata/tasks`,
      params: { limit },
      cacheKey: `metadata.tasks.${connectionId}.limit${limit}`,
      cacheTTL: 1 * 60 * 1000, // 1 minute (tasks change frequently)
      requestId,
      forceFresh
    });
  },

  scheduleTask: (connectionId, taskType, tableName, priority = 'medium') => {
    return enhancedRequest({
      method: 'POST',
      url: `/connections/${connectionId}/metadata/tasks`,
      data: {
        task_type: taskType,
        table_name: tableName,
        priority
      },
      requestId: `metadata.schedule.${connectionId}.${taskType}`
    });
  },

  getTaskStatus: (connectionId, taskId) => {
    return enhancedRequest({
      url: `/connections/${connectionId}/metadata/tasks/${taskId}`,
      requestId: `metadata.taskStatus.${connectionId}.${taskId}`
      // Don't cache task status - it changes frequently
    });
  }
};

// Enhanced Admin API
export const adminAPI = {
  getUsers: (options = {}) => {
    const { forceFresh = false, requestId = 'admin.users' } = options;
    return enhancedRequest({
      url: '/admin/users',
      cacheKey: 'admin.users',
      cacheTTL: 5 * 60 * 1000, // 5 minutes
      requestId,
      forceFresh
    });
  },

  updateUser: (userId, userData) => {
    return enhancedRequest({
      method: 'PUT',
      url: `/admin/users/${userId}`,
      data: userData,
      requestId: `admin.updateUser.${userId}`
    }).then(response => {
      // Invalidate users cache
      clearCacheItem('admin.users');
      return response;
    });
  },

  inviteUser: (userData) => {
    return enhancedRequest({
      method: 'POST',
      url: '/admin/users',
      data: userData,
      requestId: 'admin.inviteUser'
    }).then(response => {
      // Invalidate users cache
      clearCacheItem('admin.users');
      return response;
    });
  },

  removeUser: (userId) => {
    return enhancedRequest({
      method: 'DELETE',
      url: `/admin/users/${userId}`,
      requestId: `admin.removeUser.${userId}`
    }).then(response => {
      // Invalidate users cache
      clearCacheItem('admin.users');
      return response;
    });
  },

  getOrganization: (options = {}) => {
    const { forceFresh = false, requestId = 'admin.organization' } = options;
    return enhancedRequest({
      url: '/admin/organization',
      cacheKey: 'admin.organization',
      cacheTTL: 30 * 60 * 1000, // 30 minutes (organization details change rarely)
      requestId,
      forceFresh
    });
  },

  updateOrganization: (orgData) => {
    return enhancedRequest({
      method: 'PUT',
      url: '/admin/organization',
      data: orgData,
      requestId: 'admin.updateOrganization'
    }).then(response => {
      // Invalidate organization cache
      clearCacheItem('admin.organization');
      return response;
    });
  },

  getWorkerStats: (options = {}) => {
    const { requestId = 'admin.workerStats' } = options;
    return enhancedRequest({
      url: '/metadata/worker/stats',
      requestId
      // Don't cache worker stats - they change frequently
    });
  }
};

// Enhanced User Setup API
export const userAPI = {
  setupUser: (userData) => {
    return enhancedRequest({
      method: 'POST',
      url: '/setup-user',
      data: userData,
      requestId: 'user.setup'
    });
  }
};

// Enhanced Analytics API
export const analyticsAPI = {
  getChangeFrequency: (connectionId, objectType, objectName, days = 30, options = {}) => {
    const { forceFresh = false, requestId = `analytics.changeFrequency.${connectionId}.${objectName}` } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/analytics/change-frequency`,
      params: { object_type: objectType, object_name: objectName, days },
      cacheKey: `analytics.changeFrequency.${connectionId}.${objectType}.${objectName}.days${days}`,
      cacheTTL: 1 * 60 * 60 * 1000, // 1 hour (analytics data changes slowly)
      requestId,
      forceFresh
    });
  },

  getRefreshSuggestion: (connectionId, objectType, objectName, currentInterval = 24, options = {}) => {
    const { forceFresh = false, requestId = `analytics.refreshSuggestion.${connectionId}.${objectName}` } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/analytics/refresh-suggestion`,
      params: {
        object_type: objectType,
        object_name: objectName,
        current_interval: currentInterval
      },
      cacheKey: `analytics.refreshSuggestion.${connectionId}.${objectType}.${objectName}.interval${currentInterval}`,
      cacheTTL: 12 * 60 * 60 * 1000, // 12 hours (suggestions change very slowly)
      requestId,
      forceFresh
    });
  },

  getHighImpactObjects: (connectionId, limit = 10, options = {}) => {
    const { forceFresh = false, requestId = `analytics.highImpact.${connectionId}` } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/analytics/high-impact`,
      params: { limit },
      cacheKey: `analytics.highImpact.${connectionId}.limit${limit}`,
      cacheTTL: 6 * 60 * 60 * 1000, // 6 hours (high impact objects change slowly)
      requestId,
      forceFresh
    });
  },

  getDashboard: (connectionId, options = {}) => {
    const { forceFresh = false, requestId = `analytics.dashboard.${connectionId}` } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/analytics/dashboard`,
      cacheKey: `analytics.dashboard.${connectionId}`,
      cacheTTL: 1 * 60 * 60 * 1000, // 1 hour
      requestId,
      forceFresh
    });
  }
};

// Export default enhancedRequest for custom API calls
export default enhancedRequest;