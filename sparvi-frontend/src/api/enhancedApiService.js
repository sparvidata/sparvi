import axios from 'axios';
import {getSession, signOut, supabase} from './supabase';
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
  : process.env.REACT_APP_API_BASE_URL || 'https://sparvi-webapp-fjdjdvh2bse9d0gm.centralus-01.azurewebsites.net/api';

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

const isCancelledRequest = (error) => {
  return axios.isCancel(error) ||
         error.name === 'CanceledError' ||
         error.cancelled === true;
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

// const debugAuth = async () => {
//   try {
//     const session = await getSession();
//     console.log("Auth check: Session exists:", !!session);
//
//     // Check different possible token locations
//     const token = session?.access_token ||
//                  session?.token ||
//                  session?.accessToken ||
//                  (session?.user?.token) ||
//                  (session?.data?.access_token);
//
//     console.log("Auth check: Token exists:", !!token);
//     if (token) {
//       console.log("Auth check: Token preview:", `${token.substring(0, 10)}...`);
//     } else {
//       // If no token found, log the session structure to help debug
//       console.log("Auth check: Session structure:", JSON.stringify(session, null, 2));
//     }
//     return !!token;
//   } catch (e) {
//     console.error("Error in debugAuth:", e);
//     return false;
//   }
// };

apiClient.interceptors.request.use(
  async (config) => {
    try {
      const session = await getSession();
      console.log("Full Session Object:", session);

      const token =
        session?.access_token ||
        session?.token ||
        session?.accessToken ||
        (session?.user?.token) ||
        (session?.data?.access_token);

      console.log("Token being sent:", token ? token.substring(0, 20) + '...' : 'No token');

      if (token) {
        config.headers.Authorization = `Bearer ${token}`;
      }
    } catch (err) {
      console.error('Error adding auth token to request:', err);
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor for error handling
apiClient.interceptors.response.use(
  (response) => response,
  async (error) => {
    // Don't treat cancelled requests as errors
    if (axios.isCancel(error)) {
      return Promise.reject({ cancelled: true });
    }

    // Handle specific error codes
    if (error.response) {
      if (error.response.status === 401) {
        console.error('Unauthorized access. Redirecting to login...');

        // Prevent multiple redirects
        if (!window._redirectingToLogin) {
          window._redirectingToLogin = true;

          try {
            // Force signout from Supabase
            await signOut();

            // Clear any cached session data
            window._sessionCache = null;

            // Clear session storage
            sessionStorage.removeItem('activeConnectionId');

            // Redirect to login with current location
            const returnPath = window.location.pathname;
            window.location.href = `/login?returnTo=${encodeURIComponent(returnPath)}`;
          } catch (e) {
            console.error('Error during forced logout:', e);
            // Still redirect to login as fallback
            window.location.href = '/login';
          }
        }

        return Promise.reject(error);
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
  // Ensure controller is valid and has abort method
  const signal = abortSignal || (controller && typeof controller.abort === 'function' ?
                               controller.signal : undefined);

  // Set a timeout
  let timeoutId;
  if (typeof controller?.abort === 'function') {
    timeoutId = setTimeout(() => {
      console.log(`Request timeout (${timeout}ms) for: ${url}`, { requestId });
      try {
        controller.abort();
      } catch (e) {
        console.error('Error aborting request:', e);
      }
    }, timeout);
  }

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
    // If it's a cancelled request, log it but don't treat it as a full error
    if (isCancelledRequest(error)) {
      console.warn(`Request to ${url} was cancelled`, error);
      return null;
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
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
};

// Enhanced Connections API
export const connectionsAPI = {
  getAll: (options = {}) => {
    const { forceFresh = false, requestId = 'connections.getAll' } = options;

    // Check if we already have cached data and we're not forcing a fresh fetch
    const cachedData = getCacheItem('connections.list');
    if (cachedData && !forceFresh) {
      // Return the cached data immediately
      return Promise.resolve(cachedData);
    }

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
    // Make sure we're actually trying to connect to the database
    console.log('Testing connection with data:', JSON.stringify(data, null, 2));

    return enhancedRequest({
      method: 'POST',
      url: '/connections/test',
      data,
      requestId: 'connections.test'
    }).then(response => {
      // Log the response to help diagnose issues
      console.log('Test connection response:', response);

      // Ensure the response contains proper success/failure indicators
      if (!response.message && !response.success) {
        console.warn('Test connection response missing expected fields');
      }

      return response;
    }).catch(error => {
      console.error('Test connection failed:', error);
      throw error;
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
    }).then(response => {
      // Normalize response structure
      console.log("Raw tables response:", response);

      if (response?.data?.tables) {
        return response;
      }
      else if (response?.tables) {
        return { data: { tables: response.tables } };
      }
      else if (Array.isArray(response)) {
        return { data: { tables: response } };
      }
      // If we reach here, try to find any array property
      for (const key in response) {
        if (Array.isArray(response[key])) {
          return { data: { tables: response[key] } };
        }
      }

      // Last resort - empty array
      return { data: { tables: [] } };
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
      url: '/preview', // Changed URL based on the API documentation
      params: {
        connection_id: connectionId,
        table: tableName,
        max_rows: maxRows
      },
      requestId
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

  detectChanges: (connectionId, options = {}) => {
    return enhancedRequest({
      method: 'POST',
      url: `/connections/${connectionId}/schema/detect-changes`,
      requestId: `schema.detectChanges.${connectionId}`,
      headers: {
        'Content-Type': 'application/json'
      },
      data: options || {}  // Make sure we send an empty object if no options
    }).then(response => {
      // Clear schema cache if changes were detected
      if (response.changes_detected > 0) {
        clearCacheItem(`schema.tables.${connectionId}`);
      }
      return response;
    });
  },

  getChanges: (connectionId, options = {}) => {
    // Change default to include acknowledged changes
    const { acknowledged = 'all', since = null } = options;

    const params = {};
    // Only add acknowledged param if it's not 'all'
    if (acknowledged !== 'all') {
      params.acknowledged = acknowledged;
    }
    if (since) params.since = since;

    return enhancedRequest({
      url: `/connections/${connectionId}/changes`,
      params,
      requestId: `schema.changes.${connectionId}`,
      // Don't cache changes - they should always be fresh
      cacheKey: null
    });
  },

  acknowledgeChanges: (connectionId, tableName) => {
    return enhancedRequest({
      method: 'POST',
      url: `/connections/${connectionId}/changes/acknowledge`,
      data: { table_name: tableName },
      requestId: `schema.acknowledge.${connectionId}.${tableName}`
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
    const {
      forceFresh = false,
      requestId = `validations.rules.${tableName}`,
      connectionId,
      includeInactive = false // New parameter
    } = options;

    if (!connectionId) {
      console.warn('connectionId is required for getRules');
      return Promise.reject(new Error('connectionId is required'));
    }

    return enhancedRequest({
      url: '/validations',
      params: {
        table: tableName,
        connection_id: connectionId,
        include_inactive: includeInactive // Pass to API
      },
      cacheKey: `validations.rules.${tableName}${includeInactive ? '.all' : ''}`,
      cacheTTL: 10 * 60 * 1000, // 10 minutes
      requestId,
      forceFresh
    }).then(response => {
      console.log('Raw validation rules response:', response);

      // Normalize the response structure
      if (response && response.data && response.data.rules) {
        return { data: { rules: response.data.rules } };
      } else if (response && response.rules) {
        return { data: { rules: response.rules } };
      } else if (Array.isArray(response)) {
        return { data: { rules: response } };
      }

      // If we can't find rules, return an empty array in the expected format
      console.warn('Unexpected validation rules API response format:', response);
      return { data: { rules: [] } };
    }).catch(error => {
      console.error(`Error fetching validation rules for ${tableName}:`, error);
      throw error;
    });
  },

  createRule: (tableName, rule, connectionId) => {
    if (!connectionId) {
      console.warn('connectionId is required for createRule');
    }

    return enhancedRequest({
      method: 'POST',
      url: '/validations',
      params: { table: tableName, connection_id: connectionId },
      data: rule,
      requestId: `validations.create.${tableName}`
    }).then(response => {
      // Invalidate rules cache for this table
      clearCacheItem(`validations.rules.${tableName}`);
      return response;
    });
  },

  updateRule: (ruleId, tableName, rule, connectionId) => {
    if (!connectionId) {
      console.warn('connectionId is required for updateRule');
    }

    return enhancedRequest({
      method: 'PUT',
      url: `/validations/${ruleId}`,
      params: { table: tableName, connection_id: connectionId },
      data: rule,
      requestId: `validations.update.${tableName}.${ruleId}`
    }).then(response => {
      // Invalidate rules cache for this table
      clearCacheItem(`validations.rules.${tableName}`);
      return response;
    });
  },

  deleteRule: (tableName, ruleName, connectionId) => {
    if (!connectionId) {
      console.warn('connectionId is required for deleteRule');
    }

    return enhancedRequest({
      method: 'DELETE',
      url: '/validations',
      params: { table: tableName, rule_name: ruleName, connection_id: connectionId },
      requestId: `validations.delete.${tableName}.${ruleName}`
    }).then(response => {
      // Invalidate rules cache for this table
      clearCacheItem(`validations.rules.${tableName}`);
      return response;
    });
  },

  deactivateRule: (tableName, ruleName, connectionId) => {
    if (!connectionId) {
      console.warn('connectionId is required for deactivateRule');
      return Promise.reject(new Error('connectionId is required'));
    }

    console.log(`Calling API to deactivate rule "${ruleName}" for table "${tableName}"`);

    return enhancedRequest({
      method: 'PUT', // Changed from POST to PUT
      url: '/validations/deactivate',
      params: {
        table: tableName,
        rule_name: ruleName,
        connection_id: connectionId
      },
      requestId: `validations.deactivate.${tableName}.${ruleName}`
    }).then(response => {
      // Invalidate rules cache for this table
      clearCacheItem(`validations.rules.${tableName}`);
      console.log("Deactivation API call successful");
      return response;
    }).catch(error => {
      console.error(`API error deactivating rule "${ruleName}":`, error);
      throw error;
    });
  },

  getSummary: (connectionId, options = {}) => {
    const { forceFresh = false, requestId = `validations.summary.${connectionId}` } = options;

    if (!connectionId) {
      console.warn('connectionId is required for getSummary');
    }

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
    if (!connectionId) {
      console.error('connectionId is required for runValidations');
      return Promise.reject(new Error('connectionId is required'));
    }

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
      // Log the response to debug
      console.log("Raw validation response:", response);

      // Invalidate validation rules cache to reflect new execution results
      clearCacheItem(`validations.rules.${tableName}`);

      // Process and normalize the response if needed
      if (response && typeof response === 'object') {
        // If response is already in the expected format with 'results' array, return it
        if (response.results && Array.isArray(response.results)) {
          return response;
        }
        // If response is an array directly, wrap it in an object with results property
        else if (Array.isArray(response)) {
          return { results: response };
        }
        // If we have data nested under a data property
        else if (response.data) {
          if (Array.isArray(response.data)) {
            return { results: response.data };
          } else if (response.data.results) {
            return { results: response.data.results };
          }
        }
      }

      // Return the original response as a fallback
      return { results: Array.isArray(response) ? response : [response] };
    }).catch(error => {
      console.error("Error in runValidations:", error);
      throw error;
    });
  },

  getValidationHistory: (tableName, connectionId, options = {}) => {
    const {
      limit = 30,  // Default to 30 days
      forceFresh = false,
      requestId = `validations.history.${tableName}`
    } = options;

    if (!connectionId) {
      console.warn('connectionId is required for getValidationHistory');
      return Promise.reject(new Error('connectionId is required'));
    }

    return enhancedRequest({
      url: '/validation-history',
      params: {
        table: tableName,
        connection_id: connectionId,
        limit
      },
      cacheKey: `validations.history.${tableName}.${connectionId}.${limit}`,
      cacheTTL: 30 * 60 * 1000, // 30 minutes (historical data changes rarely)
      requestId,
      forceFresh
    }).catch(error => {
      console.error(`Error fetching validation history for ${tableName}:`, error);

      // Return empty history if we fail
      return { history: [] };
    });
  },

  // Aggregated validation trends per day
  getValidationTrends: (connectionId, tableName, options = {}) => {
    const {
      days = 30,
      forceFresh = false,
      requestId = `validations.trends.${connectionId}.${tableName}`
    } = options;

    return enhancedRequest({
      url: `/connections/${connectionId}/validations/${tableName}/trends`,
      params: { days },
      cacheKey: `validations.trends.${connectionId}.${tableName}.days${days}`,
      cacheTTL: 10 * 60 * 1000, // 10 minutes
      requestId,
      forceFresh
    });
  },

  // New method for getting latest validation results
  getLatestValidationResults: (connectionId, tableName, options = {}) => {
    const {
      forceFresh = false,
      requestId = `validations.latest.${connectionId}.${tableName}`
    } = options;

    if (!connectionId || !tableName) {
      console.warn('connectionId and tableName are required for getLatestValidationResults');
      return Promise.reject(new Error('Missing required parameters'));
    }

    return enhancedRequest({
      url: `/validations/latest/${connectionId}/${tableName}`,
      cacheKey: `validations.latest.${connectionId}.${tableName}`,
      cacheTTL: 5 * 60 * 1000, // 5 minutes
      requestId,
      forceFresh
    });
  },

  generateDefaultValidations: (connectionId, tableName, connectionString) => {
    if (!connectionId) {
      console.error('connectionId is required for generateDefaultValidations');
      return Promise.reject(new Error('connectionId is required'));
    }

    console.log(`Generating default validations for table: ${tableName}, connectionId: ${connectionId}`);

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
      // Log the response to see what we're getting back
      console.log('Default validation generation response:', response);

      // Clear all validation-related caches for this table
      clearCacheItem(`validations.rules.${tableName}`);
      clearCacheItem(`validations.summary.${connectionId}`);
      clearCacheItem(`table.dashboard.${connectionId}.${tableName}`);

      // After generating, explicitly fetch the validations with the connection ID
      return enhancedRequest({
        url: '/validations',
        params: {
          table: tableName,
          connection_id: connectionId
        },
        requestId: `validations.get.${tableName}.${connectionId}`,
        forceFresh: true // Skip cache to get fresh data
      }).then(rulesResponse => {
        console.log('Validation rules after generation:', rulesResponse);

        // Return both responses in a structured way
        return {
          generation: response,
          rules: rulesResponse
        };
      }).catch(fetchError => {
        console.error('Error fetching rules after generation:', fetchError);
        // Still return the original response if fetching the rules fails
        return response;
      });
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
  },

  // New methods for enhanced analytics

  getHistoricalMetrics: (connectionId, options = {}) => {
    const {
      metric_name, table_name, column_name, days = 30,
      limit = 100, group_by_date = false, forceFresh = false
    } = options;

    return enhancedRequest({
      url: `/connections/${connectionId}/analytics/historical-metrics`,
      params: {
        metric_name, table_name, column_name,
        days, limit, group_by_date
      },
      cacheKey: `analytics.historicalMetrics.${connectionId}.${metric_name || 'all'}.${table_name || 'all'}.${days}`,
      cacheTTL: 1 * 60 * 60 * 1000, // 1 hour
      requestId: `analytics.historicalMetrics.${connectionId}`,
      forceFresh
    });
  },

  trackMetrics: (connectionId, metrics) => {
    return enhancedRequest({
      method: 'POST',
      url: `/connections/${connectionId}/analytics/track-metrics`,
      data: { metrics },
      requestId: `analytics.trackMetrics.${connectionId}`
    });
  },

  getDashboardMetrics: (connectionId, options = {}) => {
    const { days = 30, limit = 100, forceFresh = false } = options;

    return enhancedRequest({
      url: `/connections/${connectionId}/analytics/dashboard/metrics`,
      params: { days, limit },
      cacheKey: `analytics.dashboardMetrics.${connectionId}.${days}`,
      cacheTTL: 1 * 60 * 60 * 1000, // 1 hour
      requestId: `analytics.dashboardMetrics.${connectionId}`,
      forceFresh
    });
  }
};


// Enhanced Anomaly API
export const anomalyAPI = {
  // Get anomaly configurations for a connection
  getConfigs: (connectionId, options = {}) => {
    const { table_name, metric_name, forceFresh = false } = options;
    const params = {};

    if (table_name) params.table_name = table_name;
    if (metric_name) params.metric_name = metric_name;

    return enhancedRequest({
      url: `/connections/${connectionId}/anomalies/configs`,
      params,
      cacheKey: `anomaly.configs.${connectionId}`,
      cacheTTL: 5 * 60 * 1000, // 5 minutes
      requestId: `anomaly.configs.${connectionId}`,
      forceFresh
    });
  },

  // Get a specific configuration
  getConfig: (connectionId, configId, options = {}) => {
    const { forceFresh = false } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/anomalies/configs/${configId}`,
      cacheKey: `anomaly.config.${connectionId}.${configId}`,
      cacheTTL: 5 * 60 * 1000, // 5 minutes
      requestId: `anomaly.config.${connectionId}.${configId}`,
      forceFresh
    });
  },

  // Create a new configuration
  createConfig: (connectionId, configData) => {
    return enhancedRequest({
      method: 'POST',
      url: `/connections/${connectionId}/anomalies/configs`,
      data: configData,
      requestId: `anomaly.createConfig.${connectionId}`
    }).then(response => {
      // Invalidate configs cache
      clearCacheItem(`anomaly.configs.${connectionId}`);
      return response;
    });
  },

  // Update a configuration
  updateConfig: (connectionId, configId, configData) => {
    return enhancedRequest({
      method: 'PUT',
      url: `/connections/${connectionId}/anomalies/configs/${configId}`,
      data: configData,
      requestId: `anomaly.updateConfig.${connectionId}.${configId}`
    }).then(response => {
      // Invalidate affected cache items
      clearCacheItem(`anomaly.config.${connectionId}.${configId}`);
      clearCacheItem(`anomaly.configs.${connectionId}`);
      return response;
    });
  },

  // Delete a configuration
  deleteConfig: (connectionId, configId) => {
    return enhancedRequest({
      method: 'DELETE',
      url: `/connections/${connectionId}/anomalies/configs/${configId}`,
      requestId: `anomaly.deleteConfig.${connectionId}.${configId}`
    }).then(response => {
      // Invalidate affected cache items
      clearCacheItem(`anomaly.config.${connectionId}.${configId}`);
      clearCacheItem(`anomaly.configs.${connectionId}`);
      return response;
    });
  },

  // Get anomalies for a connection
  getAnomalies: (connectionId, options = {}) => {
    const {
      table_name,
      status,
      days = 30,
      limit = 100,
      forceFresh = false
    } = options;

    const params = { days, limit };
    if (table_name) params.table_name = table_name;
    if (status) params.status = status;

    return enhancedRequest({
      url: `/connections/${connectionId}/anomalies`,
      params,
      cacheKey: `anomaly.list.${connectionId}.${JSON.stringify(params)}`,
      cacheTTL: 2 * 60 * 1000, // 2 minutes (anomalies change frequently)
      requestId: `anomaly.list.${connectionId}`,
      forceFresh
    });
  },

  // Get a specific anomaly
  getAnomaly: (connectionId, anomalyId, options = {}) => {
    const { forceFresh = false } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/anomalies/${anomalyId}`,
      cacheKey: `anomaly.detail.${connectionId}.${anomalyId}`,
      cacheTTL: 5 * 60 * 1000, // 5 minutes
      requestId: `anomaly.detail.${connectionId}.${anomalyId}`,
      forceFresh
    });
  },

  // Update anomaly status
  updateAnomalyStatus: (connectionId, anomalyId, status, resolutionNote = null) => {
    return enhancedRequest({
      method: 'PUT',
      url: `/connections/${connectionId}/anomalies/${anomalyId}/status`,
      data: {
        status,
        resolution_note: resolutionNote
      },
      requestId: `anomaly.updateStatus.${connectionId}.${anomalyId}`
    }).then(response => {
      // Invalidate affected cache items
      clearCacheItem(`anomaly.detail.${connectionId}.${anomalyId}`);
      clearCacheItem(`anomaly.list.${connectionId}`);
      clearCacheItem(`anomaly.summary.${connectionId}`);
      clearCacheItem(`anomaly.dashboard.${connectionId}`);
      return response;
    });
  },

  // Run anomaly detection manually
  runDetection: (connectionId, options = {}) => {
    return enhancedRequest({
      method: 'POST',
      url: `/connections/${connectionId}/anomalies/detect`,
      data: options,
      requestId: `anomaly.detect.${connectionId}`
    }).then(response => {
      // Invalidate anomaly-related caches after detection
      clearCacheItem(`anomaly.list.${connectionId}`);
      clearCacheItem(`anomaly.summary.${connectionId}`);
      clearCacheItem(`anomaly.dashboard.${connectionId}`);
      return response;
    });
  },

  // Get anomaly summary
  getSummary: (connectionId, options = {}) => {
    const { days = 30, forceFresh = false } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/anomalies/summary`,
      params: { days },
      cacheKey: `anomaly.summary.${connectionId}.${days}`,
      cacheTTL: 5 * 60 * 1000, // 5 minutes
      requestId: `anomaly.summary.${connectionId}`,
      forceFresh
    });
  },

  // Get dashboard data
  getDashboardData: (connectionId, options = {}) => {
    const { days = 30, forceFresh = false } = options;
    return enhancedRequest({
      url: `/connections/${connectionId}/anomalies/dashboard`,
      params: { days },
      cacheKey: `anomaly.dashboard.${connectionId}.${days}`,
      cacheTTL: 5 * 60 * 1000, // 5 minutes
      requestId: `anomaly.dashboard.${connectionId}`,
      forceFresh
    });
  }
};

// Enhanced Automation API
export const automationAPI = {
  // Global Configuration
  getGlobalConfig: (options = {}) => {
    const { forceFresh = false, requestId = 'automation.globalConfig.get' } = options;
    return enhancedRequest({
      url: '/automation/global-config',
      cacheKey: 'automation.globalConfig',
      cacheTTL: 5 * 60 * 1000, // 5 minutes
      requestId,
      forceFresh
    });
  },

  updateGlobalConfig: (configData) => {
    return enhancedRequest({
      method: 'PUT',
      url: '/automation/global-config',
      data: configData,
      requestId: 'automation.globalConfig.update'
    }).then(response => {
      // Clear global config cache
      clearCacheItem('automation.globalConfig');
      return response;
    });
  },

  toggleGlobalAutomation: (enabled) => {
    return enhancedRequest({
      method: 'POST',
      url: '/automation/global-toggle',
      data: { enabled },
      requestId: 'automation.globalToggle'
    }).then(response => {
      // Clear global config cache
      clearCacheItem('automation.globalConfig');
      return response;
    });
  },

  // Get next run times for a specific connection
  getNextRunTimes: (connectionId, options = {}) => {
    const { forceFresh = false, requestId = `automation.nextRuns.${connectionId}` } = options;

    if (!connectionId) {
      console.warn('getNextRunTimes called without connectionId');
      return Promise.resolve({ next_runs: {} });
    }

    return enhancedRequest({
      url: `/automation/connections/${connectionId}/next-runs`,
      cacheKey: `automation.nextRuns.${connectionId}`,
      cacheTTL: 1 * 60 * 1000, // 1 minute cache - next runs change frequently
      requestId,
      forceFresh
    }).catch(error => {
      console.error(`Error fetching next run times for connection ${connectionId}:`, error);
      // Return a safe default instead of throwing
      return {
        connection_id: connectionId,
        next_runs: {},
        error: error.message || 'Failed to load next run times'
      };
    });
  },

  // Get next run times for all connections
  getAllNextRunTimes: (options = {}) => {
    const { forceFresh = false, requestId = 'automation.allNextRuns' } = options;
    return enhancedRequest({
      url: '/automation/next-runs',
      cacheKey: 'automation.allNextRuns',
      cacheTTL: 1 * 60 * 1000, // 1 minute cache
      requestId,
      forceFresh
    }).catch(error => {
      console.error('Error fetching all next run times:', error);
      // Return a safe default instead of throwing
      return {
        next_runs_by_connection: {},
        error: error.message || 'Failed to load next run times'
      };
    });
  },

  // Enhanced status with next run times
  getEnhancedStatus: (connectionId = null, options = {}) => {
    const { forceFresh = false } = options;

    const params = {};
    if (connectionId) params.connection_id = connectionId;

    const requestId = connectionId
      ? `automation.enhancedStatus.${connectionId}`
      : 'automation.enhancedStatus.all';

    const cacheKey = connectionId
      ? `automation.enhancedStatus.${connectionId}`
      : 'automation.enhancedStatus.all';

    return enhancedRequest({
      url: '/automation/status-enhanced',
      params,
      cacheKey,
      cacheTTL: 1 * 60 * 1000, // 1 minute cache
      requestId,
      forceFresh
    });
  },

  // Connection Configuration
  getConnectionConfigs: (options = {}) => {
    const { forceFresh = false, requestId = 'automation.connectionConfigs.getAll' } = options;
    return enhancedRequest({
      url: '/automation/connection-configs',
      cacheKey: 'automation.connectionConfigs',
      cacheTTL: 5 * 60 * 1000, // 5 minutes
      requestId,
      forceFresh
    });
  },

  getConnectionConfig: (connectionId, options = {}) => {
    const { forceFresh = false, requestId = `automation.connectionConfig.get.${connectionId}` } = options;
    return enhancedRequest({
      url: `/automation/connection-configs/${connectionId}`,
      cacheKey: `automation.connectionConfig.${connectionId}`,
      cacheTTL: 5 * 60 * 1000, // 5 minutes
      requestId,
      forceFresh
    });
  },

  updateConnectionConfig: (connectionId, configData) => {
    return enhancedRequest({
      method: 'PUT',
      url: `/automation/connection-configs/${connectionId}`,
      data: configData,
      requestId: `automation.connectionConfig.update.${connectionId}`
    }).then(response => {
      // Clear affected cache items
      clearCacheItem(`automation.connectionConfig.${connectionId}`);
      clearCacheItem('automation.connectionConfigs');
      clearCacheItem(`automation.status.${connectionId}`);
      return response;
    });
  },

  // Table Configuration
  getTableConfig: (connectionId, tableName, options = {}) => {
    const { forceFresh = false, requestId = `automation.tableConfig.get.${connectionId}.${tableName}` } = options;
    return enhancedRequest({
      url: `/automation/table-configs/${connectionId}/${tableName}`,
      cacheKey: `automation.tableConfig.${connectionId}.${tableName}`,
      cacheTTL: 5 * 60 * 1000, // 5 minutes
      requestId,
      forceFresh
    });
  },

  updateTableConfig: (connectionId, tableName, configData) => {
    return enhancedRequest({
      method: 'PUT',
      url: `/automation/table-configs/${connectionId}/${tableName}`,
      data: configData,
      requestId: `automation.tableConfig.update.${connectionId}.${tableName}`
    }).then(response => {
      // Clear affected cache items
      clearCacheItem(`automation.tableConfig.${connectionId}.${tableName}`);
      return response;
    });
  },

  // Status and Monitoring
  getAutomationStatus: (organizationId = null, connectionId = null, options = {}) => {
    const { forceFresh = false } = options;
    const endpoint = connectionId
      ? `/automation/status/${connectionId}`
      : '/automation/status';

    const requestId = connectionId
      ? `automation.status.connection.${connectionId}`
      : 'automation.status.global';

    const cacheKey = connectionId
      ? `automation.status.${connectionId}`
      : 'automation.status.global';

    return enhancedRequest({
      url: endpoint,
      cacheKey,
      cacheTTL: 1 * 60 * 1000, // 1 minute (status changes frequently)
      requestId,
      forceFresh
    });
  },

  getJobs: (options = {}) => {
    const {
      connectionId,
      status,
      limit = 50,
      forceFresh = false,
      requestId = 'automation.jobs.get'
    } = options;

    const params = {};
    if (connectionId) params.connection_id = connectionId;
    if (status) params.status = status;
    if (limit) params.limit = limit;

    return enhancedRequest({
      url: '/automation/jobs',
      params,
      cacheKey: `automation.jobs.${JSON.stringify(params)}`,
      cacheTTL: 30 * 1000, // 30 seconds (jobs change frequently)
      requestId,
      forceFresh
    });
  },

  // Control Operations
  toggleConnectionAutomation: (connectionId, enabled) => {
    return enhancedRequest({
      method: 'POST',
      url: `/automation/toggle/${connectionId}`,
      data: { enabled },
      requestId: `automation.toggle.${connectionId}`
    }).then(response => {
      // Clear affected cache items
      clearCacheItem(`automation.connectionConfig.${connectionId}`);
      clearCacheItem(`automation.status.${connectionId}`);
      clearCacheItem('automation.connectionConfigs');
      return response;
    });
  },

  triggerAutomation: (connectionId, automationType = null) => {
    return enhancedRequest({
      method: 'POST',
      url: `/automation/trigger/${connectionId}`,
      data: { automation_type: automationType },
      requestId: `automation.trigger.${connectionId}.${automationType || 'all'}`
    }).then(response => {
      // Clear job cache to reflect new job
      clearCacheItem('automation.jobs');
      clearCacheItem(`automation.status.${connectionId}`);
      return response;
    });
  },

  cancelJob: (jobId) => {
    return enhancedRequest({
      method: 'POST',
      url: `/automation/jobs/${jobId}/cancel`,
      requestId: `automation.cancelJob.${jobId}`
    }).then(response => {
      // Clear job cache
      clearCacheItem('automation.jobs');
      return response;
    });
  },

  // Dashboard
  getDashboard: (options = {}) => {
    const { forceFresh = false, requestId = 'automation.dashboard' } = options;
    return enhancedRequest({
      url: '/automation/dashboard',
      cacheKey: 'automation.dashboard',
      cacheTTL: 2 * 60 * 1000, // 2 minutes
      requestId,
      forceFresh
    });
  },

  // Templates
  getTemplates: (options = {}) => {
    const { forceFresh = false, requestId = 'automation.templates' } = options;
    return enhancedRequest({
      url: '/automation/templates',
      cacheKey: 'automation.templates',
      cacheTTL: 30 * 60 * 1000, // 30 minutes (templates don't change often)
      requestId,
      forceFresh
    });
  },

  // Bulk Operations
  bulkUpdateConfigs: (connectionIds, configData) => {
    return enhancedRequest({
      method: 'POST',
      url: '/automation/bulk-update',
      data: {
        connection_ids: connectionIds,
        config: configData
      },
      requestId: 'automation.bulkUpdate'
    }).then(response => {
      // Clear all connection config caches
      connectionIds.forEach(id => {
        clearCacheItem(`automation.connectionConfig.${id}`);
        clearCacheItem(`automation.status.${id}`);
      });
      clearCacheItem('automation.connectionConfigs');
      return response;
    });
  },

  // Admin Operations
  getSchedulerStatus: (options = {}) => {
    const { forceFresh = false, requestId = 'automation.scheduler.status' } = options;
    return enhancedRequest({
      url: '/automation/scheduler/status',
      cacheKey: 'automation.scheduler.status',
      cacheTTL: 30 * 1000, // 30 seconds
      requestId,
      forceFresh
    });
  },

  restartScheduler: () => {
    return enhancedRequest({
      method: 'POST',
      url: '/automation/scheduler/restart',
      requestId: 'automation.scheduler.restart'
    }).then(response => {
      // Clear scheduler status cache
      clearCacheItem('automation.scheduler.status');
      return response;
    });
  }
};

// Export default enhancedRequest for custom API calls
export default enhancedRequest;