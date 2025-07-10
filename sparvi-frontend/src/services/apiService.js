import { safeFetch, resetCircuitBreaker } from '../utils/requestUtils';

/**
 * Enhanced API service with circuit breaker and error handling
 */
class APIService {
  constructor() {
    this.baseURL = process.env.REACT_APP_API_URL || '';
    this.defaultHeaders = {
      'Content-Type': 'application/json',
    };
  }

  /**
   * Get authorization headers
   */
  getAuthHeaders() {
    const token = localStorage.getItem('token');
    return token ? { Authorization: `Bearer ${token}` } : {};
  }

  /**
   * Handle authentication errors
   */
  handleAuthError(error) {
    if (error.status === 401) {
      // Token expired or invalid
      localStorage.removeItem('token');
      localStorage.removeItem('user');

      // Only redirect if we're not already on the login page
      if (window.location.pathname !== '/login') {
        window.location.href = '/login';
      }
    }
    throw error;
  }

  /**
   * Enhanced fetch wrapper
   */
  async fetch(endpoint, options = {}) {
    const url = `${this.baseURL}${endpoint}`;
    const headers = {
      ...this.defaultHeaders,
      ...this.getAuthHeaders(),
      ...options.headers
    };

    try {
      return await safeFetch(url, {
        ...options,
        headers
      });
    } catch (error) {
      // Handle authentication errors
      if (error.status === 401 || error.status === 403) {
        return this.handleAuthError(error);
      }
      throw error;
    }
  }

  /**
   * GET request
   */
  async get(endpoint, params = {}, options = {}) {
    return this.fetch(endpoint, {
      method: 'GET',
      params,
      ...options
    });
  }

  /**
   * POST request
   */
  async post(endpoint, data = {}, options = {}) {
    return this.fetch(endpoint, {
      method: 'POST',
      body: JSON.stringify(data),
      ...options
    });
  }

  /**
   * PUT request
   */
  async put(endpoint, data = {}, options = {}) {
    return this.fetch(endpoint, {
      method: 'PUT',
      body: JSON.stringify(data),
      ...options
    });
  }

  /**
   * DELETE request
   */
  async delete(endpoint, options = {}) {
    return this.fetch(endpoint, {
      method: 'DELETE',
      ...options
    });
  }

  /**
   * Manual recovery for circuit breaker
   */
  resetEndpointCircuitBreaker(endpoint) {
    const url = `${this.baseURL}${endpoint}`;
    resetCircuitBreaker(url);
  }
}

// Create singleton instance
const apiService = new APIService();

export default apiService;

// Export specific methods for backwards compatibility
export const API = {
  get: (endpoint, params, options) => apiService.get(endpoint, params, options),
  post: (endpoint, data, options) => apiService.post(endpoint, data, options),
  put: (endpoint, data, options) => apiService.put(endpoint, data, options),
  delete: (endpoint, options) => apiService.delete(endpoint, options),
  resetCircuitBreaker: (endpoint) => apiService.resetEndpointCircuitBreaker(endpoint)
};