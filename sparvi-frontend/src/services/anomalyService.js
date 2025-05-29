import { getSession } from '../api/supabase';

// Fixed API base URL to be consistent with enhancedApiService
const API_BASE_URL = process.env.NODE_ENV === 'development'
  ? 'http://localhost:5000/api'
  : process.env.REACT_APP_API_BASE_URL || '/api';

console.log('🔍 AnomalyService Configuration Debug:');
console.log('  NODE_ENV:', process.env.NODE_ENV);
console.log('  REACT_APP_API_BASE_URL:', process.env.REACT_APP_API_BASE_URL);
console.log('  API_BASE_URL:', API_BASE_URL);

class AnomalyService {
  constructor() {
    this.baseURL = API_BASE_URL;
    console.log('🔍 AnomalyService constructor - baseURL:', this.baseURL);
  }

  async makeRequest(endpoint, options = {}) {
    // Add debug logging here too
    console.log('🔍 makeRequest Debug:');
    console.log('  this.baseURL:', this.baseURL);
    console.log('  endpoint:', endpoint);
    console.log('  final URL:', `${this.baseURL}${endpoint}`);

    try {
      const session = await getSession();
      const token = session?.access_token;

      if (!token) {
        throw new Error('Authentication required');
      }

      const url = `${this.baseURL}${endpoint}`;
      const requestOptions = {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
          ...options.headers
        },
        ...options
      };

      console.log(`Making ${options.method || 'GET'} request to ${url}`);

      const response = await fetch(url, requestOptions);

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`API Error: ${response.status} ${response.statusText}`, errorText);

        // Try to parse as JSON, fallback to text
        let errorData;
        try {
          errorData = JSON.parse(errorText);
        } catch (e) {
          errorData = { error: errorText || `HTTP ${response.status}` };
        }

        throw new Error(errorData.error || `Request failed with status ${response.status}`);
      }

      const data = await response.json();
      console.log(`Response from ${endpoint}:`, data);
      return data;
    } catch (error) {
      console.error(`Error in ${endpoint}:`, error);
      throw error;
    }
  }

  // Get anomaly configurations for a connection
  async getConfigs(connectionId, options = {}) {
    const { table_name, metric_name } = options;
    const params = new URLSearchParams();

    if (table_name) params.append('table_name', table_name);
    if (metric_name) params.append('metric_name', metric_name);

    const endpoint = `/connections/${connectionId}/anomalies/configs${params.toString() ? `?${params.toString()}` : ''}`;

    const response = await this.makeRequest(endpoint);
    return response.configs || [];
  }

  // Get a specific configuration
  async getConfig(connectionId, configId) {
    const endpoint = `/connections/${connectionId}/anomalies/configs/${configId}`;
    const response = await this.makeRequest(endpoint);
    return response.config;
  }

  // Create a new configuration
  async createConfig(connectionId, configData) {
    const endpoint = `/connections/${connectionId}/anomalies/configs`;
    const response = await this.makeRequest(endpoint, {
      method: 'POST',
      body: JSON.stringify(configData)
    });
    return response.config;
  }

  // Update a configuration
  async updateConfig(connectionId, configId, configData) {
    const endpoint = `/connections/${connectionId}/anomalies/configs/${configId}`;
    const response = await this.makeRequest(endpoint, {
      method: 'PUT',
      body: JSON.stringify(configData)
    });
    return response.config;
  }

  // Delete a configuration
  async deleteConfig(connectionId, configId) {
    const endpoint = `/connections/${connectionId}/anomalies/configs/${configId}`;
    await this.makeRequest(endpoint, {
      method: 'DELETE'
    });
    return true;
  }

  // Get anomalies for a connection
  async getAnomalies(connectionId, options = {}) {
    const { table_name, status, days = 30, limit = 100 } = options;
    const params = new URLSearchParams();

    if (table_name) params.append('table_name', table_name);
    if (status) params.append('status', status);
    params.append('days', days.toString());
    params.append('limit', limit.toString());

    const endpoint = `/connections/${connectionId}/anomalies?${params.toString()}`;
    const response = await this.makeRequest(endpoint);
    return response.anomalies || [];
  }

  // Get a specific anomaly
  async getAnomaly(connectionId, anomalyId) {
    const endpoint = `/connections/${connectionId}/anomalies/${anomalyId}`;
    const response = await this.makeRequest(endpoint);
    return response.anomaly;
  }

  // Update anomaly status
  async updateAnomalyStatus(connectionId, anomalyId, status, resolutionNote = null) {
    const endpoint = `/connections/${connectionId}/anomalies/${anomalyId}/status`;
    const response = await this.makeRequest(endpoint, {
      method: 'PUT',
      body: JSON.stringify({
        status,
        resolution_note: resolutionNote
      })
    });
    return response.anomaly;
  }

  // Run anomaly detection manually
  async runDetection(connectionId, options = {}) {
    const endpoint = `/connections/${connectionId}/anomalies/detect`;
    const response = await this.makeRequest(endpoint, {
      method: 'POST',
      body: JSON.stringify(options)
    });
    return response;
  }

  // Get anomaly summary
  async getSummary(connectionId, days = 30) {
    const params = new URLSearchParams();
    params.append('days', days.toString());

    const endpoint = `/connections/${connectionId}/anomalies/summary?${params.toString()}`;
    return await this.makeRequest(endpoint);
  }

  // Get dashboard data
  async getDashboardData(connectionId, days = 30) {
    const params = new URLSearchParams();
    params.append('days', days.toString());

    const endpoint = `/connections/${connectionId}/anomalies/dashboard?${params.toString()}`;
    return await this.makeRequest(endpoint);
  }

  // Get historical metrics for anomaly analysis
  async getHistoricalMetrics(connectionId, options = {}) {
    const { metric_name, table_name, column_name, days = 30 } = options;
    const params = new URLSearchParams();

    if (metric_name) params.append('metric_name', metric_name);
    if (table_name) params.append('table_name', table_name);
    if (column_name) params.append('column_name', column_name);
    params.append('days', days.toString());

    const endpoint = `/connections/${connectionId}/analytics/historical-metrics?${params.toString()}`;
    const response = await this.makeRequest(endpoint);
    return response.metrics || [];
  }

  // Helper methods for formatting and processing
  formatMetricValue(value) {
    if (value === null || value === undefined) return 'N/A';

    if (!isNaN(parseFloat(value))) {
      const num = parseFloat(value);
      if (num === Math.floor(num)) {
        return num.toFixed(0);
      } else {
        return num.toFixed(2);
      }
    }

    return String(value);
  }

  getSeverityColor(severity) {
    switch (severity) {
      case 'high':
        return 'text-red-600';
      case 'medium':
        return 'text-yellow-500';
      case 'low':
        return 'text-green-500';
      default:
        return 'text-gray-400';
    }
  }

  getStatusBadgeStyle(status) {
    switch (status) {
      case 'open':
        return 'bg-blue-100 text-blue-800';
      case 'acknowledged':
        return 'bg-yellow-100 text-yellow-800';
      case 'resolved':
        return 'bg-green-100 text-green-800';
      case 'expected':
        return 'bg-gray-100 text-gray-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  }
}

export default new AnomalyService();