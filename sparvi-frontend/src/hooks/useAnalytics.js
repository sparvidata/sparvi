// src/hooks/useAnalytics.js
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { analyticsAPI } from '../api/enhancedApiService';
import { formatDate } from '../utils/formatting';

/**
 * Custom hook to fetch dashboard metrics data
 * @param {string} connectionId - The connection ID
 * @param {Object} options - Additional options for the query
 * @returns {Object} Query result object
 */
export const useAnalyticsDashboard = (connectionId, options = {}) => {
  const {
    enabled = !!connectionId,
    refetchInterval = false,
    days = 30,
    ...queryOptions
  } = options;

  return useQuery({
    queryKey: ['analytics-dashboard', connectionId, days],
    queryFn: () => analyticsAPI.getDashboardMetrics(connectionId, { days, forceFresh: false }),
    enabled: enabled,
    refetchInterval: refetchInterval,
    ...queryOptions,
    select: (data) => {
      console.log("Analytics dashboard data:", data);

      // Handle different response formats
      if (!data) return null;

      // If data is already in the expected format, return it directly
      if (data.row_count_trends || data.recent_metrics) {
        return data;
      }

      // If data has a nested data property, extract it
      if (data.data) {
        return data.data;
      }

      // If data is in some other format, try to normalize it
      return {
        row_count_trends: data.row_count_trends || [],
        validation_trends: data.validation_trends || [],
        schema_change_trends: data.schema_change_trends || [],
        quality_score_trends: data.quality_score_trends || [],
        recent_metrics: data.recent_metrics || []
      };
    },
    refetchOnWindowFocus: false,
    cacheTime: 30 * 60 * 1000,
    staleTime: 10 * 60 * 1000,
  });
};

/**
 * Custom hook to fetch historical metrics data
 * @param {string} connectionId - The connection ID
 * @param {Object} options - Additional options for the query
 * @returns {Object} Query result object
 */
export const useHistoricalMetrics = (connectionId, options = {}) => {
  const {
    enabled = !!connectionId,
    refetchInterval = false,
    metricName,
    tableName,
    columnName,
    days = 30,
    groupByDate = true,
    ...queryOptions
  } = options;

  return useQuery({
    queryKey: ['historical-metrics', connectionId, metricName, tableName, columnName, days],
    queryFn: () => analyticsAPI.getHistoricalMetrics(connectionId, {
      metric_name: metricName,
      table_name: tableName,
      column_name: columnName,
      days,
      group_by_date: groupByDate,
      limit: 100,
    }),
    enabled: enabled && !!connectionId,
    refetchInterval: refetchInterval,
    ...queryOptions,
    select: (data) => {
      // Handle different response formats
      if (!data) return { metrics: [], metrics_by_date: {} };

      // If data has metrics property, it's already in the right format
      if (data.metrics) return data;

      // If data is an array, assume it's the metrics array
      if (Array.isArray(data)) return { metrics: data, metrics_by_date: {} };

      // Last resort - return the data as is
      return data;
    },
    // Don't refetch on window focus
    refetchOnWindowFocus: false,
    // Keep the data for 1 hour
    cacheTime: 60 * 60 * 1000,
    // Consider it stale after 30 minutes
    staleTime: 30 * 60 * 1000,
  });
};

/**
 * Process row count trend data into a format suitable for visualization
 * @param {Array} rowCountData - Raw row count data
 * @returns {Array} Processed data for charts
 */
export const processRowCountTrends = (rowCountData = []) => {
  if (!rowCountData || rowCountData.length === 0) return [];

  // Group row counts by table name and timestamp
  const dataByTable = {};

  rowCountData.forEach(item => {
    const tableName = item.table_name || 'Unknown';
    if (!dataByTable[tableName]) {
      dataByTable[tableName] = [];
    }

    dataByTable[tableName].push({
      timestamp: item.timestamp,
      value: item.metric_value || 0
    });
  });

  // Create series data for each table
  return Object.entries(dataByTable).map(([tableName, data]) => ({
    name: tableName,
    data: data.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp))
  }));
};

/**
 * Custom hook to fetch high impact objects
 * @param {string} connectionId - The connection ID
 * @param {Object} options - Additional options for the query
 * @returns {Object} Query result object
 */
export const useHighImpactObjects = (connectionId, options = {}) => {
  const {
    enabled = !!connectionId,
    refetchInterval = false,
    limit = 10,
    ...queryOptions
  } = options;

  return useQuery({
    queryKey: ['high-impact-objects', connectionId, limit],
    queryFn: () => analyticsAPI.getHighImpactObjects(connectionId, limit, { forceFresh: false }),
    enabled: enabled,
    refetchInterval: refetchInterval,
    ...queryOptions,
    select: (data) => {
      // Convert to standard format
      return {
        objects: data?.objects || data?.data?.objects || [],
        count: data?.count || data?.data?.count || 0
      };
    }
  });
};

/**
 * Custom hook to track a custom metric
 */
export const useTrackMetric = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ connectionId, metrics }) =>
      analyticsAPI.trackMetrics(connectionId, metrics),
    onSuccess: (data, variables) => {
      // Invalidate relevant queries
      queryClient.invalidateQueries(['historical-metrics', variables.connectionId]);
    }
  });
};