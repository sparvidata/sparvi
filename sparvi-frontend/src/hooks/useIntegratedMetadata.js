import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { metadataIntegrationService } from '../services/metadataIntegrationService';

/**
 * Custom hook for fetching integrated metadata that combines tables, columns, and statistics
 * @param {string} connectionId - Database connection ID
 * @param {Object} options - Configuration options
 * @returns {Object} Query result with integrated metadata
 */
export const useIntegratedMetadata = (connectionId, options = {}) => {
  const {
    enabled = true,
    includeStatistics = true,
    includeColumns = true,
    refetchInterval = false,
    staleTime = 5 * 60 * 1000, // 5 minutes
    forceFresh = false
  } = options;

  return useQuery({
    queryKey: ['integratedMetadata', connectionId, { includeStatistics, includeColumns }],
    queryFn: () => metadataIntegrationService.getIntegratedMetadata(connectionId, {
      includeStatistics,
      includeColumns,
      forceFresh
    }),
    enabled: enabled && !!connectionId,
    staleTime,
    refetchInterval,
    retry: 2,
    retryDelay: 3000,
    // Keep previous data while refetching to prevent UI flickering
    keepPreviousData: true
  });
};

/**
 * Hook for getting enhanced information about a specific table
 * @param {string} connectionId - Database connection ID
 * @param {string} tableName - Name of the table
 * @param {Object} options - Configuration options
 * @returns {Object} Query result with enhanced table information
 */
export const useEnhancedTableInfo = (connectionId, tableName, options = {}) => {
  const {
    enabled = true,
    staleTime = 10 * 60 * 1000, // 10 minutes
  } = options;

  return useQuery({
    queryKey: ['enhancedTableInfo', connectionId, tableName],
    queryFn: () => metadataIntegrationService.getEnhancedTableInfo(connectionId, tableName),
    enabled: enabled && !!connectionId && !!tableName,
    staleTime,
    retry: 2,
    retryDelay: 3000
  });
};

/**
 * Hook for getting tables with progressive loading states
 * @param {string} connectionId - Database connection ID
 * @param {Object} options - Configuration options
 * @returns {Object} Tables with loading states and data availability flags
 */
export const useProgressiveMetadata = (connectionId, options = {}) => {
  const {
    enabled = true,
    refetchInterval = false
  } = options;

  // First, get just tables quickly
  const tablesQuery = useQuery({
    queryKey: ['metadata', connectionId, 'tables'],
    queryFn: () => metadataIntegrationService.getIntegratedMetadata(connectionId, {
      includeColumns: false,
      includeStatistics: false
    }),
    enabled: enabled && !!connectionId,
    staleTime: 2 * 60 * 1000, // 2 minutes
    keepPreviousData: true
  });

  // Then enhance with full data
  const fullQuery = useQuery({
    queryKey: ['integratedMetadata', connectionId, 'full'],
    queryFn: () => metadataIntegrationService.getIntegratedMetadata(connectionId, {
      includeColumns: true,
      includeStatistics: true
    }),
    enabled: enabled && !!connectionId && tablesQuery.isSuccess,
    staleTime: 5 * 60 * 1000, // 5 minutes
    refetchInterval,
    keepPreviousData: true
  });

  // Determine what data is available
  const hasBasicTables = tablesQuery.isSuccess && tablesQuery.data?.success;
  const hasFullData = fullQuery.isSuccess && fullQuery.data?.success;
  const isLoadingInitial = tablesQuery.isLoading;
  const isEnhancing = !fullQuery.isLoading && tablesQuery.isSuccess && !fullQuery.isSuccess;

  // Choose the best available data
  const bestAvailableData = hasFullData ? fullQuery.data : hasBasicTables ? tablesQuery.data : null;

  return {
    // Data states
    data: bestAvailableData?.data,
    tables: bestAvailableData?.data?.tables || [],
    columns: bestAvailableData?.data?.columns || [],
    statistics: bestAvailableData?.data?.statistics || [],
    summary: bestAvailableData?.data?.summary,

    // Loading states
    isLoadingInitial,
    isEnhancing,
    isFullyLoaded: hasFullData,

    // Data availability flags
    hasBasicTables,
    hasColumns: hasFullData && (bestAvailableData?.data?.columns?.length > 0),
    hasStatistics: hasFullData && (bestAvailableData?.data?.statistics?.length > 0),

    // Error states
    error: fullQuery.error || tablesQuery.error,
    isError: fullQuery.isError || tablesQuery.isError,

    // Freshness information
    freshness: bestAvailableData?.freshness,
    errors: bestAvailableData?.errors || [],

    // Refetch functions
    refetch: () => {
      tablesQuery.refetch();
      fullQuery.refetch();
    },
    refetchTables: tablesQuery.refetch,
    refetchFull: fullQuery.refetch,

    // Raw query objects for advanced use cases
    tablesQuery,
    fullQuery
  };
};

/**
 * Hook for getting metadata summary statistics
 * @param {string} connectionId - Database connection ID
 * @param {Object} options - Configuration options
 * @returns {Object} Summary statistics about the metadata
 */
export const useMetadataSummary = (connectionId, options = {}) => {
  const { data, isLoading, error } = useIntegratedMetadata(connectionId, {
    includeStatistics: true,
    includeColumns: true,
    ...options
  });

  const summary = React.useMemo(() => {
    if (!data?.success || !data?.data) {
      return null;
    }

    const { tables, columns, statistics } = data.data;

    return {
      // Basic counts
      totalTables: tables.length,
      totalColumns: columns.length,
      totalStatistics: statistics.length,

      // Data quality metrics
      tablesWithData: tables.filter(t => (t.row_count || 0) > 0).length,
      emptyTables: tables.filter(t => (t.row_count || 0) === 0).length,

      // Row count statistics
      totalRows: tables.reduce((sum, t) => sum + (t.row_count || 0), 0),
      largestTable: tables.reduce((largest, current) =>
        (current.row_count || 0) > (largest?.row_count || 0) ? current : largest, null),

      // Column statistics
      nullableColumns: columns.filter(c => c.nullable).length,
      nonNullableColumns: columns.filter(c => !c.nullable).length,

      // Data type distribution
      columnTypeDistribution: columns.reduce((acc, col) => {
        const type = col.type || col.data_type || 'unknown';
        acc[type] = (acc[type] || 0) + 1;
        return acc;
      }, {}),

      // Health indicators
      tablesWithPrimaryKeys: tables.filter(t => t.has_primary_key).length,
      averageHealthScore: tables.length > 0 ?
        tables.reduce((sum, t) => sum + (t.health_score || 0), 0) / tables.length : 0,

      // Freshness
      freshness: data.freshness,
      lastUpdated: Math.max(...tables.map(t => new Date(t.last_analyzed || 0).getTime()), 0)
    };
  }, [data]);

  return {
    summary,
    isLoading,
    error,
    isSuccess: !!summary
  };
};