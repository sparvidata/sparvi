// src/services/metadataIntegrationService.js
import { metadataAPI } from '../api/enhancedApiService';

/**
 * Service for integrating and transforming metadata from different sources
 * Handles the complex nested API response structures and combines them into unified data
 */
class MetadataIntegrationService {

  constructor() {
    // Cache for table row counts extracted from statistics
    this._tableRowCounts = {};
  }

  /**
   * Fetch and integrate all metadata types for a connection
   * @param {string} connectionId - Database connection ID
   * @param {Object} options - Configuration options
   * @returns {Promise<Object>} Integrated metadata object
   */
  async getIntegratedMetadata(connectionId, options = {}) {
    const {
      includeStatistics = true,
      includeColumns = true,
      forceFresh = false
    } = options;

    try {
      console.log(`[MetadataIntegration] Fetching integrated metadata for connection ${connectionId}`);

      // Fetch all metadata types in parallel
      const promises = [
        metadataAPI.getMetadata(connectionId, 'tables', { forceFresh })
      ];

      if (includeColumns) {
        promises.push(metadataAPI.getMetadata(connectionId, 'columns', { forceFresh }));
      }

      if (includeStatistics) {
        promises.push(metadataAPI.getMetadata(connectionId, 'statistics', { forceFresh }));
      }

      const results = await Promise.allSettled(promises);

      // Extract results, handling any failures gracefully
      const tablesResult = results[0];
      const columnsResult = includeColumns ? results[1] : null;
      const statisticsResult = includeStatistics ? results[includeColumns ? 2 : 1] : null;

      // Extract raw metadata from API responses
      const rawTablesMetadata = tablesResult.status === 'fulfilled' ? tablesResult.value : null;
      const rawColumnsMetadata = columnsResult?.status === 'fulfilled' ? columnsResult.value : null;
      const rawStatisticsMetadata = statisticsResult?.status === 'fulfilled' ? statisticsResult.value : null;

      console.log('[MetadataIntegration] Raw API responses:', {
        tables: rawTablesMetadata,
        columns: rawColumnsMetadata,
        statistics: rawStatisticsMetadata
      });

      // Transform and integrate the metadata
      const integratedMetadata = this.integrateMetadata({
        tables: rawTablesMetadata,
        columns: rawColumnsMetadata,
        statistics: rawStatisticsMetadata
      });

      console.log('[MetadataIntegration] Integrated metadata:', integratedMetadata);

      return {
        success: true,
        data: integratedMetadata,
        errors: this.collectErrors(results),
        freshness: this.calculateFreshness([rawTablesMetadata, rawColumnsMetadata, rawStatisticsMetadata])
      };

    } catch (error) {
      console.error('[MetadataIntegration] Error fetching integrated metadata:', error);
      return {
        success: false,
        data: { tables: [], columns: [], statistics: [] },
        errors: [error.message],
        freshness: { status: 'error', age_seconds: null }
      };
    }
  }

  /**
   * Integrate metadata from different sources into unified structures
   * @param {Object} rawMetadata - Raw metadata from API responses
   * @returns {Object} Integrated metadata
   */
  integrateMetadata({ tables, columns, statistics }) {
    // Extract tables
    const extractedTables = this.extractTables(tables);
    const extractedColumns = this.extractColumns(columns);
    const extractedStatistics = this.extractStatistics(statistics);

    console.log('[MetadataIntegration] Extracted data:', {
      tables: extractedTables.length,
      columns: extractedColumns.length,
      statistics: extractedStatistics.length
    });

    // Create lookup maps for efficient integration
    const columnsByTable = this.createColumnsByTableMap(extractedColumns);
    const statisticsByTable = this.createStatisticsByTableMap(extractedStatistics);

    // Integrate tables with their columns and statistics
    const integratedTables = extractedTables.map(table => {
      const tableColumns = columnsByTable[table.name] || [];
      const tableStatistics = statisticsByTable[table.name] || {};

      // Get row count from statistics (this is where the row count actually lives)
      const rowCount = this._tableRowCounts?.[table.name] || tableStatistics.row_count || table.row_count || 0;

      return {
        ...table,
        // Enhanced column information
        columns: tableColumns,
        column_count: tableColumns.length || table.column_count || 0,

        // Enhanced row count from statistics (this is the key fix)
        row_count: rowCount,

        // Additional statistics
        health_score: tableStatistics.health_score,
        has_primary_key: tableStatistics.has_primary_key || false,
        primary_keys: tableStatistics.primary_keys || table.primary_key || [],

        // Column type distribution
        column_type_distribution: tableStatistics.column_type_distribution || {},

        // Nullable/non-nullable column counts
        nullable_columns: tableStatistics.nullable_columns || 0,
        non_nullable_columns: tableStatistics.non_nullable_columns || 0,

        // Table metadata
        size_bytes: tableStatistics.size_bytes,
        last_analyzed: tableStatistics.collected_at
      };
    });

    // Enhance columns with statistics
    const integratedColumns = extractedColumns.map(column => {
      const columnStats = extractedStatistics.find(stat =>
        stat.table_name === column.table_name && stat.column_name === column.name
      );

      return {
        ...column,
        // Add statistical information
        null_count: columnStats?.null_count,
        null_percentage: columnStats?.null_percentage,
        distinct_count: columnStats?.distinct_count,
        distinct_percentage: columnStats?.distinct_percentage,
        is_unique: columnStats?.is_unique,

        // Numeric statistics
        min_value: columnStats?.min,
        max_value: columnStats?.max,
        avg_value: columnStats?.avg,

        // String statistics
        min_length: columnStats?.min,
        max_length: columnStats?.max,
        avg_length: columnStats?.avg
      };
    });

    return {
      tables: integratedTables,
      columns: integratedColumns,
      statistics: extractedStatistics,
      summary: {
        total_tables: integratedTables.length,
        total_columns: integratedColumns.length,
        total_statistics: extractedStatistics.length,
        tables_with_data: integratedTables.filter(t => t.row_count > 0).length,
        largest_table: this.findLargestTable(integratedTables),
        column_types: this.summarizeColumnTypes(integratedColumns)
      }
    };
  }

  /**
   * Extract tables from API response
   * @param {Object} tablesResponse - API response containing tables
   * @returns {Array} Array of table objects
   */
  extractTables(tablesResponse) {
    if (!tablesResponse) return [];

    let extractedTables = [];

    // Handle different nesting levels in the API response
    const possiblePaths = [
      tablesResponse?.metadata?.metadata?.tables,
      tablesResponse?.metadata?.metadata?.metadata?.tables,
      tablesResponse?.metadata?.tables,
      tablesResponse?.tables,
      tablesResponse?.metadata,
      tablesResponse
    ];

    for (const path of possiblePaths) {
      if (path && Array.isArray(path)) {
        extractedTables = path;
        break;
      } else if (path && typeof path === 'object' && !Array.isArray(path)) {
        // Convert object to array if it's a key-value structure
        extractedTables = Object.keys(path).map(key => ({
          name: key,
          id: path[key].id || `table_${key}`,
          ...path[key]
        }));
        break;
      }
    }

    console.log('[MetadataIntegration] Extracted tables:', extractedTables.length);
    return extractedTables;
  }

  /**
   * Extract columns from API response
   * @param {Object} columnsResponse - API response containing columns
   * @returns {Array} Array of column objects
   */
  extractColumns(columnsResponse) {
    if (!columnsResponse) return [];

    let extractedColumns = [];

    // Handle columns_by_table structure (new format)
    const columnsByTable = columnsResponse?.metadata?.metadata?.columns_by_table ||
                          columnsResponse?.metadata?.columns_by_table ||
                          columnsResponse?.columns_by_table;

    if (columnsByTable) {
      Object.entries(columnsByTable).forEach(([tableName, columns]) => {
        if (Array.isArray(columns)) {
          columns.forEach(column => {
            extractedColumns.push({
              ...column,
              table_name: tableName
            });
          });
        }
      });
    } else {
      // Handle flat column structure (older format)
      const possiblePaths = [
        columnsResponse?.metadata?.metadata?.columns,
        columnsResponse?.metadata?.columns,
        columnsResponse?.columns,
        columnsResponse?.metadata,
        columnsResponse
      ];

      for (const path of possiblePaths) {
        if (path && Array.isArray(path)) {
          extractedColumns = path;
          break;
        }
      }
    }

    console.log('[MetadataIntegration] Extracted columns:', extractedColumns.length);
    return extractedColumns;
  }

  /**
   * Extract statistics from API response
   * @param {Object} statisticsResponse - API response containing statistics
   * @returns {Array} Array of statistics objects
   */
  extractStatistics(statisticsResponse) {
    if (!statisticsResponse) return [];

    let extractedStatistics = [];
    let tableRowCounts = {}; // To track row counts per table

    // Handle statistics_by_table structure (new format)
    const statisticsByTable = statisticsResponse?.metadata?.metadata?.statistics_by_table ||
                             statisticsResponse?.metadata?.statistics_by_table ||
                             statisticsResponse?.statistics_by_table;

    if (statisticsByTable) {
      Object.entries(statisticsByTable).forEach(([tableName, tableData]) => {
        // Store table-level row count
        if (tableData.row_count !== undefined) {
          tableRowCounts[tableName] = tableData.row_count;
        }

        // Process column statistics
        if (tableData.column_statistics) {
          Object.entries(tableData.column_statistics).forEach(([columnName, columnStats]) => {
            const basicStats = columnStats.basic || {};
            const numericStats = columnStats.numeric || {};
            const stringStats = columnStats.string || {};

            extractedStatistics.push({
              table_name: tableName,
              column_name: columnName,
              data_type: columnStats.type || 'unknown',

              // Basic statistics
              null_count: basicStats.null_count,
              null_percentage: basicStats.null_percentage,
              distinct_count: basicStats.distinct_count,
              distinct_percentage: basicStats.distinct_percentage,
              is_unique: basicStats.is_unique,

              // Table-level information
              row_count: tableData.row_count || 0,

              // Type-specific statistics
              min: numericStats.min || stringStats.min_length,
              max: numericStats.max || stringStats.max_length,
              avg: numericStats.avg || stringStats.avg_length,

              // Additional numeric stats
              sum: numericStats.sum,
              stddev: numericStats.stddev,

              // Additional string stats
              empty_count: stringStats.empty_count,

              // Metadata
              collected_at: tableData.collected_at
            });
          });
        }
      });

      // Store table row counts for use in integration
      this._tableRowCounts = tableRowCounts;
    } else {
      // Handle flat statistics structure (older format)
      const possiblePaths = [
        statisticsResponse?.metadata?.metadata?.statistics,
        statisticsResponse?.metadata?.statistics,
        statisticsResponse?.statistics,
        statisticsResponse?.metadata,
        statisticsResponse
      ];

      for (const path of possiblePaths) {
        if (path && Array.isArray(path)) {
          extractedStatistics = path;
          break;
        }
      }
    }

    console.log('[MetadataIntegration] Extracted statistics:', extractedStatistics.length);
    console.log('[MetadataIntegration] Table row counts:', this._tableRowCounts);
    return extractedStatistics;
  }

  /**
   * Create a map of columns grouped by table name
   * @param {Array} columns - Array of column objects
   * @returns {Object} Map of table name to columns array
   */
  createColumnsByTableMap(columns) {
    const columnsByTable = {};

    columns.forEach(column => {
      const tableName = column.table_name;
      if (!columnsByTable[tableName]) {
        columnsByTable[tableName] = [];
      }
      columnsByTable[tableName].push(column);
    });

    return columnsByTable;
  }

  /**
   * Create a map of statistics grouped by table name
   * @param {Array} statistics - Array of statistics objects
   * @returns {Object} Map of table name to aggregated statistics
   */
  createStatisticsByTableMap(statistics) {
    const statisticsByTable = {};

    statistics.forEach(stat => {
      const tableName = stat.table_name;
      if (!statisticsByTable[tableName]) {
        statisticsByTable[tableName] = {
          row_count: stat.row_count || this._tableRowCounts?.[tableName] || 0, // Use cached row count
          columns_analyzed: 0,
          health_score: 65, // Default health score
          has_primary_key: false,
          primary_keys: [],
          column_type_distribution: {},
          nullable_columns: 0,
          non_nullable_columns: 0,
          collected_at: stat.collected_at
        };
      }

      const tableStats = statisticsByTable[tableName];
      tableStats.columns_analyzed++;

      // Track column type distribution
      if (stat.data_type) {
        const typeCategory = this.categorizeDataType(stat.data_type);
        tableStats.column_type_distribution[typeCategory] = (tableStats.column_type_distribution[typeCategory] || 0) + 1;
      }

      // Track nullable columns
      if (stat.null_percentage !== undefined) {
        if (stat.null_percentage === 0) {
          tableStats.non_nullable_columns++;
        } else {
          tableStats.nullable_columns++;
        }
      }

      // Update health score based on data quality indicators
      if (stat.null_percentage > 50) {
        tableStats.health_score = Math.max(0, tableStats.health_score - 10);
      }
      if (stat.is_unique) {
        tableStats.health_score = Math.min(100, tableStats.health_score + 5);
      }
    });

    return statisticsByTable;
  }

  /**
   * Categorize data type into broader categories
   * @param {string} dataType - Specific data type
   * @returns {string} Category name
   */
  categorizeDataType(dataType) {
    const type = dataType.toLowerCase();

    if (type.includes('varchar') || type.includes('text') || type.includes('char')) {
      return 'text';
    } else if (type.includes('decimal') || type.includes('numeric') || type.includes('int') || type.includes('float')) {
      return 'numeric';
    } else if (type.includes('date') || type.includes('time')) {
      return 'datetime';
    } else if (type.includes('bool')) {
      return 'boolean';
    } else {
      return 'other';
    }
  }

  /**
   * Find the largest table by row count
   * @param {Array} tables - Array of table objects
   * @returns {Object|null} Largest table or null if none found
   */
  findLargestTable(tables) {
    if (!tables || tables.length === 0) return null;

    return tables.reduce((largest, current) => {
      return (current.row_count || 0) > (largest.row_count || 0) ? current : largest;
    });
  }

  /**
   * Summarize column types across all tables
   * @param {Array} columns - Array of column objects
   * @returns {Object} Summary of column types
   */
  summarizeColumnTypes(columns) {
    const typeSummary = {};

    columns.forEach(column => {
      const type = column.type || column.data_type || 'unknown';
      const category = this.categorizeDataType(type);
      typeSummary[category] = (typeSummary[category] || 0) + 1;
    });

    return typeSummary;
  }

  /**
   * Collect errors from Promise.allSettled results
   * @param {Array} results - Results from Promise.allSettled
   * @returns {Array} Array of error messages
   */
  collectErrors(results) {
    return results
      .filter(result => result.status === 'rejected')
      .map(result => result.reason?.message || 'Unknown error');
  }

  /**
   * Calculate overall freshness from multiple metadata sources
   * @param {Array} metadataResponses - Array of metadata responses
   * @returns {Object} Freshness information
   */
  calculateFreshness(metadataResponses) {
    const validResponses = metadataResponses.filter(Boolean);
    if (validResponses.length === 0) {
      return { status: 'unknown', age_seconds: null };
    }

    // Find the oldest freshness status
    let oldestAgeSeconds = 0;
    let worstStatus = 'fresh';

    validResponses.forEach(response => {
      const freshness = response?.metadata?.freshness || response?.freshness;
      if (freshness) {
        const ageSeconds = freshness.age_seconds || 0;
        const status = freshness.status || 'unknown';

        if (ageSeconds > oldestAgeSeconds) {
          oldestAgeSeconds = ageSeconds;
        }

        // Determine worst status (unknown > stale > recent > fresh)
        const statusPriority = { fresh: 0, recent: 1, stale: 2, unknown: 3, error: 4 };
        if (statusPriority[status] > statusPriority[worstStatus]) {
          worstStatus = status;
        }
      }
    });

    return {
      status: worstStatus,
      age_seconds: oldestAgeSeconds
    };
  }

  /**
   * Get enhanced table information with all integrated data
   * @param {string} connectionId - Database connection ID
   * @param {string} tableName - Name of the table
   * @returns {Promise<Object>} Enhanced table information
   */
  async getEnhancedTableInfo(connectionId, tableName) {
    try {
      const integrated = await this.getIntegratedMetadata(connectionId);

      if (!integrated.success) {
        throw new Error('Failed to get integrated metadata');
      }

      const table = integrated.data.tables.find(t => t.name === tableName);
      if (!table) {
        throw new Error(`Table ${tableName} not found`);
      }

      const tableColumns = integrated.data.columns.filter(c => c.table_name === tableName);
      const tableStatistics = integrated.data.statistics.filter(s => s.table_name === tableName);

      return {
        table,
        columns: tableColumns,
        statistics: tableStatistics,
        summary: {
          total_columns: tableColumns.length,
          nullable_columns: tableColumns.filter(c => c.nullable).length,
          unique_columns: tableStatistics.filter(s => s.is_unique).length,
          columns_with_nulls: tableStatistics.filter(s => s.null_count > 0).length
        }
      };
    } catch (error) {
      console.error(`[MetadataIntegration] Error getting enhanced table info for ${tableName}:`, error);
      throw error;
    }
  }
}

// Export singleton instance
export const metadataIntegrationService = new MetadataIntegrationService();
export default metadataIntegrationService;