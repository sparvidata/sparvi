/**
 * Utility functions for processing validation results
 */

/**
 * Process raw validation results from the API
 * @param {Array} results - Raw validation results from API
 * @param {Array} validationRules - Existing validation rules
 * @returns {Object} Processed results with metrics
 */
export const processValidationResults = (results, validationRules = []) => {
  if (!results || !Array.isArray(results)) {
    return {
      success: false,
      message: 'Invalid results format',
      processed: [],
      metrics: createEmptyMetrics()
    };
  }

  // Create a map of validation rules by name for faster lookup
  const rulesByName = {};
  if (validationRules.length > 0) {
    validationRules.forEach(rule => {
      if (rule.rule_name) {
        rulesByName[rule.rule_name] = rule;
      }
    });
  }

  // Process each result
  const processed = results.map(result => {
    // Get the associated rule if available
    const rule = result.rule_name ? rulesByName[result.rule_name] : null;

    return {
      ...result,
      // Add rule details if available
      query: result.query || (rule ? rule.query : null),
      description: result.description || (rule ? rule.description : null),
      // Format status consistently
      status: getResultStatus(result),
      // Add timestamps if not present
      timestamp: result.timestamp || result.last_run_at || new Date().toISOString(),
      // Add additional metadata
      meta: {
        execution_time_ms: result.execution_time_ms || null,
        original_rule: rule || null
      }
    };
  });

  // Calculate metrics
  const metrics = calculateMetrics(processed);

  return {
    success: true,
    message: `Processed ${processed.length} validation results`,
    processed,
    metrics
  };
};

/**
 * Determine the status of a validation result
 * @param {Object} result - Validation result
 * @returns {string} Status (passed, failed, error, unknown)
 */
export const getResultStatus = (result) => {
  if (result.error) return 'error';
  if (result.is_valid === true) return 'passed';
  if (result.is_valid === false) return 'failed';
  return 'unknown';
};

/**
 * Calculate metrics for validation results
 * @param {Array} results - Processed validation results
 * @returns {Object} Metrics
 */
export const calculateMetrics = (results) => {
  const metrics = createEmptyMetrics();

  // Exit early if no results
  if (!results || results.length === 0) {
    return metrics;
  }

  metrics.total = results.length;

  // Count results by status
  results.forEach(result => {
    const status = getResultStatus(result);
    metrics.counts[status]++;

    // Track by category if available
    const category = result.category || 'uncategorized';
    if (!metrics.byCategory[category]) {
      metrics.byCategory[category] = createEmptyStatusCounts();
    }
    metrics.byCategory[category][status]++;

    // Track execution times
    if (result.meta?.execution_time_ms) {
      metrics.execution_times.push(result.meta.execution_time_ms);
      metrics.total_execution_time += result.meta.execution_time_ms;
    }
  });

  // Calculate health score (percentage of passed validations)
  const validResults = metrics.counts.passed + metrics.counts.failed;
  if (validResults > 0) {
    metrics.health_score = Math.round((metrics.counts.passed / validResults) * 100);
  }

  // Calculate average execution time
  if (metrics.execution_times.length > 0) {
    metrics.avg_execution_time = metrics.total_execution_time / metrics.execution_times.length;
  }

  // Find slowest validations
  if (metrics.execution_times.length > 0) {
    const sortedByTime = [...results]
      .filter(r => r.meta?.execution_time_ms)
      .sort((a, b) => b.meta.execution_time_ms - a.meta.execution_time_ms);

    metrics.slowest_validations = sortedByTime.slice(0, 3).map(r => ({
      name: r.rule_name,
      execution_time_ms: r.meta.execution_time_ms
    }));
  }

  return metrics;
};

/**
 * Create empty metrics object
 * @returns {Object} Empty metrics
 */
const createEmptyMetrics = () => ({
  total: 0,
  counts: createEmptyStatusCounts(),
  byCategory: {},
  health_score: 0,
  execution_times: [],
  total_execution_time: 0,
  avg_execution_time: 0,
  slowest_validations: []
});

/**
 * Create empty status counts object
 * @returns {Object} Empty status counts
 */
const createEmptyStatusCounts = () => ({
  passed: 0,
  failed: 0,
  error: 0,
  unknown: 0
});

/**
 * Compare current validation results with historical results
 * @param {Array} currentResults - Current validation results
 * @param {Array} historicalResults - Historical validation results
 * @returns {Object} Comparison metrics
 */
export const compareWithHistory = (currentResults, historicalResults) => {
  if (!currentResults || !historicalResults) {
    return null;
  }

  // Process both result sets
  const current = processValidationResults(currentResults).metrics;
  const historical = processValidationResults(historicalResults).metrics;

  // Calculate changes
  const changes = {
    health_score_change: current.health_score - historical.health_score,
    passed_change: current.counts.passed - historical.counts.passed,
    failed_change: current.counts.failed - historical.counts.failed,
    error_change: current.counts.error - historical.counts.error,
    total_change: current.total - historical.total,
    improved: current.health_score > historical.health_score,
    regressed: current.health_score < historical.health_score,
    execution_time_change: current.avg_execution_time - historical.avg_execution_time
  };

  return {
    current,
    historical,
    changes
  };
};

/**
 * Group validation results by table
 * @param {Array} results - Processed validation results
 * @returns {Object} Results grouped by table
 */
export const groupResultsByTable = (results) => {
  const grouped = {};

  if (!results || results.length === 0) return grouped;

  results.forEach(result => {
    const table = result.table_name || 'unknown';

    if (!grouped[table]) {
      grouped[table] = [];
    }

    grouped[table].push(result);
  });

  // Calculate metrics for each table
  Object.keys(grouped).forEach(table => {
    grouped[table] = {
      results: grouped[table],
      metrics: calculateMetrics(grouped[table])
    };
  });

  return grouped;
};

/**
 * Get trend data from historical validation results
 * @param {Array} historicalData - Array of historical validation results with timestamps
 * @param {number} days - Number of days to include
 * @returns {Object} Trend data
 */
export const getValidationTrends = (historicalData, days = 7) => {
  if (!historicalData || historicalData.length === 0) {
    return { trend: [], days };
  }

  // Group results by date
  const now = new Date();
  const startDate = new Date();
  startDate.setDate(now.getDate() - days);

  // Create date buckets for the specified number of days
  const dateBuckets = {};
  for (let i = 0; i <= days; i++) {
    const date = new Date();
    date.setDate(now.getDate() - i);
    const dateStr = date.toISOString().split('T')[0];
    dateBuckets[dateStr] = {
      date: dateStr,
      total: 0,
      passed: 0,
      failed: 0,
      error: 0,
      health_score: 0
    };
  }

  // Process historical data
  historicalData.forEach(item => {
    if (!item.timestamp) return;

    const date = new Date(item.timestamp);
    const dateStr = date.toISOString().split('T')[0];

    // Skip if not in our date range
    if (date < startDate || date > now) return;

    // Create bucket if it doesn't exist
    if (!dateBuckets[dateStr]) {
      dateBuckets[dateStr] = {
        date: dateStr,
        total: 0,
        passed: 0,
        failed: 0,
        error: 0,
        health_score: 0
      };
    }

    // Update counts
    dateBuckets[dateStr].total++;
    const status = getResultStatus(item);
    dateBuckets[dateStr][status]++;
  });

  // Calculate health scores
  Object.values(dateBuckets).forEach(bucket => {
    const validResults = bucket.passed + bucket.failed;
    if (validResults > 0) {
      bucket.health_score = Math.round((bucket.passed / validResults) * 100);
    }
  });

  // Convert to array and sort by date
  const trendArray = Object.values(dateBuckets)
    .sort((a, b) => a.date.localeCompare(b.date));

  return {
    trend: trendArray,
    days
  };
};

export default {
  processValidationResults,
  getResultStatus,
  calculateMetrics,
  compareWithHistory,
  groupResultsByTable,
  getValidationTrends
};