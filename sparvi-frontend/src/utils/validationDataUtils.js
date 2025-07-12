/**
 * Validation data processing utilities
 * Ensures data consistency and provides fallbacks for missing data
 */

/**
 * Validate and normalize trends data from API
 * @param {any} rawData - Raw data from API
 * @returns {Object} Normalized trends data with validation results
 */
export const validateAndNormalizeTrendsData = (rawData) => {
  const result = {
    isValid: false,
    data: [],
    errors: [],
    warnings: [],
    metadata: {
      totalDays: 0,
      activeDays: 0,
      dateRange: null
    }
  };

  try {
    // Handle null/undefined
    if (!rawData) {
      result.errors.push('No data received from API');
      return result;
    }

    // Extract trends array from different possible response formats
    let trendsArray = [];

    if (Array.isArray(rawData)) {
      trendsArray = rawData;
    } else if (rawData.trends && Array.isArray(rawData.trends)) {
      trendsArray = rawData.trends;
    } else if (rawData.data && Array.isArray(rawData.data)) {
      trendsArray = rawData.data;
    } else {
      result.errors.push('Invalid data format - expected array of trends');
      return result;
    }

    // Validate and normalize each trend point
    const normalizedTrends = [];
    let activeDays = 0;
    let dateRange = { start: null, end: null };

    trendsArray.forEach((trend, index) => {
      const normalizedTrend = normalizeTrendPoint(trend, index);

      if (normalizedTrend.isValid) {
        normalizedTrends.push(normalizedTrend.data);

        // Count active days
        if (normalizedTrend.data.total_validations > 0) {
          activeDays++;
        }

        // Track date range
        const trendDate = new Date(normalizedTrend.data.day);
        if (!dateRange.start || trendDate < dateRange.start) {
          dateRange.start = trendDate;
        }
        if (!dateRange.end || trendDate > dateRange.end) {
          dateRange.end = trendDate;
        }
      } else {
        result.warnings.push(`Invalid trend point at index ${index}: ${normalizedTrend.errors.join(', ')}`);
      }
    });

    // Set results
    result.data = normalizedTrends;
    result.isValid = normalizedTrends.length > 0;
    result.metadata = {
      totalDays: normalizedTrends.length,
      activeDays,
      dateRange
    };

    // Add warnings for common issues
    if (activeDays === 0) {
      result.warnings.push('No active days found (all days have 0 validations)');
    }

    if (normalizedTrends.length < trendsArray.length) {
      result.warnings.push(`${trendsArray.length - normalizedTrends.length} invalid trend points were excluded`);
    }

    console.log('[validateAndNormalizeTrendsData] Validation result:', result);
    return result;

  } catch (error) {
    console.error('[validateAndNormalizeTrendsData] Validation error:', error);
    result.errors.push(`Validation failed: ${error.message}`);
    return result;
  }
};

/**
 * Normalize a single trend point
 * @param {Object} trend - Raw trend point
 * @param {number} index - Index for error reporting
 * @returns {Object} Normalized trend point with validation result
 */
const normalizeTrendPoint = (trend, index) => {
  const result = {
    isValid: false,
    data: null,
    errors: []
  };

  try {
    // Validate required fields
    if (!trend || typeof trend !== 'object') {
      result.errors.push('Trend point is not an object');
      return result;
    }

    if (!trend.day) {
      result.errors.push('Missing required field: day');
      return result;
    }

    // Validate date format
    const dayDate = new Date(trend.day);
    if (isNaN(dayDate.getTime())) {
      result.errors.push(`Invalid date format: ${trend.day}`);
      return result;
    }

    // Normalize numeric fields with defaults
    const normalizeNumber = (value, defaultValue = 0) => {
      const num = Number(value);
      return isNaN(num) ? defaultValue : Math.max(0, num); // Ensure non-negative
    };

    const normalizedTrend = {
      day: trend.day,
      timestamp: trend.timestamp || `${trend.day}T00:00:00Z`,
      total_validations: normalizeNumber(trend.total_validations),
      passed: normalizeNumber(trend.passed),
      failed: normalizeNumber(trend.failed),
      errored: normalizeNumber(trend.errored),
      not_run: normalizeNumber(trend.not_run),
      health_score: normalizeNumber(trend.health_score)
    };

    // Validate data consistency
    const totalCalculated = normalizedTrend.passed + normalizedTrend.failed + normalizedTrend.errored;

    if (normalizedTrend.total_validations > 0 && totalCalculated === 0) {
      result.errors.push('Invalid data: total_validations > 0 but no passed/failed/errored counts');
      return result;
    }

    // Health score should be 0-100
    if (normalizedTrend.health_score < 0 || normalizedTrend.health_score > 100) {
      normalizedTrend.health_score = Math.max(0, Math.min(100, normalizedTrend.health_score));
    }

    result.isValid = true;
    result.data = normalizedTrend;

    return result;

  } catch (error) {
    result.errors.push(`Normalization error: ${error.message}`);
    return result;
  }
};

/**
 * Calculate enhanced summary metrics from trends data
 * @param {Array} trendsData - Normalized trends data
 * @returns {Object|null} Summary metrics or null if insufficient data
 */
export const calculateEnhancedSummaryMetrics = (trendsData) => {
  try {
    if (!trendsData || trendsData.length === 0) {
      console.log('[calculateEnhancedSummaryMetrics] No trends data provided');
      return null;
    }

    // Filter out days with zero validations for meaningful stats
    const activeDays = trendsData.filter(day => day.total_validations > 0);

    if (activeDays.length === 0) {
      console.log('[calculateEnhancedSummaryMetrics] No active days found');
      return null;
    }

    // Get latest and previous days
    const latestDay = activeDays[activeDays.length - 1];
    const previousDay = activeDays.length > 1 ? activeDays[activeDays.length - 2] : null;

    // Calculate trends with safety checks
    const healthScoreTrend = previousDay
      ? (latestDay.health_score || 0) - (previousDay.health_score || 0)
      : 0;

    // Calculate averages
    const avgHealthScore = activeDays.reduce((sum, day) => sum + (day.health_score || 0), 0) / activeDays.length;
    const avgDailyFailures = activeDays.reduce((sum, day) => sum + (day.failed || 0), 0) / activeDays.length;
    const totalValidationsRun = activeDays.reduce((sum, day) => sum + (day.total_validations || 0), 0);

    // Find days since last failure
    let daysSinceLastFailure = 0;
    for (let i = activeDays.length - 1; i >= 0; i--) {
      if ((activeDays[i].failed || 0) > 0) {
        break;
      }
      daysSinceLastFailure++;
    }

    // Calculate additional insights
    const maxHealthScore = Math.max(...activeDays.map(day => day.health_score || 0));
    const minHealthScore = Math.min(...activeDays.map(day => day.health_score || 0));
    const totalFailures = activeDays.reduce((sum, day) => sum + (day.failed || 0), 0);
    const totalPassed = activeDays.reduce((sum, day) => sum + (day.passed || 0), 0);

    const metrics = {
      // Current status
      currentHealthScore: latestDay.health_score || 0,
      healthScoreTrend: Number(healthScoreTrend.toFixed(1)),

      // Averages
      avgHealthScore: Math.round(avgHealthScore),
      avgDailyFailures: Math.round(avgDailyFailures * 10) / 10,

      // Totals
      totalValidationsRun,
      totalFailures,
      totalPassed,

      // Latest day breakdown
      latestTotalValidations: latestDay.total_validations || 0,
      latestPassed: latestDay.passed || 0,
      latestFailed: latestDay.failed || 0,
      latestNotRun: latestDay.not_run || 0,
      latestErrored: latestDay.errored || 0,

      // Trends
      daysSinceLastFailure,
      activeDays: activeDays.length,
      totalDays: trendsData.length,

      // Additional insights
      maxHealthScore,
      minHealthScore,
      healthScoreVariation: maxHealthScore - minHealthScore,
      successRate: totalValidationsRun > 0 ? Math.round((totalPassed / totalValidationsRun) * 100) : 0
    };

    console.log('[calculateEnhancedSummaryMetrics] Calculated metrics:', metrics);
    return metrics;

  } catch (error) {
    console.error('[calculateEnhancedSummaryMetrics] Error calculating metrics:', error);
    return null;
  }
};

/**
 * Filter trends data for a specific date range
 * @param {Array} trendsData - Trends data array
 * @param {Date} startDate - Start date (inclusive)
 * @param {Date} endDate - End date (inclusive)
 * @returns {Array} Filtered trends data
 */
export const filterTrendsByDateRange = (trendsData, startDate, endDate) => {
  if (!trendsData || trendsData.length === 0) return [];

  return trendsData.filter(trend => {
    const trendDate = new Date(trend.day);
    return trendDate >= startDate && trendDate <= endDate;
  });
};

/**
 * Get trends data for a specific period (e.g., last 7 days, last 30 days)
 * @param {Array} trendsData - Full trends data array
 * @param {number} days - Number of days to include
 * @returns {Array} Filtered trends data
 */
export const getRecentTrends = (trendsData, days = 30) => {
  if (!trendsData || trendsData.length === 0) return [];

  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(endDate.getDate() - days);

  return filterTrendsByDateRange(trendsData, startDate, endDate);
};

/**
 * Detect data quality issues in trends
 * @param {Array} trendsData - Trends data array
 * @returns {Object} Quality assessment with issues and recommendations
 */
export const assessDataQuality = (trendsData) => {
  const assessment = {
    score: 100, // Start with perfect score
    issues: [],
    recommendations: [],
    metadata: {
      totalDays: trendsData.length,
      activeDays: 0,
      gapsDetected: 0,
      inconsistenciesDetected: 0
    }
  };

  if (!trendsData || trendsData.length === 0) {
    assessment.score = 0;
    assessment.issues.push('No trend data available');
    assessment.recommendations.push('Run validations to generate trend data');
    return assessment;
  }

  // Count active days
  assessment.metadata.activeDays = trendsData.filter(day => day.total_validations > 0).length;

  // Detect gaps in data
  let consecutiveEmptyDays = 0;
  let maxGap = 0;

  trendsData.forEach(day => {
    if (day.total_validations === 0) {
      consecutiveEmptyDays++;
      maxGap = Math.max(maxGap, consecutiveEmptyDays);
    } else {
      consecutiveEmptyDays = 0;
    }
  });

  if (maxGap > 7) {
    assessment.score -= 20;
    assessment.issues.push(`Large gap detected: ${maxGap} consecutive days without validations`);
    assessment.recommendations.push('Set up automated validation schedules to avoid data gaps');
    assessment.metadata.gapsDetected++;
  }

  // Detect data inconsistencies
  trendsData.forEach((day, index) => {
    const totalCalculated = (day.passed || 0) + (day.failed || 0) + (day.errored || 0);

    if (day.total_validations > 0 && totalCalculated === 0) {
      assessment.score -= 10;
      assessment.issues.push(`Data inconsistency on ${day.day}: total validations but no breakdown`);
      assessment.metadata.inconsistenciesDetected++;
    }

    if (day.health_score < 0 || day.health_score > 100) {
      assessment.score -= 5;
      assessment.issues.push(`Invalid health score on ${day.day}: ${day.health_score}`);
      assessment.metadata.inconsistenciesDetected++;
    }
  });

  // Check data freshness
  const latestDay = trendsData[trendsData.length - 1];
  if (latestDay) {
    const latestDate = new Date(latestDay.day);
    const daysSinceLatest = Math.floor((new Date() - latestDate) / (1000 * 60 * 60 * 24));

    if (daysSinceLatest > 7) {
      assessment.score -= 15;
      assessment.issues.push(`Data appears stale: latest data is ${daysSinceLatest} days old`);
      assessment.recommendations.push('Run recent validations to update trend data');
    }
  }

  // Low activity warning
  const activityRate = assessment.metadata.activeDays / assessment.metadata.totalDays;
  if (activityRate < 0.3) {
    assessment.score -= 10;
    assessment.issues.push(`Low validation activity: only ${Math.round(activityRate * 100)}% of days have validation data`);
    assessment.recommendations.push('Increase validation frequency for better trend visibility');
  }

  // Ensure score doesn't go below 0
  assessment.score = Math.max(0, assessment.score);

  return assessment;
};