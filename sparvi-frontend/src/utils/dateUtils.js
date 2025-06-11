// sparvi-frontend/src/utils/dateUtils.js

/**
 * Utility functions for consistent UTC date handling
 * This ensures frontend dates align with backend UTC timestamps
 */

/**
 * Get current UTC timestamp as ISO string
 * @returns {string} UTC timestamp in ISO format
 */
export const getCurrentUTCTimestamp = () => {
  return new Date().toISOString();
};

/**
 * Get current UTC timestamp in seconds (Unix timestamp)
 * @returns {number} UTC timestamp in seconds
 */
export const getCurrentUTCSeconds = () => {
  return Math.floor(Date.now() / 1000);
};

/**
 * Parse a date string ensuring UTC interpretation
 * @param {string} dateString - Date string (handles both 'Z' and ISO formats)
 * @returns {Date} Date object in UTC
 */
export const parseUTCDate = (dateString) => {
  if (!dateString) return null;

  try {
    // Handle various formats that might come from backend
    let normalizedDate = dateString;

    // If it doesn't end with 'Z' or timezone info, assume UTC
    if (!normalizedDate.includes('Z') && !normalizedDate.includes('+') && !normalizedDate.includes('-')) {
      normalizedDate += 'Z';
    }

    return new Date(normalizedDate);
  } catch (error) {
    console.error('Error parsing UTC date:', dateString, error);
    return null;
  }
};

/**
 * Calculate time difference between two UTC timestamps
 * @param {string|Date} startTime - Start time (UTC)
 * @param {string|Date} endTime - End time (UTC), defaults to now
 * @returns {number} Difference in milliseconds
 */
export const getUTCTimeDifference = (startTime, endTime = null) => {
  const start = typeof startTime === 'string' ? parseUTCDate(startTime) : startTime;
  const end = endTime ? (typeof endTime === 'string' ? parseUTCDate(endTime) : endTime) : new Date();

  if (!start || !end) return 0;

  return end.getTime() - start.getTime();
};

/**
 * Check if a UTC timestamp is expired
 * @param {number} expiresAtSeconds - Expiration time in seconds (UTC)
 * @param {number} bufferSeconds - Buffer time in seconds (default: 60)
 * @returns {boolean} True if expired
 */
export const isUTCTimestampExpired = (expiresAtSeconds, bufferSeconds = 60) => {
  const nowSeconds = getCurrentUTCSeconds();
  return nowSeconds >= (expiresAtSeconds - bufferSeconds);
};

/**
 * Get the most recent timestamp from an array of objects
 * @param {Array} items - Array of objects with timestamp properties
 * @param {string} timestampKey - Key name for timestamp property
 * @returns {Date|null} Most recent timestamp as Date object
 */
export const getMostRecentUTCTimestamp = (items, timestampKey = 'timestamp') => {
  if (!items || items.length === 0) return null;

  const timestamps = items
    .map(item => parseUTCDate(item[timestampKey]))
    .filter(date => date !== null)
    .map(date => date.getTime());

  return timestamps.length > 0 ? new Date(Math.max(...timestamps)) : null;
};

/**
 * Group items by UTC date (YYYY-MM-DD format)
 * @param {Array} items - Array of items with timestamp properties
 * @param {string} timestampKey - Key name for timestamp property
 * @returns {Object} Object grouped by date string
 */
export const groupByUTCDate = (items, timestampKey = 'timestamp') => {
  if (!items || items.length === 0) return {};

  const grouped = {};

  items.forEach(item => {
    const date = parseUTCDate(item[timestampKey]);
    if (date) {
      // Extract date portion in UTC (YYYY-MM-DD)
      const dateKey = date.toISOString().split('T')[0];

      if (!grouped[dateKey]) {
        grouped[dateKey] = [];
      }

      grouped[dateKey].push(item);
    }
  });

  return grouped;
};

/**
 * Format UTC date for display (respects user's local timezone for display only)
 * @param {string|Date} utcDate - UTC date
 * @param {boolean} includeTime - Whether to include time
 * @param {string} locale - Locale for formatting
 * @param {boolean} showTimezone - Whether to show timezone abbreviation
 * @returns {string} Formatted date string in user's local timezone
 */
export const formatUTCDateForDisplay = (utcDate, includeTime = false, locale = undefined, showTimezone = false) => {
  const date = typeof utcDate === 'string' ? parseUTCDate(utcDate) : utcDate;

  if (!date) return '-';

  try {
    const options = includeTime
      ? {
          year: 'numeric',
          month: 'short',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          ...(showTimezone && { timeZoneName: 'short' })
        }
      : {
          year: 'numeric',
          month: 'short',
          day: 'numeric'
        };

    // This automatically converts UTC to user's local timezone for display
    return date.toLocaleDateString(locale, options);
  } catch (error) {
    console.error('Error formatting UTC date for display:', error);
    return '-';
  }
};

/**
 * Get user's current timezone for display purposes
 * @returns {string} User's timezone (e.g., "America/New_York")
 */
export const getUserTimezone = () => {
  return Intl.DateTimeFormat().resolvedOptions().timeZone;
};

/**
 * Format relative time (e.g., "5 minutes ago") from UTC timestamp
 * @param {string|Date} utcDate - UTC date
 * @returns {string} Relative time string
 */
export const formatRelativeTime = (utcDate) => {
  const date = typeof utcDate === 'string' ? parseUTCDate(utcDate) : utcDate;

  if (!date) return '-';

  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffSeconds = Math.floor(diffMs / 1000);
  const diffMinutes = Math.floor(diffSeconds / 60);
  const diffHours = Math.floor(diffMinutes / 60);
  const diffDays = Math.floor(diffHours / 24);

  if (diffSeconds < 60) {
    return 'just now';
  } else if (diffMinutes < 60) {
    return `${diffMinutes} minute${diffMinutes !== 1 ? 's' : ''} ago`;
  } else if (diffHours < 24) {
    return `${diffHours} hour${diffHours !== 1 ? 's' : ''} ago`;
  } else if (diffDays < 7) {
    return `${diffDays} day${diffDays !== 1 ? 's' : ''} ago`;
  } else {
    // For older dates, show the actual date in local timezone
    return formatUTCDateForDisplay(date, false);
  }
};