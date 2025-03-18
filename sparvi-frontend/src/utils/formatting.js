/**
 * Format a number with commas for thousands
 * @param {number} value - The number to format
 * @param {number} decimalPlaces - Number of decimal places (defaults to 0)
 * @returns {string} Formatted number
 */
export const formatNumber = (value, decimalPlaces = 0) => {
  if (value === undefined || value === null) return '-';

  // Handle edge cases
  if (isNaN(value)) return '-';

  // Format the number
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: decimalPlaces,
    maximumFractionDigits: decimalPlaces
  });
};

/**
 * Format a value as a percentage
 * @param {number} value - The percentage value (0-100)
 * @param {number} decimalPlaces - Number of decimal places (defaults to 1)
 * @returns {string} Formatted percentage
 */
export const formatPercentage = (value, decimalPlaces = 1) => {
  if (value === undefined || value === null) return '-';

  // Handle edge cases
  if (isNaN(value)) return '-';

  // Format the percentage
  return `${Number(value).toLocaleString(undefined, {
    minimumFractionDigits: decimalPlaces,
    maximumFractionDigits: decimalPlaces
  })}%`;
};

/**
 * Format a date as a string
 * @param {string|Date} date - The date to format
 * @param {boolean} includeTime - Whether to include the time (defaults to false)
 * @returns {string} Formatted date
 */
export const formatDate = (date, includeTime = false) => {
  if (!date) return '-';

  try {
    const dateObj = typeof date === 'string' ? new Date(date) : date;

    // Check if valid date
    if (isNaN(dateObj.getTime())) return '-';

    // Format options
    const options = includeTime
      ? {
          year: 'numeric',
          month: 'short',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit'
        }
      : {
          year: 'numeric',
          month: 'short',
          day: 'numeric'
        };

    return dateObj.toLocaleDateString(undefined, options);
  } catch (e) {
    return '-';
  }
};

/**
 * Format bytes to human-readable format
 * @param {number} bytes - Number of bytes
 * @param {number} decimals - Number of decimal places (defaults to 2)
 * @returns {string} Formatted size
 */
export const formatBytes = (bytes, decimals = 2) => {
  if (bytes === 0 || bytes === undefined || bytes === null) return '0 Bytes';

  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(decimals))} ${sizes[i]}`;
};

/**
 * Format a duration in milliseconds to human-readable format
 * @param {number} milliseconds - Duration in milliseconds
 * @returns {string} Formatted duration
 */
export const formatDuration = (milliseconds) => {
  if (milliseconds === undefined || milliseconds === null) return '-';

  // For very small durations, show milliseconds
  if (milliseconds < 1000) {
    return `${milliseconds.toFixed(0)}ms`;
  }

  // For seconds
  if (milliseconds < 60000) {
    return `${(milliseconds / 1000).toFixed(1)}s`;
  }

  // For minutes
  if (milliseconds < 3600000) {
    const minutes = Math.floor(milliseconds / 60000);
    const seconds = Math.floor((milliseconds % 60000) / 1000);
    return `${minutes}m ${seconds}s`;
  }

  // For hours
  const hours = Math.floor(milliseconds / 3600000);
  const minutes = Math.floor((milliseconds % 3600000) / 60000);
  return `${hours}h ${minutes}m`;
};

/**
 * Truncate text with ellipsis if longer than maxLength
 * @param {string} text - The text to truncate
 * @param {number} maxLength - Maximum length before truncation
 * @returns {string} Truncated text
 */
export const truncateText = (text, maxLength = 50) => {
  if (!text) return '';
  if (text.length <= maxLength) return text;

  return `${text.substring(0, maxLength)}...`;
};

/**
 * Format a number with specified precision
 * @param {number} value - The number to format
 * @param {number} precision - Number of significant digits
 * @returns {string} Formatted number
 */
export const formatPrecision = (value, precision = 3) => {
  if (value === undefined || value === null) return '-';
  if (isNaN(value)) return '-';

  // Handle special cases
  if (value === 0) return '0';

  return Number(value).toPrecision(precision);
};