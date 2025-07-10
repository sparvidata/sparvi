/**
 * Utility functions for schedule validation and formatting
 */

// Available automation types
export const AUTOMATION_TYPES = {
  metadata_refresh: 'Metadata Refresh',
  schema_change_detection: 'Schema Change Detection',
  validation_automation: 'Validation Automation'
};

// Schedule types
export const SCHEDULE_TYPES = {
  daily: 'Daily',
  weekly: 'Weekly'
};

// Days of the week
export const WEEK_DAYS = [
  'monday', 'tuesday', 'wednesday', 'thursday',
  'friday', 'saturday', 'sunday'
];

// Common timezones
export const COMMON_TIMEZONES = [
  { value: 'UTC', label: 'UTC' },
  { value: 'America/New_York', label: 'Eastern Time (ET)' },
  { value: 'America/Chicago', label: 'Central Time (CT)' },
  { value: 'America/Denver', label: 'Mountain Time (MT)' },
  { value: 'America/Los_Angeles', label: 'Pacific Time (PT)' },
  { value: 'America/Phoenix', label: 'Arizona Time' },
  { value: 'Europe/London', label: 'London Time' },
  { value: 'Europe/Paris', label: 'Central European Time' },
  { value: 'Asia/Tokyo', label: 'Japan Time' },
  { value: 'Asia/Shanghai', label: 'China Time' },
  { value: 'Australia/Sydney', label: 'Australian Eastern Time' }
];

/**
 * Validate time format (HH:MM)
 */
export const isValidTimeFormat = (time) => {
  if (!time) return false;

  const timePattern = /^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$/;
  return timePattern.test(time);
};

/**
 * Validate timezone
 */
export const isValidTimezone = (timezone) => {
  try {
    Intl.DateTimeFormat(undefined, { timeZone: timezone });
    return true;
  } catch (e) {
    return false;
  }
};

/**
 * Validate schedule configuration
 */
export const validateScheduleConfig = (scheduleConfig) => {
  const errors = {};

  Object.entries(scheduleConfig).forEach(([automationType, config]) => {
    if (!config.enabled) return; // Skip validation for disabled automations

    const automationErrors = [];

    // Validate time format
    if (!isValidTimeFormat(config.time)) {
      automationErrors.push('Invalid time format. Use HH:MM format (24-hour)');
    }

    // Validate timezone
    if (!isValidTimezone(config.timezone)) {
      automationErrors.push('Invalid timezone');
    }

    // Validate schedule type
    if (!['daily', 'weekly'].includes(config.schedule_type)) {
      automationErrors.push('Invalid schedule type. Must be "daily" or "weekly"');
    }

    // Validate days for weekly schedule
    if (config.schedule_type === 'weekly') {
      if (!config.days || !Array.isArray(config.days) || config.days.length === 0) {
        automationErrors.push('Weekly schedule requires at least one day');
      } else {
        const invalidDays = config.days.filter(day => !WEEK_DAYS.includes(day));
        if (invalidDays.length > 0) {
          automationErrors.push(`Invalid days: ${invalidDays.join(', ')}`);
        }
      }
    }

    if (automationErrors.length > 0) {
      errors[automationType] = automationErrors;
    }
  });

  return {
    isValid: Object.keys(errors).length === 0,
    errors
  };
};

/**
 * Format next run time for display
 */
export const formatNextRunTime = (nextRunData) => {
  if (!nextRunData) return 'Not scheduled';

  if (nextRunData.currently_running) {
    return 'Currently running';
  }

  if (nextRunData.is_overdue) {
    return 'Overdue';
  }

  return nextRunData.time_until_next || 'Soon';
};

/**
 * Get user's current timezone
 */
export const getUserTimezone = () => {
  return Intl.DateTimeFormat().resolvedOptions().timeZone;
};

/**
 * Format automation type name for display
 */
export const formatAutomationType = (automationType) => {
  return AUTOMATION_TYPES[automationType] || automationType.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase());
};

/**
 * Create default schedule configuration
 */
export const createDefaultScheduleConfig = (timezone = null) => {
  const userTimezone = timezone || getUserTimezone();

  return {
    metadata_refresh: {
      enabled: false,
      schedule_type: 'daily',
      time: '02:00',
      timezone: userTimezone
    },
    schema_change_detection: {
      enabled: false,
      schedule_type: 'daily',
      time: '03:00',
      timezone: userTimezone
    },
    validation_automation: {
      enabled: false,
      schedule_type: 'weekly',
      time: '01:00',
      timezone: userTimezone,
      days: ['sunday']
    }
  };
};

/**
 * Check if any automation is enabled
 */
export const hasEnabledAutomation = (scheduleConfig) => {
  if (!scheduleConfig) return false;

  return Object.values(scheduleConfig).some(config => config.enabled);
};

/**
 * Get status color based on next run data
 */
export const getNextRunStatusColor = (nextRunData) => {
  if (!nextRunData) return 'text-gray-400';

  if (nextRunData.currently_running) return 'text-blue-600';
  if (nextRunData.is_overdue) return 'text-red-600';
  return 'text-green-600';
};