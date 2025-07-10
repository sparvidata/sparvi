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
  if (!timezone) return false;

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
  console.log('Validating schedule config:', scheduleConfig);

  const errors = {};

  // Ensure we have a valid object
  if (!scheduleConfig || typeof scheduleConfig !== 'object') {
    console.error('Invalid schedule config: not an object', scheduleConfig);
    return {
      isValid: false,
      errors: { general: ['Invalid schedule configuration'] }
    };
  }

  Object.entries(scheduleConfig).forEach(([automationType, config]) => {
    console.log(`Validating ${automationType}:`, config);

    // Skip validation for disabled automations
    if (!config || !config.enabled) {
      console.log(`Skipping validation for disabled automation: ${automationType}`);
      return;
    }

    const automationErrors = [];

    // Ensure config is an object
    if (typeof config !== 'object') {
      automationErrors.push('Invalid configuration format');
      errors[automationType] = automationErrors;
      return;
    }

    // Validate time format
    if (!config.time || !isValidTimeFormat(config.time)) {
      automationErrors.push('Invalid time format. Use HH:MM format (24-hour)');
    }

    // Validate timezone
    if (!config.timezone || !isValidTimezone(config.timezone)) {
      automationErrors.push('Invalid or missing timezone');
    }

    // Validate schedule type - be more explicit about what we're checking
    const scheduleType = config.schedule_type;
    console.log(`Checking schedule type for ${automationType}: "${scheduleType}" (type: ${typeof scheduleType})`);

    if (!scheduleType) {
      automationErrors.push('Schedule type is required');
    } else if (typeof scheduleType !== 'string') {
      automationErrors.push('Schedule type must be a string');
    } else if (!['daily', 'weekly'].includes(scheduleType.toLowerCase().trim())) {
      automationErrors.push(`Invalid schedule type: "${scheduleType}". Must be "daily" or "weekly"`);
    }

    // Validate days for weekly schedule
    if (scheduleType === 'weekly') {
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
      console.error(`Validation errors for ${automationType}:`, automationErrors);
      errors[automationType] = automationErrors;
    } else {
      console.log(`Validation passed for ${automationType}`);
    }
  });

  const isValid = Object.keys(errors).length === 0;
  console.log('Validation result:', { isValid, errors });

  return {
    isValid,
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
 * Normalize schedule configuration to ensure proper structure
 */
export const normalizeScheduleConfig = (scheduleConfig) => {
  if (!scheduleConfig || typeof scheduleConfig !== 'object') {
    return createDefaultScheduleConfig();
  }

  const normalized = {};
  const userTimezone = getUserTimezone();

  // Ensure all automation types are present with valid defaults
  Object.keys(AUTOMATION_TYPES).forEach(automationType => {
    const config = scheduleConfig[automationType] || {};

    normalized[automationType] = {
      enabled: Boolean(config.enabled),
      schedule_type: config.schedule_type || 'daily',
      time: config.time || '02:00',
      timezone: config.timezone || userTimezone,
      ...(config.schedule_type === 'weekly' && {
        days: Array.isArray(config.days) && config.days.length > 0
          ? config.days
          : ['sunday']
      })
    };
  });

  return normalized;
};

/**
 * Check if any automation is enabled
 */
export const hasEnabledAutomation = (scheduleConfig) => {
  if (!scheduleConfig) return false;

  return Object.values(scheduleConfig).some(config => config && config.enabled);
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