import React, { useState, useEffect } from 'react';
import { useScheduleConfig } from '../../hooks/useScheduleConfig';
import { automationAPI } from '../../api/enhancedApiService';
import {
  ClockIcon,
  CalendarIcon,
  GlobeAltIcon,
  CheckCircleIcon,
  ExclamationTriangleIcon,
  SparklesIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../common/LoadingSpinner';
import {
  AUTOMATION_TYPES,
  SCHEDULE_TYPES,
  WEEK_DAYS,
  COMMON_TIMEZONES,
  validateScheduleConfig,
  normalizeScheduleConfig,
  formatAutomationType,
  getUserTimezone
} from '../../utils/scheduleUtils';

const ScheduleConfig = ({ connectionId, onUpdate, className = '' }) => {
  const { schedule, loading, error, saving, updateSchedule, applyTemplate } = useScheduleConfig(connectionId);
  const [localSchedule, setLocalSchedule] = useState(null);
  const [validationErrors, setValidationErrors] = useState({});
  const [templates, setTemplates] = useState({});
  const [templatesLoading, setTemplatesLoading] = useState(true);
  const [showValidation, setShowValidation] = useState(false);

  // Load templates on mount
  useEffect(() => {
    const loadTemplates = async () => {
      try {
        const response = await automationAPI.getScheduleTemplates();
        setTemplates(response?.templates || {});
      } catch (err) {
        console.error('Error loading templates:', err);
      } finally {
        setTemplatesLoading(false);
      }
    };

    loadTemplates();
  }, []);

  // Sync schedule to local state with normalization
  useEffect(() => {
    if (schedule) {
      console.log('Original schedule from API:', schedule);
      const normalized = normalizeScheduleConfig(schedule);
      console.log('Normalized schedule:', normalized);
      setLocalSchedule(normalized);
    } else {
      // Set default schedule if none exists
      const defaultSchedule = normalizeScheduleConfig({});
      console.log('Setting default schedule:', defaultSchedule);
      setLocalSchedule(defaultSchedule);
    }
  }, [schedule]);

  // Validate schedule when it changes and validation is shown
  useEffect(() => {
    if (localSchedule && showValidation) {
      console.log('Running validation on local schedule:', localSchedule);
      const validation = validateScheduleConfig(localSchedule);
      console.log('Validation result:', validation);
      setValidationErrors(validation.errors);
    }
  }, [localSchedule, showValidation]);

  const handleScheduleChange = (automationType, field, value) => {
    console.log(`Updating ${automationType}.${field} to:`, value, `(type: ${typeof value})`);

    setLocalSchedule(prev => {
      const updated = {
        ...prev,
        [automationType]: {
          ...prev[automationType],
          [field]: value
        }
      };

      console.log('Updated local schedule:', updated);
      return updated;
    });
  };

  const handleDayToggle = (automationType, day) => {
    const currentDays = localSchedule[automationType]?.days || [];
    const newDays = currentDays.includes(day)
      ? currentDays.filter(d => d !== day)
      : [...currentDays, day];

    console.log(`Toggling day ${day} for ${automationType}. New days:`, newDays);
    handleScheduleChange(automationType, 'days', newDays);
  };

  const handleTemplateApply = async (templateName) => {
    const template = templates[templateName];
    if (template?.schedule_config) {
      const normalized = normalizeScheduleConfig(template.schedule_config);
      console.log('Applying template:', templateName, normalized);
      setLocalSchedule(normalized);
    }
  };

  const handleSave = async () => {
    if (!localSchedule) {
      console.error('No local schedule to save');
      return;
    }

    console.log('Attempting to save schedule:', localSchedule);
    setShowValidation(true);

    // Normalize the schedule before validation
    const normalizedSchedule = normalizeScheduleConfig(localSchedule);
    console.log('Normalized schedule for validation:', normalizedSchedule);

    const validation = validateScheduleConfig(normalizedSchedule);
    console.log('Save validation result:', validation);

    if (!validation.isValid) {
      console.error('Validation failed:', validation.errors);
      setValidationErrors(validation.errors);
      return;
    }

    // Clear validation errors if validation passed
    setValidationErrors({});

    const success = await updateSchedule(normalizedSchedule);
    if (success && onUpdate) {
      onUpdate(normalizedSchedule);
    }
  };

  const handleReset = () => {
    console.log('Resetting to original schedule:', schedule);
    const normalized = schedule ? normalizeScheduleConfig(schedule) : normalizeScheduleConfig({});
    setLocalSchedule(normalized);
    setValidationErrors({});
    setShowValidation(false);
  };

  if (loading) {
    return (
      <div className="flex justify-center py-8">
        <LoadingSpinner size="lg" />
        <span className="ml-3 text-gray-600">Loading schedule configuration...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-md bg-red-50 p-4">
        <div className="flex">
          <ExclamationTriangleIcon className="h-5 w-5 text-red-400" />
          <div className="ml-3">
            <h3 className="text-sm font-medium text-red-800">Error loading schedule</h3>
            <p className="mt-1 text-sm text-red-700">{error.message}</p>
          </div>
        </div>
      </div>
    );
  }

  if (!localSchedule) {
    return (
      <div className="text-center py-8">
        <p className="text-gray-500">No schedule configuration available</p>
      </div>
    );
  }

  const hasChanges = JSON.stringify(localSchedule) !== JSON.stringify(schedule);

  return (
    <div className={`space-y-6 ${className}`}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold text-gray-900 flex items-center">
            <ClockIcon className="h-5 w-5 mr-2" />
            Automation Schedules
          </h3>
          <p className="mt-1 text-sm text-gray-500">
            Configure when automated processes run for this connection
          </p>
        </div>

        {/* Save/Reset buttons */}
        {hasChanges && (
          <div className="flex items-center space-x-3">
            <button
              onClick={handleReset}
              className="px-3 py-2 text-sm text-gray-600 hover:text-gray-800"
            >
              Reset
            </button>
            <button
              onClick={handleSave}
              disabled={saving}
              className="flex items-center px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {saving ? (
                <>
                  <LoadingSpinner size="xs" className="mr-2" />
                  Saving...
                </>
              ) : (
                <>
                  <CheckCircleIcon className="h-4 w-4 mr-2" />
                  Save Changes
                </>
              )}
            </button>
          </div>
        )}
      </div>

      {/* Templates */}
      {!templatesLoading && Object.keys(templates).length > 0 && (
        <div className="bg-gray-50 rounded-lg p-4">
          <h4 className="text-sm font-medium text-gray-900 mb-3 flex items-center">
            <SparklesIcon className="h-4 w-4 mr-2" />
            Quick Templates
          </h4>
          <div className="flex flex-wrap gap-2">
            {Object.entries(templates).map(([templateName, template]) => (
              <button
                key={templateName}
                onClick={() => handleTemplateApply(templateName)}
                className="px-3 py-2 text-sm bg-white border border-gray-300 rounded-md hover:bg-gray-50 hover:border-gray-400 transition-colors"
              >
                {template.name}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Schedule Configuration */}
      <div className="space-y-6">
        {Object.entries(AUTOMATION_TYPES).map(([automationType, label]) => {
          const config = localSchedule[automationType] || {
            enabled: false,
            schedule_type: 'daily',
            time: '02:00',
            timezone: getUserTimezone(),
            days: ['sunday']
          };

          const errors = validationErrors[automationType] || [];

          console.log(`Rendering ${automationType} with config:`, config); // Temp Debugging - Delete Later

          return (
            <div key={automationType} className="bg-white border border-gray-200 rounded-lg p-6">
              {/* Header */}
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    checked={config.enabled}
                    onChange={(e) => handleScheduleChange(automationType, 'enabled', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <label className="ml-3 text-lg font-medium text-gray-900">
                    {label}
                  </label>
                </div>

                {config.enabled && (
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                    Enabled
                  </span>
                )}
              </div>

              {/* Configuration */}
              {config.enabled && (
                <div className="ml-7 space-y-4">
                  {/* Schedule Type */}
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        <CalendarIcon className="h-4 w-4 inline mr-1" />
                        Schedule Type
                      </label>
                      <select
                        value={config.schedule_type}
                        onChange={(e) => {
                          console.log(`Schedule type changing from "${config.schedule_type}" to "${e.target.value}"`); // Temp Debugging - Delete Later
                          handleScheduleChange(automationType, 'schedule_type', e.target.value);
                        }}
                        className="block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                      >
                        {Object.entries(SCHEDULE_TYPES).map(([value, label]) => (
                          <option key={value} value={value}>{label}</option>
                        ))}
                      </select>
                    </div>

                    {/* Time */}
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        <ClockIcon className="h-4 w-4 inline mr-1" />
                        Time
                      </label>
                      <input
                        type="time"
                        value={config.time}
                        onChange={(e) => handleScheduleChange(automationType, 'time', e.target.value)}
                        className="block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                      />
                    </div>

                    {/* Timezone */}
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        <GlobeAltIcon className="h-4 w-4 inline mr-1" />
                        Timezone
                      </label>
                      <select
                        value={config.timezone}
                        onChange={(e) => handleScheduleChange(automationType, 'timezone', e.target.value)}
                        className="block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                      >
                        {COMMON_TIMEZONES.map(tz => (
                          <option key={tz.value} value={tz.value}>
                            {tz.label}
                          </option>
                        ))}
                      </select>
                    </div>
                  </div>

                  {/* Days (for weekly) */}
                  {config.schedule_type === 'weekly' && (
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-3">
                        Days of the week
                      </label>
                      <div className="flex flex-wrap gap-2">
                        {WEEK_DAYS.map(day => (
                          <label key={day} className="flex items-center">
                            <input
                              type="checkbox"
                              checked={config.days?.includes(day) || false}
                              onChange={() => handleDayToggle(automationType, day)}
                              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded mr-2"
                            />
                            <span className="text-sm text-gray-700 capitalize">
                              {day}
                            </span>
                          </label>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Validation Errors */}
                  {errors.length > 0 && (
                    <div className="rounded-md bg-red-50 p-3">
                      <div className="flex">
                        <ExclamationTriangleIcon className="h-5 w-5 text-red-400" />
                        <div className="ml-3">
                          <h4 className="text-sm font-medium text-red-800">
                            Configuration Errors
                          </h4>
                          <ul className="mt-1 text-sm text-red-700 list-disc list-inside">
                            {errors.map((error, index) => (
                              <li key={index}>{error}</li>
                            ))}
                          </ul>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Global validation errors */}
      {showValidation && Object.keys(validationErrors).length > 0 && (
        <div className="rounded-md bg-red-50 p-4">
          <div className="flex">
            <ExclamationTriangleIcon className="h-5 w-5 text-red-400" />
            <div className="ml-3">
              <h3 className="text-sm font-medium text-red-800">
                Please fix the configuration errors above before saving
              </h3>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ScheduleConfig;