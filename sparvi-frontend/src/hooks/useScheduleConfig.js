import { useState, useEffect, useCallback } from 'react';
import { automationAPI } from '../api/enhancedApiService';

export const useScheduleConfig = (connectionId) => {
  const [schedule, setSchedule] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [saving, setSaving] = useState(false);

  const loadSchedule = useCallback(async () => {
    if (!connectionId) {
      setLoading(false);
      return;
    }

    try {
      setLoading(true);
      setError(null);

      const response = await automationAPI.getSchedule(connectionId);

      if (response?.schedule_config) {
        setSchedule(response.schedule_config);
      } else {
        // Set default schedule structure if none exists
        setSchedule({
          metadata_refresh: {
            enabled: false,
            schedule_type: 'daily',
            time: '02:00',
            timezone: Intl.DateTimeFormat().resolvedOptions().timeZone
          },
          schema_change_detection: {
            enabled: false,
            schedule_type: 'daily',
            time: '03:00',
            timezone: Intl.DateTimeFormat().resolvedOptions().timeZone
          },
          validation_automation: {
            enabled: false,
            schedule_type: 'weekly',
            time: '01:00',
            timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
            days: ['sunday']
          }
        });
      }
    } catch (err) {
      console.error('Error loading schedule:', err);
      setError(err);
    } finally {
      setLoading(false);
    }
  }, [connectionId]);

  useEffect(() => {
    loadSchedule();
  }, [loadSchedule]);

  const updateSchedule = async (newSchedule) => {
    if (!connectionId) return false;

    try {
      setSaving(true);
      setError(null);

      // Validate first
      const validationResponse = await automationAPI.validateSchedule(newSchedule);

      if (!validationResponse.valid) {
        throw new Error(validationResponse.error || 'Invalid schedule configuration');
      }

      // Update if valid
      const response = await automationAPI.updateSchedule(connectionId, newSchedule);

      if (response?.schedule_config) {
        setSchedule(response.schedule_config);
        return true;
      }

      return false;
    } catch (err) {
      console.error('Error updating schedule:', err);
      setError(err);
      return false;
    } finally {
      setSaving(false);
    }
  };

  const applyTemplate = async (templateName) => {
    try {
      const templatesResponse = await automationAPI.getScheduleTemplates();
      const template = templatesResponse?.templates?.[templateName];

      if (template?.schedule_config) {
        setSchedule(template.schedule_config);
        return true;
      }

      return false;
    } catch (err) {
      console.error('Error applying template:', err);
      setError(err);
      return false;
    }
  };

  const refreshSchedule = useCallback(() => {
    loadSchedule();
  }, [loadSchedule]);

  return {
    schedule,
    loading,
    error,
    saving,
    updateSchedule,
    applyTemplate,
    refreshSchedule
  };
};