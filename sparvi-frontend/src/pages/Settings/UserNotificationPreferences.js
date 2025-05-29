import React, { useState, useEffect } from 'react';
import { useUI } from '../../contexts/UIContext';
import { useAuth } from '../../contexts/AuthContext';
import {
  BellIcon,
  EnvelopeIcon,
  ComputerDesktopIcon,
  MoonIcon,
  ClockIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../components/common/LoadingSpinner';

const UserNotificationPreferences = () => {
  const { showNotification } = useUI();
  const { user } = useAuth();
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [preferences, setPreferences] = useState({
    // Email preferences
    email_enabled: true,
    email_frequency: 'immediate', // immediate, daily, weekly, never
    email_types: {
      anomalies: true,
      validations: true,
      schema_changes: true,
      system_updates: false
    },

    // In-app preferences
    push_enabled: true,
    push_types: {
      anomalies: true,
      validations: true,
      schema_changes: true,
      system_updates: true
    },

    // Quiet hours
    quiet_hours_enabled: false,
    quiet_hours_start: '22:00',
    quiet_hours_end: '08:00',
    quiet_hours_timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,

    // Severity preferences
    min_severity_email: 'medium', // low, medium, high
    min_severity_push: 'low'
  });

  useEffect(() => {
    loadPreferences();
  }, []);

  const loadPreferences = async () => {
    try {
      setLoading(true);
      // TODO: Implement API call to get user preferences
      // For now, we'll use the default preferences
      console.log('Loading user notification preferences...');
    } catch (error) {
      console.error('Error loading preferences:', error);
      showNotification('Failed to load notification preferences', 'error');
    } finally {
      setLoading(false);
    }
  };

  const savePreferences = async () => {
    try {
      setSaving(true);
      // TODO: Implement API call to save user preferences
      console.log('Saving preferences:', preferences);

      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 1000));

      showNotification('Notification preferences saved successfully', 'success');
    } catch (error) {
      console.error('Error saving preferences:', error);
      showNotification('Failed to save notification preferences', 'error');
    } finally {
      setSaving(false);
    }
  };

  const handleToggle = (path, value) => {
    setPreferences(prev => {
      const newPrefs = { ...prev };
      const keys = path.split('.');
      let current = newPrefs;

      for (let i = 0; i < keys.length - 1; i++) {
        if (!(keys[i] in current)) {
          current[keys[i]] = {};
        }
        current = current[keys[i]];
      }

      current[keys[keys.length - 1]] = value;
      return newPrefs;
    });
  };

  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Email Notifications */}
      <div className="border-b border-secondary-200 pb-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center">
            <EnvelopeIcon className="h-5 w-5 text-secondary-400 mr-3" />
            <div>
              <h3 className="text-lg font-medium text-secondary-900">Email Notifications</h3>
              <p className="text-sm text-secondary-500">Control when and how you receive email alerts</p>
            </div>
          </div>
          <label className="flex items-center">
            <input
              type="checkbox"
              checked={preferences.email_enabled}
              onChange={(e) => handleToggle('email_enabled', e.target.checked)}
              className="h-4 w-4 text-primary-600 focus:ring-primary-500 border-secondary-300 rounded"
            />
            <span className="ml-2 text-sm text-secondary-700">Enable email notifications</span>
          </label>
        </div>

        {preferences.email_enabled && (
          <div className="space-y-4 ml-8">
            {/* Email Frequency */}
            <div>
              <label className="block text-sm font-medium text-secondary-700 mb-2">
                Email Frequency
              </label>
              <select
                value={preferences.email_frequency}
                onChange={(e) => handleToggle('email_frequency', e.target.value)}
                className="block w-full max-w-xs border-secondary-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
              >
                <option value="immediate">Immediate</option>
                <option value="daily">Daily digest</option>
                <option value="weekly">Weekly summary</option>
                <option value="never">Never</option>
              </select>
            </div>

            {/* Email Types */}
            <div>
              <label className="block text-sm font-medium text-secondary-700 mb-2">
                Email me about
              </label>
              <div className="space-y-2">
                {[
                  { key: 'anomalies', label: 'Data anomalies and quality issues' },
                  { key: 'validations', label: 'Validation failures' },
                  { key: 'schema_changes', label: 'Schema changes' },
                  { key: 'system_updates', label: 'System updates and maintenance' }
                ].map(({ key, label }) => (
                  <label key={key} className="flex items-center">
                    <input
                      type="checkbox"
                      checked={preferences.email_types[key]}
                      onChange={(e) => handleToggle(`email_types.${key}`, e.target.checked)}
                      className="h-4 w-4 text-primary-600 focus:ring-primary-500 border-secondary-300 rounded"
                    />
                    <span className="ml-2 text-sm text-secondary-700">{label}</span>
                  </label>
                ))}
              </div>
            </div>

            {/* Minimum Severity */}
            <div>
              <label className="block text-sm font-medium text-secondary-700 mb-2">
                Minimum severity for email alerts
              </label>
              <select
                value={preferences.min_severity_email}
                onChange={(e) => handleToggle('min_severity_email', e.target.value)}
                className="block w-full max-w-xs border-secondary-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
              >
                <option value="low">All severities (Low, Medium, High)</option>
                <option value="medium">Medium and High only</option>
                <option value="high">High severity only</option>
              </select>
            </div>
          </div>
        )}
      </div>

      {/* In-App Notifications */}
      <div className="border-b border-secondary-200 pb-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center">
            <ComputerDesktopIcon className="h-5 w-5 text-secondary-400 mr-3" />
            <div>
              <h3 className="text-lg font-medium text-secondary-900">In-App Notifications</h3>
              <p className="text-sm text-secondary-500">Notifications you see while using Sparvi</p>
            </div>
          </div>
          <label className="flex items-center">
            <input
              type="checkbox"
              checked={preferences.push_enabled}
              onChange={(e) => handleToggle('push_enabled', e.target.checked)}
              className="h-4 w-4 text-primary-600 focus:ring-primary-500 border-secondary-300 rounded"
            />
            <span className="ml-2 text-sm text-secondary-700">Enable in-app notifications</span>
          </label>
        </div>

        {preferences.push_enabled && (
          <div className="space-y-4 ml-8">
            {/* Push Types */}
            <div>
              <label className="block text-sm font-medium text-secondary-700 mb-2">
                Show notifications for
              </label>
              <div className="space-y-2">
                {[
                  { key: 'anomalies', label: 'Data anomalies and quality issues' },
                  { key: 'validations', label: 'Validation failures' },
                  { key: 'schema_changes', label: 'Schema changes' },
                  { key: 'system_updates', label: 'System updates and maintenance' }
                ].map(({ key, label }) => (
                  <label key={key} className="flex items-center">
                    <input
                      type="checkbox"
                      checked={preferences.push_types[key]}
                      onChange={(e) => handleToggle(`push_types.${key}`, e.target.checked)}
                      className="h-4 w-4 text-primary-600 focus:ring-primary-500 border-secondary-300 rounded"
                    />
                    <span className="ml-2 text-sm text-secondary-700">{label}</span>
                  </label>
                ))}
              </div>
            </div>

            {/* Minimum Severity */}
            <div>
              <label className="block text-sm font-medium text-secondary-700 mb-2">
                Minimum severity for in-app alerts
              </label>
              <select
                value={preferences.min_severity_push}
                onChange={(e) => handleToggle('min_severity_push', e.target.value)}
                className="block w-full max-w-xs border-secondary-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
              >
                <option value="low">All severities (Low, Medium, High)</option>
                <option value="medium">Medium and High only</option>
                <option value="high">High severity only</option>
              </select>
            </div>
          </div>
        )}
      </div>

      {/* Quiet Hours */}
      <div className="border-b border-secondary-200 pb-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center">
            <MoonIcon className="h-5 w-5 text-secondary-400 mr-3" />
            <div>
              <h3 className="text-lg font-medium text-secondary-900">Quiet Hours</h3>
              <p className="text-sm text-secondary-500">Pause notifications during specific hours</p>
            </div>
          </div>
          <label className="flex items-center">
            <input
              type="checkbox"
              checked={preferences.quiet_hours_enabled}
              onChange={(e) => handleToggle('quiet_hours_enabled', e.target.checked)}
              className="h-4 w-4 text-primary-600 focus:ring-primary-500 border-secondary-300 rounded"
            />
            <span className="ml-2 text-sm text-secondary-700">Enable quiet hours</span>
          </label>
        </div>

        {preferences.quiet_hours_enabled && (
          <div className="space-y-4 ml-8">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-secondary-700 mb-1">
                  Start time
                </label>
                <input
                  type="time"
                  value={preferences.quiet_hours_start}
                  onChange={(e) => handleToggle('quiet_hours_start', e.target.value)}
                  className="block w-full border-secondary-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-secondary-700 mb-1">
                  End time
                </label>
                <input
                  type="time"
                  value={preferences.quiet_hours_end}
                  onChange={(e) => handleToggle('quiet_hours_end', e.target.value)}
                  className="block w-full border-secondary-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                />
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-secondary-700 mb-1">
                Timezone
              </label>
              <select
                value={preferences.quiet_hours_timezone}
                onChange={(e) => handleToggle('quiet_hours_timezone', e.target.value)}
                className="block w-full max-w-md border-secondary-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
              >
                <option value="America/New_York">Eastern Time</option>
                <option value="America/Chicago">Central Time</option>
                <option value="America/Denver">Mountain Time</option>
                <option value="America/Los_Angeles">Pacific Time</option>
                <option value="UTC">UTC</option>
              </select>
            </div>
          </div>
        )}
      </div>

      {/* Save Button */}
      <div className="flex justify-end pt-4">
        <button
          type="button"
          onClick={savePreferences}
          disabled={saving}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
        >
          {saving ? (
            <>
              <LoadingSpinner size="xs" className="mr-2" />
              Saving...
            </>
          ) : (
            'Save Preferences'
          )}
        </button>
      </div>
    </div>
  );
};

export default UserNotificationPreferences;