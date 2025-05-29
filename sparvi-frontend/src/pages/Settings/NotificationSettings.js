import React, { useState, useEffect } from 'react';
import { useUI } from '../../contexts/UIContext';
import {
  BellIcon,
  EnvelopeIcon,
  ChatBubbleLeftRightIcon,
  LinkIcon,
  CheckIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import { apiRequest } from '../../utils/apiUtils';

const NotificationSettings = () => {
  const { showNotification } = useUI();
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [settings, setSettings] = useState({
    notify_high_severity: true,
    notify_medium_severity: true,
    notify_low_severity: false,
    email_enabled: false,
    email_config: {
      smtp_host: '',
      smtp_port: 587,
      smtp_user: '',
      smtp_password: '',
      from_email: '',
      to_emails: [],
      use_tls: true
    },
    slack_enabled: false,
    slack_config: {
      webhook_url: '',
      channel: '#alerts',
      username: 'Sparvi Bot'
    },
    webhook_enabled: false,
    webhook_config: {
      url: '',
      headers: {}
    }
  });

  // Load notification settings
  useEffect(() => {
    loadSettings();
  }, []);

  const loadSettings = async () => {
    try {
      setLoading(true);
      const response = await apiRequest('/notification-settings');

      if (response && response.data) {
        setSettings(prev => ({
          ...prev,
          ...response.data
        }));
      }
    } catch (error) {
      console.error('Error loading notification settings:', error);
      showNotification('Failed to load notification settings', 'error');
    } finally {
      setLoading(false);
    }
  };

  const saveSettings = async () => {
    try {
      setSaving(true);

      await apiRequest('/notification-settings', {
        method: 'POST',
        data: settings
      });

      showNotification('Notification settings saved successfully', 'success');
    } catch (error) {
      console.error('Error saving notification settings:', error);
      showNotification('Failed to save notification settings', 'error');
    } finally {
      setSaving(false);
    }
  };

  const handleInputChange = (path, value) => {
    setSettings(prev => {
      const newSettings = { ...prev };
      const keys = path.split('.');
      let current = newSettings;

      for (let i = 0; i < keys.length - 1; i++) {
        if (!(keys[i] in current)) {
          current[keys[i]] = {};
        }
        current = current[keys[i]];
      }

      current[keys[keys.length - 1]] = value;
      return newSettings;
    });
  };

  const testNotification = async (type) => {
    try {
      await apiRequest('/notification-settings/test', {
        method: 'POST',
        data: { type }
      });
      showNotification(`Test ${type} notification sent!`, 'success');
    } catch (error) {
      showNotification(`Failed to send test ${type} notification`, 'error');
    }
  };

  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <div className="bg-white shadow rounded-lg">
        <div className="px-4 py-5 sm:p-6">
          <h3 className="text-lg leading-6 font-medium text-gray-900 flex items-center">
            <BellIcon className="h-5 w-5 mr-2" />
            Notification Settings
          </h3>
          <p className="mt-1 text-sm text-gray-500">
            Configure how and when you receive anomaly detection alerts.
          </p>

          {/* Severity Preferences */}
          <div className="mt-6">
            <h4 className="text-md font-medium text-gray-900">Alert Severity Levels</h4>
            <p className="text-sm text-gray-500">Choose which severity levels trigger notifications</p>

            <div className="mt-4 space-y-3">
              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={settings.notify_high_severity}
                  onChange={(e) => handleInputChange('notify_high_severity', e.target.checked)}
                  className="h-4 w-4 text-red-600 focus:ring-red-500 border-gray-300 rounded"
                />
                <span className="ml-2 text-sm text-gray-700">
                  <span className="font-medium text-red-600">High Severity</span> - Critical anomalies requiring immediate attention
                </span>
              </label>

              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={settings.notify_medium_severity}
                  onChange={(e) => handleInputChange('notify_medium_severity', e.target.checked)}
                  className="h-4 w-4 text-yellow-600 focus:ring-yellow-500 border-gray-300 rounded"
                />
                <span className="ml-2 text-sm text-gray-700">
                  <span className="font-medium text-yellow-600">Medium Severity</span> - Notable anomalies worth investigating
                </span>
              </label>

              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={settings.notify_low_severity}
                  onChange={(e) => handleInputChange('notify_low_severity', e.target.checked)}
                  className="h-4 w-4 text-green-600 focus:ring-green-500 border-gray-300 rounded"
                />
                <span className="ml-2 text-sm text-gray-700">
                  <span className="font-medium text-green-600">Low Severity</span> - Minor anomalies for reference
                </span>
              </label>
            </div>
          </div>

          {/* Email Settings */}
          <div className="mt-8 border-t border-gray-200 pt-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <EnvelopeIcon className="h-5 w-5 text-gray-400 mr-2" />
                <h4 className="text-md font-medium text-gray-900">Email Notifications</h4>
              </div>
              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={settings.email_enabled}
                  onChange={(e) => handleInputChange('email_enabled', e.target.checked)}
                  className="h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded"
                />
                <span className="ml-2 text-sm text-gray-700">Enable</span>
              </label>
            </div>

            {settings.email_enabled && (
              <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-2">
                <div>
                  <label className="block text-sm font-medium text-gray-700">SMTP Host</label>
                  <input
                    type="text"
                    value={settings.email_config?.smtp_host || ''}
                    onChange={(e) => handleInputChange('email_config.smtp_host', e.target.value)}
                    className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                    placeholder="smtp.gmail.com"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700">SMTP Port</label>
                  <input
                    type="number"
                    value={settings.email_config?.smtp_port || 587}
                    onChange={(e) => handleInputChange('email_config.smtp_port', parseInt(e.target.value))}
                    className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700">SMTP Username</label>
                  <input
                    type="email"
                    value={settings.email_config?.smtp_user || ''}
                    onChange={(e) => handleInputChange('email_config.smtp_user', e.target.value)}
                    className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                    placeholder="alerts@yourcompany.com"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700">SMTP Password</label>
                  <input
                    type="password"
                    value={settings.email_config?.smtp_password || ''}
                    onChange={(e) => handleInputChange('email_config.smtp_password', e.target.value)}
                    className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                    placeholder="App password or SMTP password"
                  />
                </div>

                <div className="sm:col-span-2">
                  <label className="block text-sm font-medium text-gray-700">Recipients (comma-separated)</label>
                  <input
                    type="text"
                    value={settings.email_config?.to_emails?.join(', ') || ''}
                    onChange={(e) => handleInputChange('email_config.to_emails', e.target.value.split(',').map(email => email.trim()))}
                    className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                    placeholder="admin@yourcompany.com, devops@yourcompany.com"
                  />
                </div>

                <div className="sm:col-span-2">
                  <button
                    type="button"
                    onClick={() => testNotification('email')}
                    className="inline-flex items-center px-3 py-2 border border-gray-300 shadow-sm text-sm leading-4 font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                  >
                    Send Test Email
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* Slack Settings */}
          <div className="mt-8 border-t border-gray-200 pt-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <ChatBubbleLeftRightIcon className="h-5 w-5 text-gray-400 mr-2" />
                <h4 className="text-md font-medium text-gray-900">Slack Notifications</h4>
              </div>
              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={settings.slack_enabled}
                  onChange={(e) => handleInputChange('slack_enabled', e.target.checked)}
                  className="h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded"
                />
                <span className="ml-2 text-sm text-gray-700">Enable</span>
              </label>
            </div>

            {settings.slack_enabled && (
              <div className="mt-4 space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700">Webhook URL</label>
                  <input
                    type="url"
                    value={settings.slack_config?.webhook_url || ''}
                    onChange={(e) => handleInputChange('slack_config.webhook_url', e.target.value)}
                    className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                    placeholder="https://hooks.slack.com/services/..."
                  />
                </div>

                <button
                  type="button"
                  onClick={() => testNotification('slack')}
                  className="inline-flex items-center px-3 py-2 border border-gray-300 shadow-sm text-sm leading-4 font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                >
                  Send Test Message
                </button>
              </div>
            )}
          </div>

          {/* Webhook Settings */}
          <div className="mt-8 border-t border-gray-200 pt-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <LinkIcon className="h-5 w-5 text-gray-400 mr-2" />
                <h4 className="text-md font-medium text-gray-900">Webhook Notifications</h4>
              </div>
              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={settings.webhook_enabled}
                  onChange={(e) => handleInputChange('webhook_enabled', e.target.checked)}
                  className="h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded"
                />
                <span className="ml-2 text-sm text-gray-700">Enable</span>
              </label>
            </div>

            {settings.webhook_enabled && (
              <div className="mt-4 space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700">Webhook URL</label>
                  <input
                    type="url"
                    value={settings.webhook_config?.url || ''}
                    onChange={(e) => handleInputChange('webhook_config.url', e.target.value)}
                    className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                    placeholder="https://your-system.com/webhooks/anomaly"
                  />
                </div>

                <button
                  type="button"
                  onClick={() => testNotification('webhook')}
                  className="inline-flex items-center px-3 py-2 border border-gray-300 shadow-sm text-sm leading-4 font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                >
                  Send Test Webhook
                </button>
              </div>
            )}
          </div>

          {/* Save Button */}
          <div className="mt-8 border-t border-gray-200 pt-6">
            <div className="flex justify-end">
              <button
                type="button"
                onClick={saveSettings}
                disabled={saving}
                className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
              >
                {saving ? (
                  <>
                    <LoadingSpinner size="xs" className="mr-2" />
                    Saving...
                  </>
                ) : (
                  <>
                    <CheckIcon className="h-4 w-4 mr-2" />
                    Save Settings
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Help Text */}
      <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
        <div className="flex">
          <div className="flex-shrink-0">
            <ExclamationTriangleIcon className="h-5 w-5 text-blue-400" aria-hidden="true" />
          </div>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-blue-800">Setup Help</h3>
            <div className="mt-2 text-sm text-blue-700">
              <p><strong>Email:</strong> For Gmail, use your email and an "App Password" (not your regular password).</p>
              <p><strong>Slack:</strong> Create an "Incoming Webhook" in your Slack workspace and paste the URL here.</p>
              <p><strong>Webhook:</strong> Your system will receive POST requests with anomaly data in JSON format.</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default NotificationSettings;