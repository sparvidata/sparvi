import React, { useState } from 'react';
import { useAuth } from '../../contexts/AuthContext';
import { useUI } from '../../contexts/UIContext';
import {
  KeyIcon,
  ShieldCheckIcon,
  DevicePhoneMobileIcon,
  ClockIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../components/common/LoadingSpinner';

const UserSecuritySettings = () => {
  const { user, changePassword } = useAuth();
  const { showNotification } = useUI();
  const [loading, setLoading] = useState(false);
  const [changingPassword, setChangingPassword] = useState(false);
  const [showPasswordForm, setShowPasswordForm] = useState(false);
  const [passwordForm, setPasswordForm] = useState({
    currentPassword: '',
    newPassword: '',
    confirmPassword: ''
  });

  const handlePasswordChange = async (e) => {
    e.preventDefault();

    if (passwordForm.newPassword !== passwordForm.confirmPassword) {
      showNotification('New passwords do not match', 'error');
      return;
    }

    if (passwordForm.newPassword.length < 8) {
      showNotification('Password must be at least 8 characters long', 'error');
      return;
    }

    try {
      setChangingPassword(true);
      await changePassword(passwordForm.newPassword);

      showNotification('Password changed successfully', 'success');
      setShowPasswordForm(false);
      setPasswordForm({
        currentPassword: '',
        newPassword: '',
        confirmPassword: ''
      });
    } catch (error) {
      console.error('Error changing password:', error);
      showNotification('Failed to change password', 'error');
    } finally {
      setChangingPassword(false);
    }
  };

  const handleInputChange = (field, value) => {
    setPasswordForm(prev => ({
      ...prev,
      [field]: value
    }));
  };

  return (
    <div className="space-y-6">
      {/* Password Section */}
      <div className="border-b border-secondary-200 pb-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center">
            <KeyIcon className="h-5 w-5 text-secondary-400 mr-3" />
            <div>
              <h3 className="text-lg font-medium text-secondary-900">Password</h3>
              <p className="text-sm text-secondary-500">Manage your account password</p>
            </div>
          </div>
          <button
            type="button"
            onClick={() => setShowPasswordForm(!showPasswordForm)}
            className="inline-flex items-center px-3 py-2 border border-secondary-300 shadow-sm text-sm leading-4 font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          >
            {showPasswordForm ? 'Cancel' : 'Change Password'}
          </button>
        </div>

        {showPasswordForm && (
          <form onSubmit={handlePasswordChange} className="space-y-4 ml-8">
            <div>
              <label className="block text-sm font-medium text-secondary-700 mb-1">
                Current Password
              </label>
              <input
                type="password"
                value={passwordForm.currentPassword}
                onChange={(e) => handleInputChange('currentPassword', e.target.value)}
                className="block w-full max-w-md border-secondary-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-secondary-700 mb-1">
                New Password
              </label>
              <input
                type="password"
                value={passwordForm.newPassword}
                onChange={(e) => handleInputChange('newPassword', e.target.value)}
                className="block w-full max-w-md border-secondary-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                required
                minLength={8}
              />
              <p className="mt-1 text-xs text-secondary-500">
                Password must be at least 8 characters long
              </p>
            </div>

            <div>
              <label className="block text-sm font-medium text-secondary-700 mb-1">
                Confirm New Password
              </label>
              <input
                type="password"
                value={passwordForm.confirmPassword}
                onChange={(e) => handleInputChange('confirmPassword', e.target.value)}
                className="block w-full max-w-md border-secondary-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                required
              />
            </div>

            <div className="flex space-x-3">
              <button
                type="submit"
                disabled={changingPassword}
                className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
              >
                {changingPassword ? (
                  <>
                    <LoadingSpinner size="xs" className="mr-2" />
                    Changing...
                  </>
                ) : (
                  'Change Password'
                )}
              </button>
              <button
                type="button"
                onClick={() => setShowPasswordForm(false)}
                className="inline-flex items-center px-4 py-2 border border-secondary-300 shadow-sm text-sm font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
              >
                Cancel
              </button>
            </div>
          </form>
        )}
      </div>

      {/* Two-Factor Authentication */}
      <div className="border-b border-secondary-200 pb-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center">
            <ShieldCheckIcon className="h-5 w-5 text-secondary-400 mr-3" />
            <div>
              <h3 className="text-lg font-medium text-secondary-900">Two-Factor Authentication</h3>
              <p className="text-sm text-secondary-500">Add an extra layer of security to your account</p>
            </div>
          </div>
          <div className="flex items-center">
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-secondary-100 text-secondary-800 mr-3">
              Not Enabled
            </span>
            <button
              type="button"
              className="inline-flex items-center px-3 py-2 border border-secondary-300 shadow-sm text-sm leading-4 font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              Setup 2FA
            </button>
          </div>
        </div>

        <div className="ml-8">
          <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
            <div className="flex">
              <div className="flex-shrink-0">
                <ShieldCheckIcon className="h-5 w-5 text-blue-400" />
              </div>
              <div className="ml-3">
                <h3 className="text-sm font-medium text-blue-800">Why enable 2FA?</h3>
                <div className="mt-2 text-sm text-blue-700">
                  <p>Two-factor authentication adds an extra layer of security by requiring a second form of verification when signing in.</p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Session Management */}
      <div className="border-b border-secondary-200 pb-6">
        <div className="flex items-center mb-4">
          <ClockIcon className="h-5 w-5 text-secondary-400 mr-3" />
          <div>
            <h3 className="text-lg font-medium text-secondary-900">Active Sessions</h3>
            <p className="text-sm text-secondary-500">Manage where you're signed in</p>
          </div>
        </div>

        <div className="ml-8 space-y-4">
          {/* Current Session */}
          <div className="flex items-center justify-between p-4 border border-secondary-200 rounded-lg">
            <div className="flex items-center">
              <DevicePhoneMobileIcon className="h-8 w-8 text-secondary-400 mr-3" />
              <div>
                <h4 className="text-sm font-medium text-secondary-900">Current Browser</h4>
                <p className="text-sm text-secondary-500">Chrome on macOS • Last active: Now</p>
                <p className="text-xs text-secondary-400">IP: 192.168.1.100 • Location: San Francisco, CA</p>
              </div>
            </div>
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
              Current
            </span>
          </div>

          {/* Other Sessions */}
          <div className="flex items-center justify-between p-4 border border-secondary-200 rounded-lg">
            <div className="flex items-center">
              <DevicePhoneMobileIcon className="h-8 w-8 text-secondary-400 mr-3" />
              <div>
                <h4 className="text-sm font-medium text-secondary-900">Mobile Safari</h4>
                <p className="text-sm text-secondary-500">Safari on iOS • Last active: 2 days ago</p>
                <p className="text-xs text-secondary-400">IP: 192.168.1.105 • Location: San Francisco, CA</p>
              </div>
            </div>
            <button
              type="button"
              className="inline-flex items-center px-3 py-2 border border-secondary-300 shadow-sm text-sm leading-4 font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              Sign Out
            </button>
          </div>

          <div className="pt-4">
            <button
              type="button"
              className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-danger-700 bg-danger-100 hover:bg-danger-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-danger-500"
            >
              <ExclamationTriangleIcon className="h-4 w-4 mr-2" />
              Sign Out All Other Sessions
            </button>
          </div>
        </div>
      </div>

      {/* Account Deletion */}
      <div>
        <div className="flex items-center mb-4">
          <ExclamationTriangleIcon className="h-5 w-5 text-danger-400 mr-3" />
          <div>
            <h3 className="text-lg font-medium text-secondary-900">Danger Zone</h3>
            <p className="text-sm text-secondary-500">Irreversible actions for your account</p>
          </div>
        </div>

        <div className="ml-8">
          <div className="bg-danger-50 border border-danger-200 rounded-md p-4">
            <div className="flex justify-between items-start">
              <div>
                <h4 className="text-sm font-medium text-danger-800">Delete Account</h4>
                <p className="text-sm text-danger-700 mt-1">
                  Permanently delete your account and all associated data. This action cannot be undone.
                </p>
              </div>
              <button
                type="button"
                className="inline-flex items-center px-3 py-2 border border-danger-300 shadow-sm text-sm leading-4 font-medium rounded-md text-danger-700 bg-white hover:bg-danger-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-danger-500"
              >
                Delete Account
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default UserSecuritySettings;