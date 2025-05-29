import React, { useState, useEffect } from 'react';
import { useAuth } from '../../contexts/AuthContext';
import { useUI } from '../../contexts/UIContext';
import {
  UserCircleIcon,
  EnvelopeIcon,
  BuildingOfficeIcon,
  CalendarIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../components/common/LoadingSpinner';

const UserProfileSettings = () => {
  const { user, updateProfile } = useAuth();
  const { showNotification } = useUI();
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [profile, setProfile] = useState({
    first_name: '',
    last_name: '',
    email: '',
    organization: '',
    role: '',
    joined_date: ''
  });

  useEffect(() => {
    if (user) {
      setProfile({
        first_name: user.user_metadata?.first_name || '',
        last_name: user.user_metadata?.last_name || '',
        email: user.email || '',
        organization: user.user_metadata?.organization_name || '',
        role: user.user_metadata?.role || 'User',
        joined_date: user.created_at ? new Date(user.created_at).toLocaleDateString() : ''
      });
    }
  }, [user]);

  const handleInputChange = (field, value) => {
    setProfile(prev => ({
      ...prev,
      [field]: value
    }));
  };

  const saveProfile = async () => {
    try {
      setSaving(true);

      // Update only the fields that can be changed
      const updateData = {
        first_name: profile.first_name,
        last_name: profile.last_name
      };

      // TODO: Implement profile update API call
      console.log('Saving profile:', updateData);

      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 1000));

      showNotification('Profile updated successfully', 'success');
    } catch (error) {
      console.error('Error saving profile:', error);
      showNotification('Failed to update profile', 'error');
    } finally {
      setSaving(false);
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
    <div className="space-y-6">
      {/* Profile Information */}
      <div className="grid grid-cols-1 gap-6 sm:grid-cols-2">
        <div>
          <label className="block text-sm font-medium text-secondary-700 mb-2">
            First Name
          </label>
          <div className="relative">
            <UserCircleIcon className="absolute left-3 top-3 h-5 w-5 text-secondary-400" />
            <input
              type="text"
              value={profile.first_name}
              onChange={(e) => handleInputChange('first_name', e.target.value)}
              className="block w-full pl-10 pr-3 py-2 border border-secondary-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
              placeholder="Enter your first name"
            />
          </div>
        </div>

        <div>
          <label className="block text-sm font-medium text-secondary-700 mb-2">
            Last Name
          </label>
          <div className="relative">
            <UserCircleIcon className="absolute left-3 top-3 h-5 w-5 text-secondary-400" />
            <input
              type="text"
              value={profile.last_name}
              onChange={(e) => handleInputChange('last_name', e.target.value)}
              className="block w-full pl-10 pr-3 py-2 border border-secondary-300 rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
              placeholder="Enter your last name"
            />
          </div>
        </div>

        <div>
          <label className="block text-sm font-medium text-secondary-700 mb-2">
            Email Address
          </label>
          <div className="relative">
            <EnvelopeIcon className="absolute left-3 top-3 h-5 w-5 text-secondary-400" />
            <input
              type="email"
              value={profile.email}
              disabled
              className="block w-full pl-10 pr-3 py-2 border border-secondary-300 rounded-md shadow-sm bg-secondary-50 text-secondary-500 sm:text-sm cursor-not-allowed"
            />
          </div>
          <p className="mt-1 text-xs text-secondary-500">
            Email address cannot be changed. Contact support if you need to update this.
          </p>
        </div>

        <div>
          <label className="block text-sm font-medium text-secondary-700 mb-2">
            Organization
          </label>
          <div className="relative">
            <BuildingOfficeIcon className="absolute left-3 top-3 h-5 w-5 text-secondary-400" />
            <input
              type="text"
              value={profile.organization}
              disabled
              className="block w-full pl-10 pr-3 py-2 border border-secondary-300 rounded-md shadow-sm bg-secondary-50 text-secondary-500 sm:text-sm cursor-not-allowed"
            />
          </div>
        </div>

        <div>
          <label className="block text-sm font-medium text-secondary-700 mb-2">
            Role
          </label>
          <input
            type="text"
            value={profile.role}
            disabled
            className="block w-full px-3 py-2 border border-secondary-300 rounded-md shadow-sm bg-secondary-50 text-secondary-500 sm:text-sm cursor-not-allowed"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-secondary-700 mb-2">
            Member Since
          </label>
          <div className="relative">
            <CalendarIcon className="absolute left-3 top-3 h-5 w-5 text-secondary-400" />
            <input
              type="text"
              value={profile.joined_date}
              disabled
              className="block w-full pl-10 pr-3 py-2 border border-secondary-300 rounded-md shadow-sm bg-secondary-50 text-secondary-500 sm:text-sm cursor-not-allowed"
            />
          </div>
        </div>
      </div>

      {/* Account Actions */}
      <div className="border-t border-secondary-200 pt-6">
        <h3 className="text-lg font-medium text-secondary-900 mb-4">Account Actions</h3>
        <div className="space-y-4">
          <div className="flex items-center justify-between py-3">
            <div>
              <h4 className="text-sm font-medium text-secondary-900">Change Password</h4>
              <p className="text-sm text-secondary-500">Update your account password</p>
            </div>
            <button
              type="button"
              className="inline-flex items-center px-3 py-2 border border-secondary-300 shadow-sm text-sm leading-4 font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              Change Password
            </button>
          </div>

          <div className="flex items-center justify-between py-3">
            <div>
              <h4 className="text-sm font-medium text-secondary-900">Two-Factor Authentication</h4>
              <p className="text-sm text-secondary-500">Add an extra layer of security to your account</p>
            </div>
            <button
              type="button"
              className="inline-flex items-center px-3 py-2 border border-secondary-300 shadow-sm text-sm leading-4 font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              Setup 2FA
            </button>
          </div>
        </div>
      </div>

      {/* Save Button */}
      <div className="flex justify-end pt-4 border-t border-secondary-200">
        <button
          type="button"
          onClick={saveProfile}
          disabled={saving}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
        >
          {saving ? (
            <>
              <LoadingSpinner size="xs" className="mr-2" />
              Saving...
            </>
          ) : (
            'Save Profile'
          )}
        </button>
      </div>
    </div>
  );
};

export default UserProfileSettings;