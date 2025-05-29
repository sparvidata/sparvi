import React, { useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import SettingsLayout from '../../components/layout/SettingsLayout';
import UserProfileSettings from './UserProfileSettings';
import UserNotificationPreferences from './UserNotificationPreferences';
import UserSecuritySettings from './UserSecuritySettings';

const UserSettingsPage = () => {
  const location = useLocation();
  const navigate = useNavigate();

  // Determine active tab from URL path
  const getActiveTabFromPath = () => {
    const path = location.pathname;
    if (path.includes('/notifications')) return 'notifications';
    if (path.includes('/security')) return 'security';
    return 'profile';
  };

  const [activeTab, setActiveTab] = useState(getActiveTabFromPath());

  const handleTabChange = (tabId) => {
    setActiveTab(tabId);

    // Update URL to match the tab
    const basePath = '/settings';
    const newPath = tabId === 'profile' ? basePath : `${basePath}/${tabId}`;
    navigate(newPath, { replace: true });
  };

  const renderTabContent = () => {
    switch (activeTab) {
      case 'notifications':
        return <UserNotificationPreferences />;
      case 'security':
        return <UserSecuritySettings />;
      case 'profile':
      default:
        return <UserProfileSettings />;
    }
  };

  return (
    <SettingsLayout activeTab={activeTab} onTabChange={handleTabChange}>
      {renderTabContent()}
    </SettingsLayout>
  );
};

export default UserSettingsPage;