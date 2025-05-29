import React, { useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import AdminLayout from '../../components/layout/AdminLayout';
import AdminUsersManagement from './AdminUsersManagement';
import NotificationSettings from '../Settings/NotificationSettings';
import AdminSystemSettings from './AdminSystemSettings';
import AdminAnalytics from './AdminAnalytics';

const AdminSettingsPage = () => {
  const location = useLocation();
  const navigate = useNavigate();

  // Determine active tab from URL path
  const getActiveTabFromPath = () => {
    const path = location.pathname;
    if (path.includes('/notifications')) return 'notifications';
    if (path.includes('/settings')) return 'settings';
    if (path.includes('/analytics')) return 'analytics';
    return 'users';
  };

  const [activeTab, setActiveTab] = useState(getActiveTabFromPath());

  const handleTabChange = (tabId) => {
    setActiveTab(tabId);
  };

  const renderTabContent = () => {
    switch (activeTab) {
      case 'notifications':
        return <NotificationSettings />;
      case 'settings':
        return <AdminSystemSettings />;
      case 'analytics':
        return <AdminAnalytics />;
      case 'users':
      default:
        return <AdminUsersManagement />;
    }
  };

  return (
    <AdminLayout activeTab={activeTab} onTabChange={handleTabChange}>
      {renderTabContent()}
    </AdminLayout>
  );
};

export default AdminSettingsPage;