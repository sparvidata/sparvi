import React, { createContext, useContext, useState, useEffect } from 'react';

// Create the UI context
const UIContext = createContext();

export const useUI = () => {
  return useContext(UIContext);
};

export const UIProvider = ({ children }) => {
  // Sidebar state
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [isMobile, setIsMobile] = useState(false);

  // Theme state
  const [theme, setTheme] = useState(localStorage.getItem('sparvi-theme') || 'sparvi');

  // Toast notifications
  const [notifications, setNotifications] = useState([]);

  // Breadcrumbs
  const [breadcrumbs, setBreadcrumbs] = useState([]);

  // Loading states for different sections
  const [loadingStates, setLoadingStates] = useState({
    tables: false,
    profile: false,
    validations: false,
    metadata: false,
  });

  // Handle mobile view
  useEffect(() => {
    const handleResize = () => {
      const mobile = window.innerWidth < 768;
      setIsMobile(mobile);

      // Auto-close sidebar on mobile
      if (mobile && sidebarOpen) {
        setSidebarOpen(false);
      } else if (!mobile && !sidebarOpen) {
        setSidebarOpen(true);
      }
    };

    // Initial check
    handleResize();

    // Add event listener
    window.addEventListener('resize', handleResize);

    // Cleanup
    return () => window.removeEventListener('resize', handleResize);
  }, [sidebarOpen]);

  // Save theme preference
  useEffect(() => {
    localStorage.setItem('sparvi-theme', theme);
    document.documentElement.setAttribute('data-theme', theme);
  }, [theme]);

  // Toggle sidebar
  const toggleSidebar = () => {
    setSidebarOpen(prev => !prev);
  };

  // Toggle theme
  const toggleTheme = () => {
    setTheme(prev => {
      const newTheme = prev === 'sparvi' ? 'dark' : 'sparvi';
      return newTheme;
    });
  };

  // Show a toast notification
  const showNotification = (message, type = 'info', duration = 5000) => {
    const id = Date.now();
    const notification = { id, message, type, duration };

    setNotifications(prev => [...prev, notification]);

    // Auto remove after duration
    if (duration > 0) {
      setTimeout(() => {
        removeNotification(id);
      }, duration);
    }

    return id;
  };

  // Remove a notification
  const removeNotification = (id) => {
    setNotifications(prev => prev.filter(note => note.id !== id));
  };

  // Update breadcrumbs
  const updateBreadcrumbs = (newBreadcrumbs) => {
    setBreadcrumbs(newBreadcrumbs);
  };

  // Set loading state for a section
  const setLoading = (section, isLoading) => {
    setLoadingStates(prev => ({
      ...prev,
      [section]: isLoading
    }));
  };

  // Context value
  const value = {
    sidebarOpen,
    toggleSidebar,
    isMobile,
    theme,
    toggleTheme,
    notifications,
    showNotification,
    removeNotification,
    breadcrumbs,
    updateBreadcrumbs,
    loadingStates,
    setLoading,
  };

  return <UIContext.Provider value={value}>{children}</UIContext.Provider>;
};

export default UIContext;