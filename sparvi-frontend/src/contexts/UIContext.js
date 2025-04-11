import React, {createContext, useContext, useState, useEffect, useCallback} from 'react';

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
  // const [theme, setTheme] = useState(localStorage.getItem('sparvi-theme') || 'sparvi');
  const [theme, setTheme] = useState('sparvi'); // Always use light theme

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

  // Handle window resize and detect mobile
  useEffect(() => {
    const handleResize = () => {
      const mobile = window.innerWidth < 768;
      setIsMobile(mobile);
    };

    // Initial check
    handleResize();

    // Add event listener
    window.addEventListener('resize', handleResize);

    // Cleanup
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  // Adjust the sidebar state only when mobile status changes
  useEffect(() => {
    // Only automatically adjust sidebar when mobile status changes
    if (isMobile) {
      setSidebarOpen(false);
    } else {
      setSidebarOpen(true);
    }
  }, [isMobile]); // Only depend on isMobile changes

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
    // Generate a more unique ID by adding a random suffix
    const id = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
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
  const updateBreadcrumbs = useCallback((newBreadcrumbs) => {
    setBreadcrumbs(newBreadcrumbs);
  }, []);

  // Set loading state for a section
  const setLoading = useCallback((section, isLoading) => {
    setLoadingStates(prev => ({
      ...prev,
      [section]: isLoading
    }));
  }, []);

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