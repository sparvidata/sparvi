import React, { createContext, useContext, useState, useEffect, useRef, useCallback } from 'react';
import {
  signIn,
  signUp,
  signOut,
  resetPassword,
  updatePassword,
  getCurrentUser,
  getSession,
  setupAuthListener
} from '../api/supabase';
import { userAPI } from '../api/enhancedApiService';
import { setAuthReady } from '../api/enhancedApiService';
import { isUTCTimestampExpired } from '../utils/dateUtils';

// Create the auth context
const AuthContext = createContext();

export const useAuth = () => {
  return useContext(AuthContext);
};

// Session timeout in milliseconds (30 minutes)
const SESSION_IDLE_TIMEOUT = 30 * 60 * 1000;

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [session, setSession] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastActivity, setLastActivity] = useState(Date.now());

  // Use refs to prevent infinite loops
  const authListenerRef = useRef(null);
  const idleTimerRef = useRef(null);
  const sessionCheckRef = useRef(null);
  const initializedRef = useRef(false);

  // Memoize updateActivity to prevent unnecessary re-renders
  const updateActivity = useCallback(() => {
    setLastActivity(Date.now());
  }, []);

  // Handle logout - define with useCallback to use in dependencies
  const handleLogout = useCallback(async () => {
    try {
      // Capture current timer references
      const currentIdleTimer = idleTimerRef.current;
      const currentSessionCheck = sessionCheckRef.current;

      // Clear timers
      if (currentIdleTimer) clearTimeout(currentIdleTimer);
      if (currentSessionCheck) clearInterval(currentSessionCheck);

      // Clear state
      setUser(null);
      setSession(null);

      // Clear stored data
      sessionStorage.removeItem('activeConnectionId');

      // Sign out from Supabase
      await signOut();

      // Force navigation to login page
      window.location.href = '/login';
    } catch (err) {
      console.error('Error during logout:', err);
    }
  }, []);

  // Check for session expiration - define with useCallback to use in dependencies
  const checkSessionExpiration = useCallback(async () => {
    if (!session) return;

    try {
      // Check if token is expired based on session data using UTC utilities
      if (session.expires_at) {
        if (isUTCTimestampExpired(session.expires_at, 60)) {
          console.log('Session expired, logging out');
          await handleLogout();
          return;
        }
      }

      // Check for idle timeout
      if (Date.now() - lastActivity > SESSION_IDLE_TIMEOUT) {
        console.log('Session idle timeout, logging out');
        await handleLogout();
      }
    } catch (err) {
      console.error('Error checking session:', err);
    }
  }, [session, lastActivity, handleLogout]);

  // Set up session expiration check
  useEffect(() => {
    // Set up check interval
    sessionCheckRef.current = setInterval(checkSessionExpiration, 60 * 1000);

    // Run initial check
    checkSessionExpiration();

    // Cleanup
    return () => {
      // Capture current timer reference before clearing
      const currentSessionCheck = sessionCheckRef.current;
      if (currentSessionCheck) {
        clearInterval(currentSessionCheck);
      }
    };
  }, [checkSessionExpiration]);

  // Initialize auth state - only run once
  useEffect(() => {
    let isMounted = true;

    const initializeAuth = async () => {
      // Prevent multiple initializations
      if (initializedRef.current) return;
      initializedRef.current = true;

      try {
        // Get current session
        const currentSession = await getSession();

        if (!isMounted) return;

        if (currentSession) {
          setSession(currentSession);

          // Get user if session exists
          const currentUser = await getCurrentUser();
          if (isMounted) {
            setUser(currentUser);
          }

          // Signal that auth is ready if we have a token
          if (currentSession.access_token) {
            setAuthReady();
          }
        }
      } catch (err) {
        console.error('Error initializing auth:', err);
        if (isMounted) {
          setError(err.message);
        }
      } finally {
        if (isMounted) {
          setLoading(false);
        }
      }
    };

    // Set up auth state change listener
    authListenerRef.current = setupAuthListener((event, changedSession) => {
      console.log('Auth state changed:', event);

      if (event === 'SIGNED_OUT') {
        setUser(null);
        setSession(null);
      } else if (changedSession) {
        setSession(changedSession);
        setUser(changedSession.user || null);

        // Update auth ready state when session changes
        if (changedSession.access_token) {
          setAuthReady();
        }
      }

      setLoading(false);
    });

    // Initialize auth
    initializeAuth();

    // Set up activity tracking
    const handleActivity = () => updateActivity();

    window.addEventListener('mousemove', handleActivity);
    window.addEventListener('keydown', handleActivity);
    window.addEventListener('click', handleActivity);
    window.addEventListener('scroll', handleActivity);

    // Cleanup
    return () => {
      isMounted = false;

      // Remove event listeners
      window.removeEventListener('mousemove', handleActivity);
      window.removeEventListener('keydown', handleActivity);
      window.removeEventListener('click', handleActivity);
      window.removeEventListener('scroll', handleActivity);

      // Clear timers
      const currentIdleTimer = idleTimerRef.current;
      const currentSessionCheck = sessionCheckRef.current;

      if (currentIdleTimer) clearTimeout(currentIdleTimer);
      if (currentSessionCheck) clearInterval(currentSessionCheck);

      // Unsubscribe from auth listener
      if (authListenerRef.current?.subscription?.unsubscribe) {
        authListenerRef.current.subscription.unsubscribe();
      }
    };
  }, [updateActivity]);

  // Sign in function
  const login = async (email, password) => {
    try {
      setError(null);
      const { user: authUser, session: authSession } = await signIn(email, password);
      setUser(authUser);
      setSession(authSession);

      // Reset activity timer on login
      setLastActivity(Date.now());

      return { user: authUser, session: authSession };
    } catch (err) {
      setError(err.message);
      throw err;
    }
  };

  // Sign up function
  const register = async (email, password, userData = {}) => {
    try {
      setError(null);
      const { user: authUser, session: authSession } = await signUp(email, password, userData);

      // If email confirmation is required, session may be null
      if (authSession) {
        setUser(authUser);
        setSession(authSession);

        // Setup user in the database
        await userAPI.setupUser({
          user_id: authUser.id,
          email,
          first_name: userData.first_name || '',
          last_name: userData.last_name || '',
          organization_name: userData.organization_name || '',
        });

        // Reset activity timer on registration
        setLastActivity(Date.now());
      }

      return { user: authUser, session: authSession };
    } catch (err) {
      setError(err.message);
      throw err;
    }
  };

  // Sign out function
  const logout = async () => {
    await handleLogout();
  };

  // Reset password
  const sendPasswordResetEmail = async (email) => {
    try {
      setError(null);
      await resetPassword(email);
    } catch (err) {
      setError(err.message);
      throw err;
    }
  };

  // Update password
  const changePassword = async (newPassword) => {
    try {
      setError(null);
      await updatePassword(newPassword);

      // Reset activity timer on password change
      setLastActivity(Date.now());
    } catch (err) {
      setError(err.message);
      throw err;
    }
  };

  // Context value
  const value = {
    user,
    session,
    loading,
    error,
    login,
    register,
    logout,
    sendPasswordResetEmail,
    changePassword,
    isAuthenticated: !!user,
    lastActivity,
    updateActivity,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export default AuthContext;