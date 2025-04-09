import React, { createContext, useContext, useState, useEffect, useRef } from 'react';
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
import { useNavigate } from 'react-router-dom';

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
  const idleTimerRef = useRef(null);
  const sessionCheckIntervalRef = useRef(null);
  const navigate = useNavigate();

  // Update last activity timestamp on user interaction
  const updateActivity = () => {
    setLastActivity(Date.now());
  };

  // Check for session expiration
  const checkSessionExpiration = async () => {
    try {
      const currentSession = await getSession();

      // If no session, logout
      if (!currentSession) {
        await handleLogout(true);
        return;
      }

      // Check if token is expired
      if (currentSession.expires_at) {
        const now = Math.floor(Date.now() / 1000);
        const expiresAt = currentSession.expires_at;

        // If expired or about to expire (within 1 minute), logout
        if (now >= expiresAt - 60) {
          console.log('Session expired, logging out');
          await handleLogout(true);
        }
      }

      // Check for idle timeout
      if (Date.now() - lastActivity > SESSION_IDLE_TIMEOUT) {
        console.log('Session idle timeout, logging out');
        await handleLogout(true);
      }
    } catch (err) {
      console.error('Error checking session:', err);
    }
  };

  // Handle logout (with optional redirect)
  const handleLogout = async (shouldRedirect = false) => {
    try {
      await signOut();
      setUser(null);
      setSession(null);

      // Clear session storage
      sessionStorage.removeItem('activeConnectionId');

      // If we should redirect to login
      if (shouldRedirect) {
        navigate('/login', { state: { from: window.location.pathname } });
      }
    } catch (err) {
      console.error('Error during logout:', err);
    }
  };

  // Initialize auth state
  useEffect(() => {
    const initializeAuth = async () => {
      try {
        // Get current session
        const currentSession = await getSession();
        setSession(currentSession);

        // Get user if session exists
        if (currentSession) {
          const currentUser = await getCurrentUser();
          setUser(currentUser);

          // Signal that auth is ready if we have a token
          if (currentSession.access_token) {
            setAuthReady();
          }
        }
      } catch (err) {
        console.error('Error initializing auth:', err);
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    initializeAuth();

    // Set up auth state change listener
    const { data: authListener } = setupAuthListener((event, changedSession) => {
      console.log('Auth state changed:', event);
      setSession(changedSession);
      setUser(changedSession?.user || null);
      setLoading(false);

      // Update auth ready state when session changes
      if (changedSession?.access_token) {
        setAuthReady();
      }

      // If the event is SIGNED_OUT, make sure we clear user state
      if (event === 'SIGNED_OUT') {
        setUser(null);
        setSession(null);
      }
    });

    // Set up activity tracking
    window.addEventListener('mousemove', updateActivity);
    window.addEventListener('keydown', updateActivity);
    window.addEventListener('click', updateActivity);
    window.addEventListener('scroll', updateActivity);

    // Set up periodic session checks (every minute)
    sessionCheckIntervalRef.current = setInterval(checkSessionExpiration, 60 * 1000);

    // Cleanup
    return () => {
      // Remove event listeners
      window.removeEventListener('mousemove', updateActivity);
      window.removeEventListener('keydown', updateActivity);
      window.removeEventListener('click', updateActivity);
      window.removeEventListener('scroll', updateActivity);

      // Clear intervals
      if (idleTimerRef.current) clearTimeout(idleTimerRef.current);
      if (sessionCheckIntervalRef.current) clearInterval(sessionCheckIntervalRef.current);

      // Unsubscribe from auth listener
      if (authListener && typeof authListener.subscription?.unsubscribe === 'function') {
        authListener.subscription.unsubscribe();
      } else if (authListener) {
        console.warn('Unable to unsubscribe from auth listener - check Supabase API');
      }
    };
  }, [navigate, lastActivity]);

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
    await handleLogout(true);
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