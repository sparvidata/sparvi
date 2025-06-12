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
import { getCurrentUTCSeconds } from '../utils/dateUtils';

// Create the auth context
const AuthContext = createContext();

export const useAuth = () => {
  return useContext(AuthContext);
};

// Session timeout in milliseconds (30 minutes)
const SESSION_IDLE_TIMEOUT = 30 * 60 * 1000;
// Session refresh threshold (5 minutes before expiry)
const SESSION_REFRESH_THRESHOLD = 5 * 60;

// Debug toggle - set to false to reduce console noise
const DEBUG_AUTH = false;

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [session, setSession] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastActivity, setLastActivity] = useState(Date.now());

  // Use refs to prevent infinite loops and track state
  const authListenerRef = useRef(null);
  const idleTimerRef = useRef(null);
  const sessionCheckRef = useRef(null);
  const initializedRef = useRef(false);
  const isLoggingOutRef = useRef(false);
  const sessionRecoveryAttemptRef = useRef(0);
  const maxSessionRecoveryAttempts = 3;

  // Debug logging helper - with log levels
  const logDebug = useCallback((message, data = null, level = 'info') => {
    if (!DEBUG_AUTH) return; // Skip all debug logging if disabled

    // Only log important events, not every activity update
    if (level === 'verbose' && process.env.NODE_ENV === 'production') return;

    if (message.includes('Activity updated') && level !== 'important') {
      // Skip activity logs unless explicitly important
      return;
    }

    console.log(`[AuthContext] ${message}`, data);
  }, []);

  // Memoize updateActivity to prevent unnecessary re-renders
  const updateActivity = useCallback(() => {
    const now = Date.now();
    setLastActivity(now);
    // Remove the verbose activity logging
    // logDebug('Activity updated', new Date(now).toISOString());
  }, []);

  // Enhanced session validation
  const validateSession = useCallback((sessionToValidate) => {
    if (!sessionToValidate) {
      logDebug('Session validation failed: no session');
      return false;
    }

    // Check if session has required properties
    if (!sessionToValidate.access_token || !sessionToValidate.user) {
      logDebug('Session validation failed: missing token or user', {
        hasToken: !!sessionToValidate.access_token,
        hasUser: !!sessionToValidate.user
      });
      return false;
    }

    // Check token expiration using UTC
    if (sessionToValidate.expires_at) {
      const nowUTC = getCurrentUTCSeconds();
      const expiresAt = sessionToValidate.expires_at;
      const timeUntilExpiry = expiresAt - nowUTC;

      logDebug('Token expiration check', {
        nowUTC,
        expiresAt,
        timeUntilExpiry,
        isExpired: timeUntilExpiry <= 0
      });

      // If token is expired or expires within 30 seconds
      if (timeUntilExpiry <= 30) {
        logDebug('Session validation failed: token expired or expiring soon');
        return false;
      }
    }

    logDebug('Session validation passed');
    return true;
  }, [logDebug]);

  // Session recovery mechanism
  const attemptSessionRecovery = useCallback(async () => {
    if (sessionRecoveryAttemptRef.current >= maxSessionRecoveryAttempts) {
      logDebug('Max session recovery attempts reached, giving up');
      return null;
    }

    sessionRecoveryAttemptRef.current++;
    logDebug(`Session recovery attempt ${sessionRecoveryAttemptRef.current}`);

    try {
      // Try to get a fresh session from Supabase
      const freshSession = await getSession();

      if (freshSession && validateSession(freshSession)) {
        logDebug('Session recovery successful');
        sessionRecoveryAttemptRef.current = 0; // Reset counter on success
        return freshSession;
      }

      logDebug('Session recovery failed: no valid session found');
      return null;
    } catch (error) {
      logDebug('Session recovery error', error);
      return null;
    }
  }, [validateSession, logDebug]);

  // Handle logout - define with useCallback to use in dependencies
  const handleLogout = useCallback(async (reason = 'manual') => {
    if (isLoggingOutRef.current) {
      logDebug('Logout already in progress, skipping');
      return;
    }

    isLoggingOutRef.current = true;
    logDebug(`Initiating logout: ${reason}`);

    try {
      // Clear timers first
      if (idleTimerRef.current) clearTimeout(idleTimerRef.current);
      if (sessionCheckRef.current) clearInterval(sessionCheckRef.current);

      // Clear state
      setUser(null);
      setSession(null);
      setError(null);

      // Clear stored data
      sessionStorage.removeItem('activeConnectionId');

      // Clear session cache
      if (window._sessionCache) {
        window._sessionCache = null;
      }

      try {
        // Sign out from Supabase
        await signOut();
      } catch (signOutError) {
        logDebug('Error during Supabase signOut', signOutError);
        // Continue with logout even if signOut fails
      }

      logDebug('Logout completed, redirecting to login');

      // Small delay to ensure state is cleared
      setTimeout(() => {
        window.location.href = '/login';
      }, 100);

    } catch (err) {
      logDebug('Error during logout', err);
      // Force redirect even if logout fails
      window.location.href = '/login';
    } finally {
      isLoggingOutRef.current = false;
    }
  }, [logDebug]);

  // Check for session expiration - define with useCallback to use in dependencies
  const checkSessionExpiration = useCallback(async () => {
    if (!session || isLoggingOutRef.current) return;

    try {
      logDebug('Checking session expiration');

      // First, validate the current session
      if (!validateSession(session)) {
        logDebug('Current session invalid, attempting recovery');

        // Try to recover the session
        const recoveredSession = await attemptSessionRecovery();

        if (recoveredSession) {
          logDebug('Session recovered successfully');
          setSession(recoveredSession);
          setUser(recoveredSession.user);
          updateActivity();
          return;
        }

        logDebug('Session recovery failed, logging out');
        await handleLogout('session_invalid');
        return;
      }

      // Check for idle timeout
      const timeSinceActivity = Date.now() - lastActivity;
      if (timeSinceActivity > SESSION_IDLE_TIMEOUT) {
        logDebug('Session idle timeout exceeded', {
          timeSinceActivity,
          timeout: SESSION_IDLE_TIMEOUT
        });
        await handleLogout('idle_timeout');
        return;
      }

      // Check if token needs refresh (but be more lenient)
      if (session.expires_at) {
        const nowUTC = getCurrentUTCSeconds();
        const timeUntilExpiry = session.expires_at - nowUTC;

        // Only refresh if really close to expiry (2 minutes instead of 5)
        if (timeUntilExpiry <= 2 * 60) {
          logDebug('Token needs refresh soon', { timeUntilExpiry, threshold: 2 * 60 });

          try {
            // Try to refresh the session
            const refreshedSession = await getSession();
            if (refreshedSession && validateSession(refreshedSession)) {
              logDebug('Session refreshed successfully');
              setSession(refreshedSession);
              setUser(refreshedSession.user);
              updateActivity();
            }
          } catch (refreshError) {
            logDebug('Session refresh failed', refreshError);
            // Don't logout immediately on refresh failure, wait for next check
          }
        }
      }

    } catch (err) {
      logDebug('Error checking session', err);
    }
  }, [session, lastActivity, validateSession, attemptSessionRecovery, handleLogout, updateActivity, logDebug]);

  // Set up session expiration check
  useEffect(() => {
    if (!session) return;

    // Clear any existing timer
    if (sessionCheckRef.current) {
      clearInterval(sessionCheckRef.current);
    }

    // Set up check interval (every 2 minutes instead of 30 seconds)
    sessionCheckRef.current = setInterval(checkSessionExpiration, 2 * 60 * 1000);

    // Run initial check
    checkSessionExpiration();

    // Cleanup
    return () => {
      if (sessionCheckRef.current) {
        clearInterval(sessionCheckRef.current);
      }
    };
  }, [session, checkSessionExpiration]);

  // Initialize auth state - only run once
  useEffect(() => {
    let isMounted = true;

    const initializeAuth = async () => {
      // Prevent multiple initializations
      if (initializedRef.current) {
        logDebug('Auth already initialized, skipping');
        return;
      }

      initializedRef.current = true;
      logDebug('Initializing auth');

      try {
        // Get current session
        const currentSession = await getSession();

        if (!isMounted) return;

        logDebug('Initial session check', {
          hasSession: !!currentSession,
          hasToken: !!currentSession?.access_token,
          hasUser: !!currentSession?.user
        });

        if (currentSession && validateSession(currentSession)) {
          logDebug('Valid session found during initialization');
          setSession(currentSession);
          setUser(currentSession.user);
          updateActivity();

          // Signal that auth is ready
          setAuthReady();
          logDebug('Auth marked as ready');
        } else {
          logDebug('No valid session found during initialization');
        }
      } catch (err) {
        logDebug('Error initializing auth', err);
        if (isMounted) {
          setError(err.message);
        }
      } finally {
        if (isMounted) {
          setLoading(false);
          logDebug('Auth initialization completed');
        }
      }
    };

    // Set up auth state change listener
    authListenerRef.current = setupAuthListener((event, changedSession) => {
      logDebug('Auth state change detected', { event, hasSession: !!changedSession });

      if (event === 'SIGNED_OUT') {
        logDebug('Supabase SIGNED_OUT event');
        if (!isLoggingOutRef.current) {
          setUser(null);
          setSession(null);
          setError(null);
        }
      } else if (event === 'SIGNED_IN' && changedSession) {
        logDebug('Supabase SIGNED_IN event');
        if (validateSession(changedSession)) {
          setSession(changedSession);
          setUser(changedSession.user);
          updateActivity();
          sessionRecoveryAttemptRef.current = 0; // Reset recovery attempts

          // Update auth ready state
          if (changedSession.access_token) {
            setAuthReady();
          }
        } else {
          logDebug('Invalid session in SIGNED_IN event');
        }
      } else if (event === 'TOKEN_REFRESHED' && changedSession) {
        logDebug('Supabase TOKEN_REFRESHED event');
        if (validateSession(changedSession)) {
          setSession(changedSession);
          setUser(changedSession.user);
          updateActivity();
        }
      }
      // Don't change state for INITIAL_SESSION events if we already have a valid session

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
      if (idleTimerRef.current) clearTimeout(idleTimerRef.current);
      if (sessionCheckRef.current) clearInterval(sessionCheckRef.current);

      // Unsubscribe from auth listener
      if (authListenerRef.current?.subscription?.unsubscribe) {
        authListenerRef.current.subscription.unsubscribe();
      }
    };
  }, [updateActivity, validateSession, logDebug]);

  // Sign in function
  const login = async (email, password) => {
    try {
      setError(null);
      setLoading(true);
      logDebug('Login attempt started');

      const { user: authUser, session: authSession } = await signIn(email, password);

      logDebug('Login successful', {
        hasUser: !!authUser,
        hasSession: !!authSession,
        hasToken: !!authSession?.access_token
      });

      if (validateSession(authSession)) {
        setUser(authUser);
        setSession(authSession);
        updateActivity();
        sessionRecoveryAttemptRef.current = 0; // Reset recovery attempts

        return { user: authUser, session: authSession };
      } else {
        throw new Error('Invalid session received during login');
      }
    } catch (err) {
      logDebug('Login failed', err);
      setError(err.message);
      throw err;
    } finally {
      setLoading(false);
    }
  };

  // Sign up function
  const register = async (email, password, userData = {}) => {
    try {
      setError(null);
      setLoading(true);
      logDebug('Registration attempt started');

      const { user: authUser, session: authSession } = await signUp(email, password, userData);

      // If email confirmation is required, session may be null
      if (authSession && validateSession(authSession)) {
        setUser(authUser);
        setSession(authSession);
        updateActivity();
        sessionRecoveryAttemptRef.current = 0; // Reset recovery attempts

        // Setup user in the database
        await userAPI.setupUser({
          user_id: authUser.id,
          email,
          first_name: userData.first_name || '',
          last_name: userData.last_name || '',
          organization_name: userData.organization_name || '',
        });

        logDebug('Registration successful with immediate session');
      } else {
        logDebug('Registration successful, email confirmation required');
      }

      return { user: authUser, session: authSession };
    } catch (err) {
      logDebug('Registration failed', err);
      setError(err.message);
      throw err;
    } finally {
      setLoading(false);
    }
  };

  // Sign out function
  const logout = async () => {
    await handleLogout('manual');
  };

  // Reset password
  const sendPasswordResetEmail = async (email) => {
    try {
      setError(null);
      logDebug('Password reset email request');
      await resetPassword(email);
    } catch (err) {
      logDebug('Password reset failed', err);
      setError(err.message);
      throw err;
    }
  };

  // Update password
  const changePassword = async (newPassword) => {
    try {
      setError(null);
      logDebug('Password change attempt');
      await updatePassword(newPassword);
      updateActivity();
    } catch (err) {
      logDebug('Password change failed', err);
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
    isAuthenticated: !!user && !!session && validateSession(session),
    lastActivity,
    updateActivity,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export default AuthContext;