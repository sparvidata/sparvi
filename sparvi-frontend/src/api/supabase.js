import { createClient } from '@supabase/supabase-js';
import { getCurrentUTCSeconds } from '../utils/dateUtils';

// Initialize Supabase client
const supabaseUrl = process.env.REACT_APP_SUPABASE_URL;
const supabaseAnonKey = process.env.REACT_APP_SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseAnonKey) {
  console.error('Supabase URL or Anon Key is missing in environment variables');
}

export const supabase = createClient(supabaseUrl, supabaseAnonKey, {
  auth: {
    autoRefreshToken: true,
    persistSession: true,
    storage: localStorage,
    detectSessionInUrl: true,
    flowType: 'pkce'
  }
});

// Enhanced session cache with validation
let sessionCache = null;
let sessionCacheTimestamp = 0;
let sessionRefreshInProgress = false; // Add lock to prevent race conditions
const SESSION_CACHE_TTL = 10 * 60 * 1000; // Increase to 10 minutes for more stability
const SESSION_REFRESH_THRESHOLD = 5 * 60; // 5 minutes before expiry

// Debug toggle - set to false to reduce console noise
const DEBUG_SUPABASE = false;

// Debug logging helper with levels
const logDebug = (message, data = null, level = 'info') => {
  if (!DEBUG_SUPABASE) return; // Skip all debug logging if disabled

  // Only log important supabase events in production
  if (process.env.NODE_ENV === 'production' && level === 'verbose') return;

  // Skip repetitive cache messages unless they're important
  if (message.includes('Returning cached session') && level !== 'important') return;

  console.log(`[Supabase] ${message}`, data);
};

// Validate session object
const isValidSession = (session) => {
  if (!session) return false;

  if (!session.access_token || !session.user) {
    logDebug('Invalid session: missing token or user', {
      hasToken: !!session.access_token,
      hasUser: !!session.user
    });
    return false;
  }

  // Check token expiration
  if (session.expires_at) {
    const nowUTC = getCurrentUTCSeconds();
    const timeUntilExpiry = session.expires_at - nowUTC;

    if (timeUntilExpiry <= 0) {
      logDebug('Invalid session: token expired', {
        nowUTC,
        expiresAt: session.expires_at,
        timeUntilExpiry
      });
      return false;
    }
  }

  return true;
};

// Clear session cache
const clearSessionCache = () => {
  // Don't clear cache if a refresh is in progress
  if (sessionRefreshInProgress) {
    logDebug('Not clearing cache - refresh in progress');
    return;
  }

  sessionCache = null;
  sessionCacheTimestamp = 0;
  logDebug('Session cache cleared');
};

// Authentication functions
export const signIn = async (email, password) => {
  try {
    logDebug('Sign in attempt', { email });
    clearSessionCache(); // Clear cache before new sign in

    const { data, error } = await supabase.auth.signInWithPassword({
      email,
      password,
    });

    if (error) {
      logDebug('Sign in error', error);
      throw error;
    }

    logDebug('Sign in successful', {
      hasUser: !!data.user,
      hasSession: !!data.session,
      hasToken: !!data.session?.access_token
    });

    // Cache the new session
    if (data.session && isValidSession(data.session)) {
      sessionCache = data.session;
      sessionCacheTimestamp = Date.now();
    }

    return { user: data.user, session: data.session };
  } catch (error) {
    logDebug('Sign in failed', error);
    clearSessionCache();
    throw error;
  }
};

export const signUp = async (email, password, userData = {}) => {
  try {
    logDebug('Sign up attempt', { email });
    clearSessionCache(); // Clear cache before new sign up

    const { data, error } = await supabase.auth.signUp({
      email,
      password,
      options: {
        data: userData,
      },
    });

    if (error) {
      logDebug('Sign up error', error);
      throw error;
    }

    logDebug('Sign up successful', {
      hasUser: !!data.user,
      hasSession: !!data.session,
      emailConfirmationRequired: !data.session
    });

    // Cache the new session if available
    if (data.session && isValidSession(data.session)) {
      sessionCache = data.session;
      sessionCacheTimestamp = Date.now();
    }

    return { user: data.user, session: data.session };
  } catch (error) {
    logDebug('Sign up failed', error);
    clearSessionCache();
    throw error;
  }
};

export const signOut = async () => {
  try {
    logDebug('Sign out attempt');
    clearSessionCache(); // Clear cache immediately

    const { error } = await supabase.auth.signOut();

    if (error) {
      logDebug('Sign out error', error);
      throw error;
    }

    logDebug('Sign out successful');

    // Reset auth ready state - use dynamic import to avoid circular dependency
    try {
      const { resetAuthReady } = await import('../api/enhancedApiService');
      resetAuthReady();
      logDebug('Auth ready state reset');
    } catch (importError) {
      logDebug('Could not reset auth ready state', importError);
    }

  } catch (error) {
    logDebug('Sign out failed', error);
    // Still clear cache even if signOut fails
    clearSessionCache();
    throw error;
  }
};

export const resetPassword = async (email) => {
  try {
    logDebug('Password reset request', { email });

    const { error } = await supabase.auth.resetPasswordForEmail(email, {
      redirectTo: `${window.location.origin}/reset-password`,
    });

    if (error) {
      logDebug('Password reset error', error);
      throw error;
    }

    logDebug('Password reset email sent');
  } catch (error) {
    logDebug('Password reset failed', error);
    throw error;
  }
};

export const updatePassword = async (newPassword) => {
  try {
    logDebug('Password update attempt');

    const { error } = await supabase.auth.updateUser({
      password: newPassword,
    });

    if (error) {
      logDebug('Password update error', error);
      throw error;
    }

    logDebug('Password updated successfully');
    // Clear cache to force refresh of session
    clearSessionCache();
  } catch (error) {
    logDebug('Password update failed', error);
    throw error;
  }
};

export const getCurrentUser = async () => {
  try {
    logDebug('Get current user attempt');

    const { data, error } = await supabase.auth.getUser();

    if (error) {
      logDebug('Get current user error', error);
      return null;
    }

    logDebug('Get current user successful', { hasUser: !!data?.user });
    return data?.user || null;
  } catch (error) {
    logDebug('Get current user failed', error);
    return null;
  }
};

export const getSession = async () => {
  try {
    const now = Date.now();

    // Check if we have a valid cached session
    if (sessionCache && sessionCacheTimestamp > 0 && !sessionRefreshInProgress) {
      const cacheAge = now - sessionCacheTimestamp;

      // If cache is still fresh and session is valid, return it immediately
      if (cacheAge < SESSION_CACHE_TTL && isValidSession(sessionCache)) {
        // Only log cache hits occasionally (every 50th call or so)
        if (Math.random() < 0.02) {
          logDebug('Using cached session', { cacheAge, cacheValid: true }, 'verbose');
        }
        return sessionCache;
      } else {
        logDebug('Cached session needs refresh', {
          cacheAge,
          ttl: SESSION_CACHE_TTL,
          isValid: isValidSession(sessionCache),
          reason: cacheAge >= SESSION_CACHE_TTL ? 'cache_expired' : 'session_invalid'
        });
        // Don't clear cache immediately - try to refresh first
      }
    }

    // If another refresh is in progress, wait for it and return the result
    if (sessionRefreshInProgress) {
      logDebug('Session refresh already in progress, waiting...');
      // Wait a bit and check again
      await new Promise(resolve => setTimeout(resolve, 100));
      if (sessionCache && isValidSession(sessionCache)) {
        return sessionCache;
      }
    }

    // Set the refresh lock
    sessionRefreshInProgress = true;

    try {
      logDebug('Fetching fresh session from Supabase');

      const { data, error } = await supabase.auth.getSession();

      if (error) {
        logDebug('Get session error', error);
        clearSessionCache();
        return null;
      }

      logDebug('Get session response', {
        hasData: !!data,
        hasSession: !!data?.session,
        hasToken: !!data?.session?.access_token,
        hasUser: !!data?.session?.user
      });

      const session = data?.session;

      if (!session) {
        logDebug('No session in response');
        clearSessionCache();
        return null;
      }

      // Validate the session
      if (!isValidSession(session)) {
        logDebug('Invalid session received');
        clearSessionCache();
        return null;
      }

      // Check if token needs refresh soon (but don't do it during busy operations)
      if (session.expires_at) {
        const nowUTC = getCurrentUTCSeconds();
        const timeUntilExpiry = session.expires_at - nowUTC;

        // Only refresh if REALLY close to expiry (1 minute) to avoid disrupting operations
        if (timeUntilExpiry <= 60) {
          logDebug('Token expiring very soon, attempting refresh', { timeUntilExpiry });

          try {
            const { data: refreshData, error: refreshError } = await supabase.auth.refreshSession();

            if (refreshError) {
              logDebug('Session refresh failed', refreshError);
              // Continue with current session if refresh fails
            } else if (refreshData?.session && isValidSession(refreshData.session)) {
              logDebug('Session refreshed successfully');
              sessionCache = refreshData.session;
              sessionCacheTimestamp = Date.now();
              return refreshData.session;
            }
          } catch (refreshError) {
            logDebug('Session refresh exception', refreshError);
            // Continue with current session if refresh fails
          }
        }
      }

      // Cache the valid session
      sessionCache = session;
      sessionCacheTimestamp = Date.now();

      logDebug('Session cached successfully');
      return session;

    } finally {
      // Always clear the refresh lock
      sessionRefreshInProgress = false;
    }

  } catch (err) {
    logDebug('Unexpected error in getSession', err);
    sessionRefreshInProgress = false;
    clearSessionCache();
    return null;
  }
};

// Setup auth state change listener
export const setupAuthListener = (callback) => {
  logDebug('Setting up auth state listener');

  const authListener = supabase.auth.onAuthStateChange((event, session) => {
    logDebug('Auth state change event', {
      event,
      hasSession: !!session,
      hasToken: !!session?.access_token
    });

    // Only clear cache on actual sign out or sign in events
    if (event === 'SIGNED_OUT') {
      clearSessionCache();
    } else if (event === 'SIGNED_IN' || event === 'TOKEN_REFRESHED') {
      // Cache new session if valid
      if (session && isValidSession(session)) {
        sessionCache = session;
        sessionCacheTimestamp = Date.now();
        logDebug('New session cached from auth event');
      }
    }
    // Don't clear cache for INITIAL_SESSION events

    // Call the callback
    if (callback) {
      try {
        callback(event, session);
      } catch (callbackError) {
        logDebug('Error in auth state callback', callbackError);
      }
    }
  });

  logDebug('Auth state listener setup complete');
  return authListener;
};