import { createClient } from '@supabase/supabase-js';

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
    storage: localStorage
  }
});

// Authentication functions
export const signIn = async (email, password) => {
  try {
    const { data, error } = await supabase.auth.signInWithPassword({
      email,
      password,
    });

    if (error) throw error;
    return { user: data.user, session: data.session };
  } catch (error) {
    console.error('Error signing in:', error.message);
    throw error;
  }
};

export const signUp = async (email, password, userData = {}) => {
  try {
    const { data, error } = await supabase.auth.signUp({
      email,
      password,
      options: {
        data: userData,
      },
    });

    if (error) throw error;
    return { user: data.user, session: data.session };
  } catch (error) {
    console.error('Error signing up:', error.message);
    throw error;
  }
};

export const signOut = async () => {
  const { error } = await supabase.auth.signOut();
  if (error) {
    console.error('Error signing out:', error.message);
    throw error;
  }
};

export const resetPassword = async (email) => {
  const { error } = await supabase.auth.resetPasswordForEmail(email, {
    redirectTo: `${window.location.origin}/reset-password`,
  });

  if (error) {
    console.error('Error resetting password:', error.message);
    throw error;
  }
};

export const updatePassword = async (newPassword) => {
  const { error } = await supabase.auth.updateUser({
    password: newPassword,
  });

  if (error) {
    console.error('Error updating password:', error.message);
    throw error;
  }
};

export const getCurrentUser = async () => {
  const { data, error } = await supabase.auth.getUser();
  if (error) {
    console.error('Error getting current user:', error.message);
    return null;
  }
  return data?.user || null;
};

export const getSession = async () => {
  try {
    // Use a timestamp to prevent multiple calls within a short time
    const now = Date.now();
    const lastCheck = window._lastSessionCheck || 0;

    // Throttle checks (no more than once every 5 seconds)
    if (now - lastCheck < 5000) {
      // Get from cache if available
      if (window._sessionCache) {
        return window._sessionCache;
      }
    }

    // Update last check time
    window._lastSessionCheck = now;

    const { data, error } = await supabase.auth.getSession();

    if (error) {
      console.error('Error getting session:', error);
      window._sessionCache = null;
      return null;
    }

    // Check if session exists
    if (!data?.session) {
      console.log('No active session found');
      window._sessionCache = null;
      return null;
    }

    // Cache the session
    window._sessionCache = data.session;

    // Proactively refresh if token is close to expiring
    if (data.session && data.session.expires_at) {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const expiresIn = data.session.expires_at - nowSeconds;

      if (expiresIn < 300 && expiresIn > 0) {  // Refresh if expiring in less than 5 minutes
        try {
          console.log('Token expiring soon, refreshing...');
          const { data: refreshData, error: refreshError } = await supabase.auth.refreshSession();
          if (refreshError) throw refreshError;
          console.log('Token refreshed successfully');

          // Update cache
          window._sessionCache = refreshData.session;
          return refreshData.session;
        } catch (refreshError) {
          console.error('Session refresh failed:', refreshError);
        }
      }
    }

    return data.session;
  } catch (err) {
    console.error('Unexpected error in getSession:', err);
    return null;
  }
};

// Setup auth state change listener
export const setupAuthListener = (callback) => {
  return supabase.auth.onAuthStateChange((event, session) => {
    console.log('Auth state change detected:', event);
    callback(event, session);
  });
};