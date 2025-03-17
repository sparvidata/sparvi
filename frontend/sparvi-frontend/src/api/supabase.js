import { createClient } from '@supabase/supabase-js';

// Initialize Supabase client
const supabaseUrl = process.env.REACT_APP_SUPABASE_URL;
const supabaseAnonKey = process.env.REACT_APP_SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseAnonKey) {
  console.error('Supabase URL or Anon Key is missing in environment variables');
}

export const supabase = createClient(supabaseUrl, supabaseAnonKey);

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
  const { data, error } = await supabase.auth.getSession();
  if (error) {
    console.error('Error getting session:', error.message);
    return null;
  }
  return data?.session || null;
};

// Setup auth state change listener
export const setupAuthListener = (callback) => {
  return supabase.auth.onAuthStateChange((event, session) => {
    callback(event, session);
  });
};