import React, { createContext, useContext, useState, useEffect } from 'react';
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

// Create the auth context
const AuthContext = createContext();

export const useAuth = () => {
  return useContext(AuthContext);
};

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [session, setSession] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

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
      setSession(changedSession);
      setUser(changedSession?.user || null);
      setLoading(false);
    });

    // Cleanup
    return () => {
      if (authListener?.unsubscribe) {
        authListener.unsubscribe();
      }
    };
  }, []);

  // Sign in function
  const login = async (email, password) => {
    try {
      setError(null);
      const { user: authUser, session: authSession } = await signIn(email, password);
      setUser(authUser);
      setSession(authSession);
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
      }

      return { user: authUser, session: authSession };
    } catch (err) {
      setError(err.message);
      throw err;
    }
  };

  // Sign out function
  const logout = async () => {
    try {
      setError(null);
      await signOut();
      setUser(null);
      setSession(null);
    } catch (err) {
      setError(err.message);
      throw err;
    }
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
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export default AuthContext;