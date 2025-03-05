import { supabase } from '../lib/supabase';

/**
 * Central handler for authentication-related functions
 */
class AuthHandler {
  constructor() {
    this.supabase = supabase;
    this.refreshInterval = null;
  }

  /**
   * Check if the current session is valid
   * @returns {Promise<boolean>} True if session is valid, false otherwise
   */
  async isAuthenticated() {
    const { data: { session } } = await this.supabase.auth.getSession();
    return !!session;
  }

  /**
   * Get the current session
   * @returns {Promise<Object|null>} Session object or null if not authenticated
   */
  async getSession() {
    const { data: { session } } = await this.supabase.auth.getSession();
    return session;
  }

  /**
   * Get the current access token
   * @returns {Promise<string|null>} Access token or null if not authenticated
   */
  async getAccessToken() {
    const session = await this.getSession();
    return session?.access_token || null;
  }

  /**
   * Sign in with email and password
   * @param {string} email - User email
   * @param {string} password - User password
   * @returns {Promise<Object>} Result object with data and/or error
   */
  async signIn(email, password) {
    const { data, error } = await this.supabase.auth.signInWithPassword({
      email,
      password
    });

    return { data, error };
  }

  /**
   * Sign up a new user with email and password
   * @param {string} email - User email
   * @param {string} password - User password
   * @param {object} metadata - Additional user metadata
   * @returns {Promise<Object>} Result object with data and/or error
   */
  async signUp(email, password, metadata = {}) {
    const { data, error } = await this.supabase.auth.signUp({
      email,
      password,
      options: { data: metadata }
    });

    return { data, error };
  }

  /**
   * Send a password reset email
   * @param {string} email - User email
   * @returns {Promise<Object>} Result object with data and/or error
   */
  async resetPassword(email) {
    const { data, error } = await this.supabase.auth.resetPasswordForEmail(email, {
      redirectTo: `${window.location.origin}/reset-password`,
    });

    return { data, error };
  }

  /**
   * Update user's password
   * @param {string} password - New password
   * @returns {Promise<Object>} Result object with data and/or error
   */
  async updatePassword(password) {
    const { data, error } = await this.supabase.auth.updateUser({
      password
    });

    return { data, error };
  }

  /**
   * Update user profile
   * @param {object} updates - Profile updates
   * @returns {Promise<Object>} Result object with data and/or error
   */
  async updateProfile(updates) {
    const { data: userData, error: userError } = await this.supabase.auth.updateUser({
      data: updates
    });

    if (userError) return { data: null, error: userError };

    const user = userData.user;

    // Also update the profiles table
    const { data, error } = await this.supabase
      .from('profiles')
      .update(updates)
      .eq('id', user.id);

    return { data: data || userData, error };
  }

  /**
   * Create a new organization and associate it with the user
   * @param {string} userId - User ID
   * @param {string} name - Organization name
   * @returns {Promise<Object>} Result object with data and/or error
   */
  async createOrganization(userId, name) {
    // Create organization
    const { data: orgData, error: orgError } = await this.supabase
      .from('organizations')
      .insert([{ name }])
      .select();

    if (orgError) return { data: null, error: orgError };

    if (!orgData || orgData.length === 0) {
      return { data: null, error: { message: 'Failed to create organization' } };
    }

    // Update user profile with organization ID
    const { data, error } = await this.supabase
      .from('profiles')
      .update({ organization_id: orgData[0].id })
      .eq('id', userId);

    return { data: orgData[0], error };
  }

  /**
   * Sign out the current user
   * @returns {Promise<void>}
   */
  async signOut() {
    await this.supabase.auth.signOut();
    this.clearSessionRefresh();
  }

  /**
   * Refresh the session
   * @returns {Promise<Object>} Result object with data and/or error
   */
  async refreshSession() {
    const { data, error } = await this.supabase.auth.refreshSession();
    return { data, error };
  }

  /**
   * Set up the session refresh timer
   * This will refresh the session before it expires
   */
  setupSessionRefresh() {
    // Clear any existing interval first
    this.clearSessionRefresh();

    // Check every minute if the token needs to be refreshed
    this.refreshInterval = setInterval(async () => {
      const session = await this.getSession();
      if (!session) return;

      // If session exists but is about to expire (less than 5 minutes left)
      // Supabase tokens typically last 1 hour
      const expiresAt = session.expires_at * 1000; // Convert to milliseconds
      const now = Date.now();
      const fiveMinutesInMs = 5 * 60 * 1000;

      if (expiresAt - now < fiveMinutesInMs) {
        console.log('Token expiring soon, refreshing...');
        await this.refreshSession();
      }
    }, 60 * 1000); // Check every minute
  }

  /**
   * Clear the session refresh timer
   */
  clearSessionRefresh() {
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
      this.refreshInterval = null;
    }
  }
}

// Export a singleton instance
export default new AuthHandler();