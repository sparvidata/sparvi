import { supabase } from '../lib/supabase';

/**
 * Central handler for authentication-related functions
 */
class AuthHandler {
  constructor() {
    this.supabase = supabase;
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
   * Sign out the current user
   * @returns {Promise<void>}
   */
  async signOut() {
    await this.supabase.auth.signOut();
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