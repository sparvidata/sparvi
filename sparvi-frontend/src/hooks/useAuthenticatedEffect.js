import { useEffect } from 'react';
import { waitForAuth } from '../api/enhancedApiService';

/**
 * Custom hook that works like useEffect but only runs after authentication is ready
 * @param {function} effect - Effect callback to run
 * @param {array} dependencies - Effect dependencies
 * @param {object} options - Additional options
 */
const useAuthenticatedEffect = (effect, dependencies = [], options = {}) => {
  const { timeout = 5000, runWithoutAuth = false } = options;

  useEffect(() => {
    let isMounted = true;
    let timeoutId = null;

    const runEffect = async () => {
      try {
        // Wait for auth to be ready with timeout
        const authReady = await waitForAuth(timeout);

        // Only proceed if component is still mounted
        if (!isMounted) return;

        if (authReady || runWithoutAuth) {
          return effect();
        }
      } catch (error) {
        console.warn('Auth not ready for effect, but proceeding anyway:', error);

        // If runWithoutAuth is true, run the effect anyway
        if (runWithoutAuth && isMounted) {
          return effect();
        }
      }
    };

    // Start the effect
    const cleanup = runEffect();

    // Return cleanup function
    return () => {
      isMounted = false;
      if (timeoutId) clearTimeout(timeoutId);
      if (cleanup && typeof cleanup === 'function') {
        cleanup();
      }
    };
  }, dependencies);
};

export default useAuthenticatedEffect;