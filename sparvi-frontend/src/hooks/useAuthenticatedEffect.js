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
    let authTimeoutId = null;

    const runEffect = async () => {
      try {
        // Create a promise that resolves when auth is ready or times out
        const authPromise = waitForAuth(timeout);
        const timeoutPromise = new Promise((_, reject) => {
          authTimeoutId = setTimeout(() => {
            reject(new Error('Authentication timed out'));
          }, timeout);
        });

        // Race between auth and timeout
        const authReady = await Promise.race([authPromise, timeoutPromise]);

        if (!isMounted) return;

        if (authReady || runWithoutAuth) {
          return effect();
        }
      } catch (error) {
        console.warn('Auth check failed:', error);

        if (runWithoutAuth && isMounted) {
          return effect();
        }
      } finally {
        if (authTimeoutId) clearTimeout(authTimeoutId);
      }
    };

    const cleanup = runEffect();

    return () => {
      isMounted = false;
      if (authTimeoutId) clearTimeout(authTimeoutId);
      if (cleanup && typeof cleanup === 'function') {
        cleanup();
      }
    };
  }, dependencies);
};

export default useAuthenticatedEffect;