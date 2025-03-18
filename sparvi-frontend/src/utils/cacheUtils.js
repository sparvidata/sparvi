/**
 * A simple caching utility for the application
 * Supports both memory and localStorage backends
 */

// Default TTL (Time To Live) for cache items in milliseconds
const DEFAULT_TTL = 5 * 60 * 1000; // 5 minutes

// Memory cache storage
const memoryCache = new Map();

/**
 * Set a value in the cache
 * @param {string} key - The cache key
 * @param {any} value - The value to cache
 * @param {Object} options - Cache options
 * @param {number} options.ttl - Time to live in milliseconds
 * @param {boolean} options.persistent - Whether to store in localStorage
 */
export const setCacheItem = (key, value, options = {}) => {
  const { ttl = DEFAULT_TTL, persistent = false } = options;

  // Create cache item with expiry
  const cacheItem = {
    value,
    expiry: Date.now() + ttl
  };

  // Store in memory cache
  memoryCache.set(key, cacheItem);

  // Also store in localStorage if persistent
  if (persistent) {
    try {
      localStorage.setItem(`cache_${key}`, JSON.stringify(cacheItem));
    } catch (error) {
      console.warn('Failed to store item in localStorage:', error);
    }
  }
};

/**
 * Get a value from the cache
 * @param {string} key - The cache key
 * @param {boolean} checkPersistent - Whether to check localStorage if not found in memory
 * @returns {any|null} The cached value or null if not found or expired
 */
export const getCacheItem = (key, checkPersistent = true) => {
  // Check memory cache first
  const memoryItem = memoryCache.get(key);

  if (memoryItem && memoryItem.expiry > Date.now()) {
    return memoryItem.value;
  }

  // If expired or not found in memory, remove it
  if (memoryItem) {
    memoryCache.delete(key);
  }

  // If not checking persistent storage, return null
  if (!checkPersistent) {
    return null;
  }

  // Check localStorage
  try {
    const persistentItem = localStorage.getItem(`cache_${key}`);
    if (persistentItem) {
      const parsedItem = JSON.parse(persistentItem);

      // Check if still valid
      if (parsedItem.expiry > Date.now()) {
        // Also add back to memory cache
        memoryCache.set(key, parsedItem);
        return parsedItem.value;
      } else {
        // Remove expired item
        localStorage.removeItem(`cache_${key}`);
      }
    }
  } catch (error) {
    console.warn('Failed to retrieve item from localStorage:', error);
  }

  return null;
};

/**
 * Clear a specific item from the cache
 * @param {string} key - The cache key to clear
 * @param {boolean} clearPersistent - Whether to also clear from localStorage
 */
export const clearCacheItem = (key, clearPersistent = true) => {
  memoryCache.delete(key);

  if (clearPersistent) {
    try {
      localStorage.removeItem(`cache_${key}`);
    } catch (error) {
      console.warn('Failed to remove item from localStorage:', error);
    }
  }
};

/**
 * Clear all cache items or those matching a prefix
 * @param {string} prefix - Optional prefix to limit clearing to specific keys
 * @param {boolean} clearPersistent - Whether to also clear from localStorage
 */
export const clearCache = (prefix = '', clearPersistent = true) => {
  // Clear memory cache
  if (prefix) {
    // Clear only items with matching prefix
    for (const key of memoryCache.keys()) {
      if (key.startsWith(prefix)) {
        memoryCache.delete(key);
      }
    }
  } else {
    // Clear all items
    memoryCache.clear();
  }

  // Clear localStorage if needed
  if (clearPersistent) {
    try {
      if (prefix) {
        // Get all localStorage keys
        const keys = Object.keys(localStorage);

        // Remove matching items
        for (const key of keys) {
          if (key.startsWith(`cache_${prefix}`)) {
            localStorage.removeItem(key);
          }
        }
      } else {
        // Remove all cache items
        const keys = Object.keys(localStorage);
        for (const key of keys) {
          if (key.startsWith('cache_')) {
            localStorage.removeItem(key);
          }
        }
      }
    } catch (error) {
      console.warn('Failed to clear items from localStorage:', error);
    }
  }
};

/**
 * Get all cache keys
 * @param {string} prefix - Optional prefix to filter keys
 * @returns {string[]} Array of cache keys
 */
export const getCacheKeys = (prefix = '') => {
  const keys = [];

  // Get memory cache keys
  for (const key of memoryCache.keys()) {
    if (!prefix || key.startsWith(prefix)) {
      keys.push(key);
    }
  }

  // Get localStorage keys
  try {
    const storageKeys = Object.keys(localStorage);
    for (const key of storageKeys) {
      if (key.startsWith('cache_') && (!prefix || key.substring(6).startsWith(prefix))) {
        const actualKey = key.substring(6); // Remove 'cache_' prefix
        if (!keys.includes(actualKey)) {
          keys.push(actualKey);
        }
      }
    }
  } catch (error) {
    console.warn('Failed to retrieve keys from localStorage:', error);
  }

  return keys;
};