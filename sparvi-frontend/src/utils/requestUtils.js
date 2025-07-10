/**
 * Enhanced request deduplication and circuit breaker utilities
 * Prevents infinite loops and manages failed requests gracefully
 */

// Store for tracking ongoing requests with timestamps
const ongoingRequests = new Map();

// Circuit breaker state for different endpoints
const circuitBreakers = new Map();

// Store for abort controllers by request ID
const abortControllers = new Map();

// Request frequency tracking
const requestFrequency = new Map();
const REQUEST_THROTTLE_WINDOW = 5000; // 5 seconds
const MAX_REQUESTS_PER_WINDOW = 3;

// Configuration
const CIRCUIT_BREAKER_CONFIG = {
  failureThreshold: 5,        // Number of failures before opening circuit
  resetTimeoutMs: 30000,      // 30 seconds before attempting reset
  monitorWindowMs: 60000,     // 1 minute window for tracking failures
};

const RETRY_CONFIG = {
  maxRetries: 3,
  baseDelayMs: 1000,
  maxDelayMs: 10000,
};

/**
 * Enhanced Circuit breaker implementation
 */
class CircuitBreaker {
  constructor(key, config = CIRCUIT_BREAKER_CONFIG) {
    this.key = key;
    this.config = config;
    this.state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN
    this.failures = [];
    this.lastFailureTime = null;
    this.nextAttemptTime = null;
  }

  canExecute() {
    const now = Date.now();

    // Clean old failures outside the monitoring window
    this.failures = this.failures.filter(
      time => now - time < this.config.monitorWindowMs
    );

    switch (this.state) {
      case 'CLOSED':
        return true;

      case 'OPEN':
        if (now >= this.nextAttemptTime) {
          this.state = 'HALF_OPEN';
          console.log(`Circuit breaker HALF_OPEN for ${this.key}`);
          return true;
        }
        return false;

      case 'HALF_OPEN':
        return true;

      default:
        return false;
    }
  }

  recordSuccess() {
    this.failures = [];
    this.state = 'CLOSED';
    this.nextAttemptTime = null;
    console.log(`Circuit breaker CLOSED for ${this.key}`);
  }

  recordFailure() {
    const now = Date.now();
    this.failures.push(now);
    this.lastFailureTime = now;

    if (this.failures.length >= this.config.failureThreshold) {
      this.state = 'OPEN';
      this.nextAttemptTime = now + this.config.resetTimeoutMs;
      console.warn(`Circuit breaker OPENED for ${this.key}. Next attempt at ${new Date(this.nextAttemptTime)}`);
    }
  }

  getState() {
    return {
      state: this.state,
      failures: this.failures.length,
      nextAttemptTime: this.nextAttemptTime,
      canExecute: this.canExecute()
    };
  }
}

/**
 * Get or create circuit breaker for endpoint
 */
function getCircuitBreaker(key) {
  if (!circuitBreakers.has(key)) {
    circuitBreakers.set(key, new CircuitBreaker(key));
  }
  return circuitBreakers.get(key);
}

/**
 * Enhanced request key generation with better uniqueness
 */
function getRequestKey(url, method = 'GET', params = {}, requestId = null) {
  const paramString = Object.keys(params).length > 0
    ? `?${new URLSearchParams(params).toString()}`
    : '';

  const baseKey = `${method}:${url}${paramString}`;

  // If requestId is provided, use it for uniqueness
  if (requestId) {
    return `${baseKey}#${requestId}`;
  }

  return baseKey;
}

/**
 * Check request frequency to prevent spam
 */
function checkRequestFrequency(key) {
  const now = Date.now();

  if (!requestFrequency.has(key)) {
    requestFrequency.set(key, []);
  }

  const requests = requestFrequency.get(key);

  // Clean old requests outside the window
  const recentRequests = requests.filter(time => now - time < REQUEST_THROTTLE_WINDOW);
  requestFrequency.set(key, recentRequests);

  // Check if we're over the limit
  if (recentRequests.length >= MAX_REQUESTS_PER_WINDOW) {
    console.warn(`Request frequency limit exceeded for ${key}. ${recentRequests.length} requests in last ${REQUEST_THROTTLE_WINDOW}ms`);
    return false;
  }

  // Record this request
  recentRequests.push(now);
  requestFrequency.set(key, recentRequests);

  return true;
}

/**
 * Sleep utility for delays
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Calculate exponential backoff delay
 */
function calculateBackoffDelay(attempt, baseDelay = RETRY_CONFIG.baseDelayMs, maxDelay = RETRY_CONFIG.maxDelayMs) {
  const delay = baseDelay * Math.pow(2, attempt - 1);
  const jitter = Math.random() * 0.1 * delay; // Add 10% jitter
  return Math.min(delay + jitter, maxDelay);
}

/**
 * Enhanced fetch with circuit breaker, deduplication, and retry logic
 */
export async function safeFetch(url, options = {}) {
  const {
    method = 'GET',
    params = {},
    retries = RETRY_CONFIG.maxRetries,
    timeout = 10000,
    requestId = null,
    skipDeduplication = false,
    skipFrequencyCheck = false,
    ...fetchOptions
  } = options;

  // Generate unique key for this request
  const requestKey = getRequestKey(url, method, params, requestId);
  const frequencyKey = getRequestKey(url, method, params); // Without requestId for frequency check

  // Check request frequency (unless skipped)
  if (!skipFrequencyCheck && !checkRequestFrequency(frequencyKey)) {
    const error = new Error(`Request frequency limit exceeded for ${url}`);
    error.frequencyLimited = true;
    throw error;
  }

  // Check circuit breaker
  const circuitBreaker = getCircuitBreaker(url);
  if (!circuitBreaker.canExecute()) {
    const error = new Error(`Circuit breaker is OPEN for ${url}`);
    error.circuitBreakerOpen = true;
    error.nextAttemptTime = circuitBreaker.nextAttemptTime;
    throw error;
  }

  // Check for ongoing identical request (unless skipped)
  if (!skipDeduplication && ongoingRequests.has(requestKey)) {
    console.log(`Deduplicating request: ${requestKey}`);
    return ongoingRequests.get(requestKey);
  }

  // Create the actual request promise
  const requestPromise = executeRequestWithRetry(url, {
    method,
    params,
    retries,
    timeout,
    circuitBreaker,
    requestId,
    ...fetchOptions
  });

  // Store the promise for deduplication (unless skipped)
  if (!skipDeduplication) {
    ongoingRequests.set(requestKey, requestPromise);
  }

  try {
    const result = await requestPromise;
    circuitBreaker.recordSuccess();
    return result;
  } catch (error) {
    // Don't record failure for cancelled or frequency limited requests
    if (!error.cancelled && !error.frequencyLimited) {
      circuitBreaker.recordFailure();
    }
    throw error;
  } finally {
    // Clean up the ongoing request
    if (!skipDeduplication) {
      ongoingRequests.delete(requestKey);
    }
  }
}

/**
 * Execute request with retry logic
 */
async function executeRequestWithRetry(url, options) {
  const { method, params, retries, timeout, circuitBreaker, requestId, ...fetchOptions } = options;

  // Build full URL with params
  const fullUrl = Object.keys(params).length > 0
    ? `${url}?${new URLSearchParams(params).toString()}`
    : url;

  let lastError;

  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      // Create abort controller for timeout
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeout);

      console.log(`Making request (attempt ${attempt}/${retries + 1}): ${method} ${fullUrl}${requestId ? ` [${requestId}]` : ''}`);

      const response = await fetch(fullUrl, {
        method,
        signal: controller.signal,
        ...fetchOptions
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorText = await response.text();
        let errorData;
        try {
          errorData = JSON.parse(errorText);
        } catch {
          errorData = { error: errorText };
        }

        const error = new Error(errorData.error || `HTTP ${response.status}`);
        error.status = response.status;
        error.response = response;
        error.data = errorData;
        throw error;
      }

      const data = await response.json();
      console.log(`Request successful: ${method} ${fullUrl}${requestId ? ` [${requestId}]` : ''}`);
      return data;

    } catch (error) {
      lastError = error;

      // Handle abort errors
      if (error.name === 'AbortError') {
        const timeoutError = new Error('Request timeout');
        timeoutError.timeout = true;
        timeoutError.cancelled = true;
        throw timeoutError;
      }

      // Don't retry on auth errors (401, 403) or client errors (400-499)
      if (error.status && error.status >= 400 && error.status < 500) {
        console.warn(`Non-retryable error (${error.status}): ${error.message}`);
        break;
      }

      // Don't retry if circuit breaker is open
      if (error.circuitBreakerOpen) {
        console.warn('Circuit breaker open, not retrying');
        break;
      }

      // If we have more attempts, wait before retrying
      if (attempt <= retries) {
        const delay = calculateBackoffDelay(attempt);
        console.warn(`Request failed (attempt ${attempt}/${retries + 1}), retrying in ${delay}ms: ${error.message}`);
        await sleep(delay);
      }
    }
  }

  throw lastError;
}

/**
 * Get or create abort controller for a request
 * @param {string} requestId - Unique identifier for the request
 * @returns {AbortController} Abort controller for the request
 */
export function getRequestAbortController(requestId) {
  if (!abortControllers.has(requestId)) {
    abortControllers.set(requestId, new AbortController());
  }
  return abortControllers.get(requestId);
}

/**
 * Mark a request as completed and clean up its abort controller
 * @param {string} requestId - Unique identifier for the request
 */
export function requestCompleted(requestId) {
  if (abortControllers.has(requestId)) {
    abortControllers.delete(requestId);
  }
}

/**
 * Debounce function to limit the rate of function calls
 * @param {Function} func - Function to debounce
 * @param {number} wait - Wait time in milliseconds
 * @param {boolean} immediate - Whether to execute on leading edge
 * @returns {Function} Debounced function
 */
export function debounce(func, wait, immediate = false) {
  let timeout;

  return function executedFunction(...args) {
    const later = () => {
      timeout = null;
      if (!immediate) func.apply(this, args);
    };

    const callNow = immediate && !timeout;
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);

    if (callNow) func.apply(this, args);
  };
}

/**
 * Enhanced batch requests with better error handling
 * @param {Array} requests - Array of request objects
 * @param {Object} options - Request options
 * @returns {Promise} Promise resolving to batch results
 */
export async function batchRequests(requests, options = {}) {
  const {
    timeout = 30000,
    waitForAuthentication = true,
    retries = 1,
    signal = null
  } = options;

  // Filter out empty or invalid requests
  const validRequests = requests.filter(req => req && req.path);

  if (validRequests.length === 0) {
    console.warn('No valid requests to batch');
    return {};
  }

  try {
    console.log(`Making batch request with ${validRequests.length} requests`);

    // Use safeFetch for the batch request with proper deduplication
    const response = await safeFetch('/batch', {
      method: 'POST',
      body: JSON.stringify({ requests: validRequests }),
      timeout,
      requestId: `batch-${Date.now()}`,
      skipDeduplication: false, // Allow deduplication for batch requests
      skipFrequencyCheck: true, // Skip frequency check for batch requests
      headers: {
        'Content-Type': 'application/json'
      },
      signal
    });

    return response.results || {};
  } catch (error) {
    console.error('Batch request failed:', error);

    // Return partial results if available
    if (error.response && error.data && error.data.results) {
      console.warn('Returning partial batch results due to error');
      return error.data.results;
    }

    throw error;
  }
}

/**
 * Get circuit breaker status for monitoring
 */
export function getCircuitBreakerStatus() {
  const status = {};
  for (const [key, breaker] of circuitBreakers.entries()) {
    status[key] = breaker.getState();
  }
  return status;
}

/**
 * Reset circuit breaker (for manual recovery)
 */
export function resetCircuitBreaker(key) {
  if (circuitBreakers.has(key)) {
    circuitBreakers.delete(key);
    console.log(`Circuit breaker reset for ${key}`);
  }
}

/**
 * Reset all circuit breakers
 */
export function resetAllCircuitBreakers() {
  circuitBreakers.clear();
  ongoingRequests.clear();
  requestFrequency.clear();
  console.log('All circuit breakers and request tracking reset');
}

/**
 * Get ongoing requests for monitoring
 */
export function getOngoingRequests() {
  return Array.from(ongoingRequests.keys());
}

/**
 * Get request frequency stats for monitoring
 */
export function getRequestFrequencyStats() {
  const stats = {};
  for (const [key, requests] of requestFrequency.entries()) {
    stats[key] = {
      count: requests.length,
      lastRequest: requests.length > 0 ? new Date(Math.max(...requests)) : null
    };
  }
  return stats;
}

/**
 * Cancel all ongoing requests (useful for cleanup)
 */
export function cancelAllRequests() {
  console.log('Cancelling all ongoing requests');
  ongoingRequests.clear();

  // Also abort all ongoing controllers
  for (const controller of abortControllers.values()) {
    try {
      controller.abort();
    } catch (error) {
      console.warn('Error aborting controller:', error);
    }
  }
  abortControllers.clear();
}

/**
 * Clear old tracking data (useful for cleanup)
 */
export function cleanupOldTracking() {
  const now = Date.now();

  // Clean up old request frequency data
  for (const [key, requests] of requestFrequency.entries()) {
    const recentRequests = requests.filter(time => now - time < REQUEST_THROTTLE_WINDOW * 2);
    if (recentRequests.length === 0) {
      requestFrequency.delete(key);
    } else {
      requestFrequency.set(key, recentRequests);
    }
  }
}

// Periodic cleanup
setInterval(cleanupOldTracking, 60000); // Every minute