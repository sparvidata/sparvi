/**
 * Request deduplication and circuit breaker utilities
 * Prevents infinite loops and manages failed requests gracefully
 */

// Store for tracking ongoing requests
const ongoingRequests = new Map();

// Circuit breaker state for different endpoints
const circuitBreakers = new Map();

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
 * Circuit breaker implementation
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
 * Generate a unique key for request deduplication
 */
function getRequestKey(url, method = 'GET', params = {}) {
  const paramString = Object.keys(params).length > 0
    ? `?${new URLSearchParams(params).toString()}`
    : '';
  return `${method}:${url}${paramString}`;
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
    ...fetchOptions
  } = options;

  // Generate unique key for this request
  const requestKey = getRequestKey(url, method, params);

  // Check circuit breaker
  const circuitBreaker = getCircuitBreaker(url);
  if (!circuitBreaker.canExecute()) {
    const error = new Error(`Circuit breaker is OPEN for ${url}`);
    error.circuitBreakerOpen = true;
    error.nextAttemptTime = circuitBreaker.nextAttemptTime;
    throw error;
  }

  // Check for ongoing identical request
  if (ongoingRequests.has(requestKey)) {
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
    ...fetchOptions
  });

  // Store the promise for deduplication
  ongoingRequests.set(requestKey, requestPromise);

  try {
    const result = await requestPromise;
    circuitBreaker.recordSuccess();
    return result;
  } catch (error) {
    circuitBreaker.recordFailure();
    throw error;
  } finally {
    // Clean up the ongoing request
    ongoingRequests.delete(requestKey);
  }
}

/**
 * Execute request with retry logic
 */
async function executeRequestWithRetry(url, options) {
  const { method, params, retries, timeout, circuitBreaker, ...fetchOptions } = options;

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
      return data;

    } catch (error) {
      lastError = error;

      // Don't retry on auth errors (401, 403) or client errors (400-499)
      if (error.status && error.status >= 400 && error.status < 500) {
        break;
      }

      // Don't retry if circuit breaker is open
      if (error.name === 'AbortError' || error.circuitBreakerOpen) {
        break;
      }

      // If we have more attempts, wait before retrying
      if (attempt <= retries) {
        const delay = calculateBackoffDelay(attempt);
        console.warn(`Request failed (attempt ${attempt}/${retries + 1}), retrying in ${delay}ms:`, error.message);
        await sleep(delay);
      }
    }
  }

  throw lastError;
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
  console.log('All circuit breakers reset');
}

/**
 * Get ongoing requests for monitoring
 */
export function getOngoingRequests() {
  return Array.from(ongoingRequests.keys());
}

/**
 * Cancel all ongoing requests (useful for cleanup)
 */
export function cancelAllRequests() {
  ongoingRequests.clear();
}