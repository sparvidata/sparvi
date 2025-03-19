// src/hooks/useDebounce.js
import { useState, useEffect } from 'react';
import { debounce } from '../utils/requestUtils';

/**
 * Custom hook for debouncing a value (e.g. search input)
 * @param {any} value - The value to debounce
 * @param {number} delay - Delay in milliseconds
 * @returns {any} The debounced value
 */
const useDebounce = (value, delay = 300) => {
  const [debouncedValue, setDebouncedValue] = useState(value);

  useEffect(() => {
    // Set up the debounce timer
    const handler = setTimeout(() => {
      setDebouncedValue(value);
    }, delay);

    // Clean up the timer when value changes again before delay is complete
    return () => {
      clearTimeout(handler);
    };
  }, [value, delay]);

  return debouncedValue;
};

export default useDebounce;