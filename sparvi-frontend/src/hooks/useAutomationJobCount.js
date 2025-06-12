import { useState, useEffect } from 'react';
import { automationAPI } from '../api/enhancedApiService';

export const useAutomationJobCount = () => {
  const [activeJobCount, setActiveJobCount] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadJobCount = async () => {
      try {
        setLoading(true);

        const response = await automationAPI.getJobs({
          status: 'running',
          forceFresh: true // Always get fresh data for job counts
        });

        if (response?.jobs) {
          setActiveJobCount(response.jobs.length);
        } else if (Array.isArray(response)) {
          setActiveJobCount(response.length);
        } else {
          setActiveJobCount(0);
        }
      } catch (error) {
        console.error('Error loading job count:', error);

        // Don't treat auth errors as fatal - just set count to 0
        if (error?.response?.status === 401) {
          console.log('Auth error loading job count, skipping update');
        } else {
          setActiveJobCount(0);
        }
      } finally {
        setLoading(false);
      }
    };

    // Initial load with a small delay to let auth settle
    const initialTimeout = setTimeout(loadJobCount, 1000);

    // Poll for job count updates every 60 seconds (increased from 30)
    const interval = setInterval(loadJobCount, 60000);

    return () => {
      clearTimeout(initialTimeout);
      clearInterval(interval);
    };
  }, []);

  return {
    activeJobCount,
    loading
  };
};