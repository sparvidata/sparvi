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
        setActiveJobCount(0);
      } finally {
        setLoading(false);
      }
    };

    loadJobCount();

    // Poll for job count updates every 30 seconds
    const interval = setInterval(loadJobCount, 30000);
    return () => clearInterval(interval);
  }, []);

  return {
    activeJobCount,
    loading
  };
};