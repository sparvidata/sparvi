import { useState, useEffect } from 'react';
import { getSession } from '../api/supabase';

export const useAutomationJobCount = () => {
  const [activeJobCount, setActiveJobCount] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadJobCount = async () => {
      try {
        const session = await getSession();
        const token = session?.access_token;

        if (!token) {
          setLoading(false);
          return;
        }

        const response = await fetch('/api/automation/jobs?status=running', {
          headers: { 'Authorization': `Bearer ${token}` }
        });

        if (response.ok) {
          const data = await response.json();
          setActiveJobCount(data.jobs?.length || 0);
        }
      } catch (error) {
        console.error('Error loading job count:', error);
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