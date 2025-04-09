import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { useConnection } from './EnhancedConnectionContext';
import { validationsAPI } from '../api/enhancedApiService';
import { processValidationResults, getValidationTrends } from '../utils/validationResultsProcessor';
import axios from "axios";
import validationService from "../services/validationService";

// Helper function to process validation summary data into metrics format
const processValidationSummary = (summary, tableName) => {
  // Same as before - keeping your existing implementation
  if (!summary || !summary.validations_by_table) {
    return null;
  }

  // If we're looking for a specific table
  if (tableName && summary.validations_by_table[tableName]) {
    const tableData = summary.validations_by_table[tableName];
    return {
      health_score: tableData.health_score || 0,
      counts: {
        passed: tableData.passing || 0,
        failed: tableData.failing || 0,
        error: 0,
        unknown: tableData.unknown || 0
      },
      total: tableData.total || 0,
      avg_execution_time: 0
    };
  }

  // Otherwise return overall metrics
  return {
    health_score: summary.overall_health_score || 0,
    counts: {
      passed: summary.passing_count || 0,
      failed: summary.failing_count || 0,
      error: 0,
      unknown: summary.unknown_count || 0
    },
    total: summary.total_count || 0,
    avg_execution_time: 0
  };
};

// Create the context
const ValidationResultsContext = createContext();

// Provider component
export const ValidationResultsProvider = ({ children }) => {
  const { activeConnection } = useConnection();

  // State for validation results
  const [validationResults, setValidationResults] = useState({
    current: [],
    history: [],
    trends: [],
    metrics: null,
    lastFetched: null,
    isLoading: false,
    isLoadingRules: false,
    isLoadingResults: false,
    isLoadingHistory: false,
    error: null
  });

  // State for selected table
  const [selectedTable, setSelectedTable] = useState(null);

  // Track loaded state
  const [rulesLoaded, setRulesLoaded] = useState(false);
  const [resultsLoaded, setResultsLoaded] = useState(false);
  const [historyLoaded, setHistoryLoaded] = useState(false);

  // Use a ref to prevent duplicate loading
  const loadingInProgress = React.useRef(false);

  // Load validation rules - separate from other data
  const loadValidationRules = useCallback(async (tableName, connectionId) => {
    if (!tableName || !connectionId) return [];

    try {
      setValidationResults(prev => ({
        ...prev,
        isLoadingRules: true
      }));

      console.log(`Fetching validation rules for table: ${tableName} using validationService`);

      // Use the validationService instead of direct API call
      const rules = await validationService.getRules(connectionId, tableName);

      setRulesLoaded(true);
      console.log(`Loaded ${rules.length} validation rules for ${tableName}`);

      // Return the rules but also update the state
      setValidationResults(prev => ({
        ...prev,
        current: rules,
        isLoadingRules: false
      }));

      return rules;
    } catch (error) {
      console.error(`Error loading validation rules for ${tableName}:`, error);
      setValidationResults(prev => ({
        ...prev,
        isLoadingRules: false,
        error: `Failed to load validation rules: ${error.message}`
      }));
      return [];
    }
  }, []);

  // Load latest validation results
  const loadLatestResults = useCallback(async (tableName, connectionId, rules) => {
    if (!tableName || !connectionId) return;

    try {
      setValidationResults(prev => ({
        ...prev,
        isLoadingResults: true
      }));

      console.log(`Fetching latest validation results for ${tableName}...`);

      try {
        // Use validationService instead of direct API call
        const latestResults = await validationService.getLatestResults(connectionId, tableName);

        console.log("Latest validation results received:", latestResults);

        // Extract the results and metrics
        const resultsArray = latestResults.results || [];
        const metrics = latestResults.metrics || null;

        // Update the state with the new data
        setValidationResults(prev => ({
          ...prev,
          current: rules || [], // Use the rules we have
          metrics: metrics,
          isLoadingResults: false,
          lastFetched: new Date(),
          resultsLoaded: true
        }));

        setResultsLoaded(true);

        return { currentData: resultsArray, metrics: metrics };
      } catch (error) {
        // Handle cancelled requests
        if (axios.isCancel(error) || error.cancelled) {
          console.warn(`Request for ${tableName} validation results was cancelled`, error);
          return { currentData: [], metrics: null };
        }
        throw error;
      }
    } catch (error) {
      console.error(`Error loading latest results for ${tableName}:`, error);

      // Ensure loading state is reset even on error
      setValidationResults(prev => ({
        ...prev,
        isLoadingResults: false,
        error: error.message || 'Failed to load validation results'
      }));

      return { currentData: [], metrics: null };
    }
  }, [setResultsLoaded]);

  // Load validation history
  const loadValidationHistory = useCallback(async (tableName, connectionId) => {
    if (!tableName || !connectionId) return;

    try {
      setValidationResults(prev => ({
        ...prev,
        isLoadingHistory: true
      }));

      console.log(`Fetching validation history for ${tableName} with connectionId ${connectionId}...`);
      const historyResults = await validationsAPI.getValidationHistory(
        tableName,
        connectionId,
        { limit: 30 }
      );

      // Extract history data
      let historyData = [];
      if (historyResults?.history && Array.isArray(historyResults.history)) {
        historyData = historyResults.history;
      } else if (Array.isArray(historyResults)) {
        historyData = historyResults;
      }

      console.log(`Loaded ${historyData.length} history records for ${tableName}`);

      // Process history data for better trend calculation
      // Group by date first
      const historyByDate = {};

      historyData.forEach(record => {
        if (!record.run_at) return;

        // Extract the date part
        const date = record.run_at.split('T')[0];

        if (!historyByDate[date]) {
          historyByDate[date] = {
            date,
            total: 0,
            passed: 0,
            failed: 0,
            error: 0
          };
        }

        // Count by result status
        historyByDate[date].total++;

        if (record.error) {
          historyByDate[date].error++;
        } else if (record.is_valid === true) {
          historyByDate[date].passed++;
        } else if (record.is_valid === false) {
          historyByDate[date].failed++;
        }
      });

      // Convert to array and calculate health score
      const trendsData = Object.values(historyByDate).map(day => {
        const validResults = day.passed + day.failed;
        const health_score = validResults > 0
          ? Math.round((day.passed / validResults) * 100)
          : 0;

        return {
          ...day,
          health_score
        };
      }).sort((a, b) => a.date.localeCompare(b.date));

      console.log("Generated trend data:", trendsData);

      setHistoryLoaded(true);
      setValidationResults(prev => ({
        ...prev,
        history: historyData,
        trends: trendsData,
        isLoadingHistory: false,
        lastFetched: new Date()
      }));

      return { historyData, trendsData };
    } catch (error) {
      console.error(`Error loading validation history for ${tableName}:`, error);
      setValidationResults(prev => ({
        ...prev,
        isLoadingHistory: false
      }));
      return { historyData: [], trendsData: [] };
    }
  }, []);

  // Main function to load results for a table - initiates parallel loading
  const loadResultsForTable = useCallback(async (tableName, connectionId) => {
    if (!tableName || !connectionId || loadingInProgress.current) {
      return;
    }

    try {
      // Set loading flags
      loadingInProgress.current = true;
      setRulesLoaded(false);
      setResultsLoaded(false);
      setHistoryLoaded(false);

      // Clear previous results and set loading state
      setValidationResults({
        current: [],
        history: [],
        trends: [],
        metrics: null,
        lastFetched: null,
        isLoading: true,
        isLoadingRules: true,
        isLoadingResults: true,
        isLoadingHistory: true,
        error: null
      });

      console.log(`Loading all data for table ${tableName} with connection ${connectionId}`);

      // Load rules first - we need these for the UI to show something quickly
      const rules = await loadValidationRules(tableName, connectionId);

      // Then start both processes in parallel
      const resultsPromise = loadLatestResults(tableName, connectionId, rules);
      const historyPromise = loadValidationHistory(tableName, connectionId);

      // Let them execute in parallel
      await Promise.allSettled([resultsPromise, historyPromise]);

      console.log(`All data loading complete for ${tableName}`);

      // Final update to clear main loading flag
      setValidationResults(prev => ({
        ...prev,
        isLoading: false
      }));
    } catch (error) {
      console.error('Error in loadResultsForTable:', error);
      setValidationResults(prev => ({
        ...prev,
        isLoading: false,
        error: error.message || 'Failed to load validation results'
      }));
    } finally {
      loadingInProgress.current = false;
    }
  }, [loadValidationRules, loadLatestResults, loadValidationHistory]);

  // Effect to load results when table or connection changes
  useEffect(() => {
    if (selectedTable && activeConnection?.id && !loadingInProgress.current) {
      console.log(`Table selection changed to: ${selectedTable}, loading results...`);
      loadResultsForTable(selectedTable, activeConnection.id);
    }
  }, [selectedTable, activeConnection?.id, loadResultsForTable]);

  // Force reload results
  const reloadResults = useCallback(() => {
    if (selectedTable && activeConnection?.id && !loadingInProgress.current) {
      loadResultsForTable(selectedTable, activeConnection.id);
    }
  }, [selectedTable, activeConnection?.id, loadResultsForTable]);

  // Update results after running validations
  const updateResultsAfterRun = useCallback((results, tableName) => {
    if (!tableName || !results || !Array.isArray(results)) return;

    try {
      console.log(`Updating results after run for table ${tableName} with ${results.length} results`);

      // Process the new results
      const processedResults = results.length > 0
        ? processValidationResults(results, [])
        : {
            processed: [],
            metrics: {
              health_score: 0,
              counts: { passed: 0, failed: 0, error: 0, unknown: 0 },
              total: 0
            }
          };

      // Update state
      setValidationResults(prev => {
        // Create new history array with latest results at the beginning
        const newHistory = [
          ...results.map(result => ({
            ...result,
            run_at: result.timestamp || result.last_run_at || new Date().toISOString()
          })),
          ...prev.history
        ];

        // Update the current rules with the new results
        const updatedRules = prev.current.map(rule => {
          const newResult = results.find(r => r.rule_name === rule.rule_name);
          if (newResult) {
            return {
              ...rule,
              last_result: newResult.is_valid,
              actual_value: newResult.actual_value,
              error: newResult.error,
              last_run_at: newResult.run_at || new Date().toISOString()
            };
          }
          return rule;
        });

        // Update trends
        const trends = getValidationTrends(newHistory, 30);

        return {
          current: updatedRules,
          history: newHistory,
          trends: trends.trend || [],
          metrics: processedResults.metrics,
          lastFetched: new Date(),
          isLoading: false,
          isLoadingRules: false,
          isLoadingResults: false,
          isLoadingHistory: false,
          error: null
        };
      });
    } catch (error) {
      console.error('Error updating validation results:', error);
    }
  }, []);

  // Clear results
  const clearResults = useCallback(() => {
    setValidationResults({
      current: [],
      history: [],
      trends: [],
      metrics: null,
      lastFetched: null,
      isLoading: false,
      isLoadingRules: false,
      isLoadingResults: false,
      isLoadingHistory: false,
      error: null
    });
    setSelectedTable(null);
    setRulesLoaded(false);
    setResultsLoaded(false);
    setHistoryLoaded(false);
  }, []);

  // Value to provide
  const value = {
    ...validationResults,
    selectedTable,
    setSelectedTable,
    loadResultsForTable,
    updateResultsAfterRun,
    reloadResults,
    clearResults,
    rulesLoaded,
    resultsLoaded,
    historyLoaded
  };

  return (
    <ValidationResultsContext.Provider value={value}>
      {children}
    </ValidationResultsContext.Provider>
  );
};

// Custom hook to use the context
export const useValidationResults = () => {
  const context = useContext(ValidationResultsContext);
  if (!context) {
    throw new Error('useValidationResults must be used within a ValidationResultsProvider');
  }
  return context;
};

export default ValidationResultsContext;