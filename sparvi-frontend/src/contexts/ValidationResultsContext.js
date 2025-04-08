// src/contexts/ValidationResultsContext.js - UPDATED VERSION
import React, { createContext, useContext, useState, useEffect } from 'react';
import { useConnection } from './EnhancedConnectionContext';
import { validationsAPI } from '../api/enhancedApiService';
import { processValidationResults, getValidationTrends } from '../utils/validationResultsProcessor';

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
    error: null
  });

  // State for selected table
  const [selectedTable, setSelectedTable] = useState(null);

  // Effect to load results when table changes
  useEffect(() => {
    if (selectedTable) {
      // Clear previous results when table changes
      setValidationResults(prev => ({
        ...prev,
        current: [],
        history: [],
        trends: [],
        metrics: null,
        isLoading: true,
        error: null
      }));

      // Load results for the new table
      if (activeConnection?.id) {
        loadResultsForTable(selectedTable, activeConnection.id);
      }
    }
  }, [selectedTable, activeConnection?.id]);

  // Load results for a specific table
  const loadResultsForTable = async (tableName, connectionId) => {
    if (!tableName || !connectionId) {
      console.log("Missing table name or connection ID");
      return;
    }

    try {
      console.log(`Loading results for table ${tableName} with connection ${connectionId}`);

      setValidationResults(prev => ({
        ...prev,
        isLoading: true,
        error: null
      }));

      // Fetch latest validation results
      let latestResults;
      try {
        latestResults = await validationsAPI.getLatestValidationResults(
          connectionId,
          tableName
        );
        console.log("Latest results:", latestResults);
      } catch (error) {
        console.error("Error fetching latest results:", error);
        latestResults = { results: [] };
      }

      // Fetch historical validation results (increased limit for better trends)
      let historyResults;
      try {
        historyResults = await validationsAPI.getValidationHistory(
          tableName,
          connectionId,
          { limit: 100 } // Increased limit for better historical data
        );
        console.log("History results:", historyResults);
      } catch (error) {
        console.error("Error fetching history:", error);
        historyResults = { history: [] };
      }

      // Extract the history array
      let historyData = [];
      if (historyResults?.history && Array.isArray(historyResults.history)) {
        historyData = historyResults.history;
      } else if (Array.isArray(historyResults)) {
        historyData = historyResults;
      }

      // Extract current data
      let currentData = [];
      if (latestResults?.results && Array.isArray(latestResults.results)) {
        currentData = latestResults.results;
      } else if (Array.isArray(latestResults)) {
        currentData = latestResults;
      }

      // Process the data to generate metrics
      const processedResults = processValidationResults(currentData, []);
      const trends = getValidationTrends(historyData, 7);

      // Create default metrics if none exist
      const defaultMetrics = {
        health_score: 0,
        counts: {
          passed: 0,
          failed: 0,
          error: 0,
          unknown: 0
        },
        total: 0,
        avg_execution_time: 0
      };

      // Use processed metrics or fetch from summary if available
      let metrics = processedResults.metrics || defaultMetrics;
      
      // If we have no metrics but the table is selected, try to get metrics from the validation summary
      if (metrics.total === 0 && selectedTable) {
        try {
          const summary = await validationsAPI.getSummary(connectionId);
          console.log("Validation summary:", summary);
          
          // Check if we have data for the selected table
          if (summary?.validations_by_table && summary.validations_by_table[tableName]) {
            const tableData = summary.validations_by_table[tableName];
            
            // Update metrics with summary data
            metrics = {
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
        } catch (error) {
          console.error("Error fetching validation summary:", error);
        }
      }

      setValidationResults({
        current: currentData,
        history: historyData,
        trends: trends.trend || [],
        metrics: metrics,
        lastFetched: new Date(),
        isLoading: false,
        error: null
      });
    } catch (error) {
      console.error('Error loading validation results:', error);
      setValidationResults(prev => ({
        ...prev,
        isLoading: false,
        error: error.message || 'Failed to load validation results'
      }));
    }
  };

  // Force reload results
  const reloadResults = () => {
    if (selectedTable && activeConnection?.id) {
      loadResultsForTable(selectedTable, activeConnection.id);
    }
  };

  // Update results after running validations
  const updateResultsAfterRun = (results, tableName) => {
    if (!results || !tableName) return;

    try {
      // Process the new results
      const processedResults = processValidationResults(
        results,
        [] // Pass empty array as we don't have rules here
      );

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

        // Update trends
        const trends = getValidationTrends(newHistory, 7);

        return {
          current: processedResults.processed || [],
          history: newHistory,
          trends: trends.trend || [],
          metrics: processedResults.metrics,
          lastFetched: new Date(),
          isLoading: false,
          error: null
        };
      });
    } catch (error) {
      console.error('Error updating validation results:', error);
    }
  };

  // Clear results
  const clearResults = () => {
    setValidationResults({
      current: [],
      history: [],
      trends: [],
      metrics: null,
      lastFetched: null,
      isLoading: false,
      error: null
    });
    setSelectedTable(null);
  };

  // Value to provide
  const value = {
    ...validationResults,
    selectedTable,
    setSelectedTable,
    loadResultsForTable,
    updateResultsAfterRun,
    reloadResults,
    clearResults
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