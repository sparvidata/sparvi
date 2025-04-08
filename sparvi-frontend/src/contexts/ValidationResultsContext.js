// src/contexts/ValidationResultsContext.js

import React, { createContext, useContext, useState, useEffect } from 'react';
import { useConnection } from './EnhancedConnectionContext';
import { validationsAPI } from '../api/enhancedApiService';
import { processValidationResults, getValidationTrends } from '../utils/validationResultsProcessor';

// Helper function to process validation summary data into metrics format
const processValidationSummary = (summary, tableName) => {
  // If no summary or it doesn't have data for this table, return null
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
    error: null
  });

  // State for selected table
  const [selectedTable, setSelectedTable] = useState(null);

  // Effect to load results when table changes
  useEffect(() => {
    if (selectedTable) {
      console.log(`Table selection changed to: ${selectedTable}, loading results...`);

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

      // Fetch validation summary first to get basic metrics
      let summaryData = null;
      try {
        console.log("Fetching validation summary...");
        const summary = await validationsAPI.getSummary(connectionId);
        console.log("Validation summary response:", summary);

        // Process the summary data
        summaryData = processValidationSummary(summary, tableName);
        console.log("Processed summary data:", summaryData);
      } catch (error) {
        console.error("Error fetching validation summary:", error);
      }

      // Fetch latest validation results
      let latestResults;
      try {
        console.log(`Fetching latest validation results for ${tableName}...`);
        latestResults = await validationsAPI.getLatestValidationResults(
          connectionId,
          tableName
        );
        console.log("Latest results response:", latestResults);
      } catch (error) {
        console.error("Error fetching latest results:", error);
        latestResults = { results: [] };
      }

      // Fetch historical validation results (increased limit for better trends)
      let historyResults;
      try {
        console.log(`Fetching validation history for ${tableName} with connectionId ${connectionId}...`);

        // Log what the API function is expecting
        console.log("validationsAPI.getValidationHistory parameters:", {
          tableName,
          connectionId,
          options: { limit: 100 }
        });

        historyResults = await validationsAPI.getValidationHistory(
          tableName,
          connectionId,
          { limit: 100 } // Increased limit for better historical data
        );
        console.log("History results response:", historyResults);
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
      console.log("Extracted history data:", historyData.length, "records");

      // Extract current data
      let currentData = [];
      if (latestResults?.results && Array.isArray(latestResults.results)) {
        currentData = latestResults.results;
      } else if (Array.isArray(latestResults)) {
        currentData = latestResults;
      }
      console.log("Extracted current data:", currentData.length, "records");

      // Use summary data for metrics if available, otherwise process current data
      let metrics;
      if (summaryData) {
        metrics = summaryData;
      } else if (currentData.length > 0) {
        // Process the data to generate metrics from current results
        const processedResults = processValidationResults(currentData, []);
        metrics = processedResults.metrics;
        console.log("Using processed current data for metrics:", metrics);
      } else {
        // Default empty metrics
        metrics = {
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
      }

      // Generate trends from history data
      let trendsData = [];
      if (historyData.length > 0) {
        const trends = getValidationTrends(historyData, 7);
        trendsData = trends.trend || [];
        console.log("Generated trend data:", trendsData.length, "points");
      } else if (currentData.length > 0) {
        // Create a single data point for trend
        const today = new Date().toISOString().split('T')[0];

        // Count results by category for current data
        let passed = 0, failed = 0, error = 0;
        currentData.forEach(result => {
          if (result.error) error++;
          else if (result.is_valid === true || result.last_result === true) passed++;
          else if (result.is_valid === false || result.last_result === false) failed++;
        });

        // Calculate health score
        const total = currentData.length;
        const validResults = passed + failed;
        const health_score = validResults > 0 ? Math.round((passed / validResults) * 100) : 0;

        trendsData = [{
          date: today,
          total,
          passed,
          failed,
          error,
          health_score
        }];
        console.log("Created single trend data point:", trendsData);
      } else if (summaryData) {
        // Create a trend point from summary data if we have it
        const today = new Date().toISOString().split('T')[0];
        trendsData = [{
          date: today,
          total: summaryData.total || 0,
          passed: summaryData.counts.passed || 0,
          failed: summaryData.counts.failed || 0,
          error: summaryData.counts.error || 0,
          health_score: Math.round(summaryData.health_score) || 0
        }];
        console.log("Created trend point from summary data:", trendsData);
      }

      // Update state with all our data
      setValidationResults({
        current: currentData,
        history: historyData,
        trends: trendsData,
        metrics: metrics,
        lastFetched: new Date(),
        isLoading: false,
        error: null
      });
      console.log("ValidationResults state updated successfully with:", {
        currentDataCount: currentData.length,
        historyDataCount: historyData.length,
        trendsDataCount: trendsData.length,
        metrics: metrics
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
    if (!tableName) return;

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