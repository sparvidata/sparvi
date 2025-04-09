import { useState, useEffect, useCallback } from 'react';
import validationService from '../services/validationService';
import {validationsAPI} from "../api/enhancedApiService";

export default function useValidations(connectionId, tableName) {
  const [validations, setValidations] = useState([]);
  const [metrics, setMetrics] = useState(null);
  const [trends, setTrends] = useState([]);
  const [loading, setLoading] = useState(false);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [runningValidation, setRunningValidation] = useState(false);
  const [runningRuleId, setRunningRuleId] = useState(null);
  const [error, setError] = useState(null);

  // Load validation rules and results
  const loadValidations = useCallback(async (force = false) => {
    if (!connectionId || !tableName) return;

    try {
      setLoading(true);
      setError(null);

      const data = await validationService.getValidations(connectionId, tableName);
      setValidations(data);

      // Calculate metrics
      const calculatedMetrics = validationService.calculateMetrics(data);
      setMetrics(calculatedMetrics);
    } catch (err) {
      console.error('Error loading validations:', err);
      setError(err.message || 'Failed to load validations');
    } finally {
      setLoading(false);
    }
  }, [connectionId, tableName]);

  // Load validation history
  const loadHistory = useCallback(async () => {
    if (!connectionId || !tableName) return;

    try {
      setLoadingHistory(true);

      const response = await validationsAPI.getValidationHistory(
        tableName,
        connectionId,
        { limit: 30 }
      );

      // Extract history data
      let historyData = [];
      if (response?.history && Array.isArray(response.history)) {
        historyData = response.history;
      } else if (Array.isArray(response)) {
        historyData = response;
      }

      // Process trend data
      const trendData = validationService.processTrendData(historyData);
      setTrends(trendData);
    } catch (err) {
      console.error('Error loading validation history:', err);
    } finally {
      setLoadingHistory(false);
    }
  }, [connectionId, tableName]);

  // Run all validations
  const runAllValidations = useCallback(async () => {
    if (!connectionId || !tableName) return;

    try {
      setRunningValidation(true);

      const results = await validationService.runValidations(connectionId, tableName);

      // Reload validations to get updated data
      await loadValidations(true);
      await loadHistory();

      return results;
    } catch (err) {
      console.error('Error running validations:', err);
      setError(err.message || 'Failed to run validations');
      throw err;
    } finally {
      setRunningValidation(false);
    }
  }, [connectionId, tableName, loadValidations, loadHistory]);

  // Run a single validation
  const runSingleValidation = useCallback(async (validationRule) => {
    if (!connectionId || !tableName || !validationRule) return;

    try {
      setRunningRuleId(validationRule.id || validationRule.rule_name);

      const result = await validationService.runSingleValidation(
        connectionId,
        tableName,
        validationRule
      );

      // Update the validation in the list
      if (result) {
        setValidations(prev => prev.map(v => {
          if (v.rule_name === validationRule.rule_name) {
            return {
              ...v,
              last_result: result.error ? null : result.is_valid,
              actual_value: result.actual_value,
              error: result.error,
              last_run_at: new Date().toISOString(),
              execution_time_ms: result.execution_time_ms
            };
          }
          return v;
        }));

        // Update metrics
        const updatedMetrics = validationService.calculateMetrics(
          validations.map(v => {
            if (v.rule_name === validationRule.rule_name) {
              return {
                ...v,
                last_result: result.error ? null : result.is_valid,
                actual_value: result.actual_value,
                error: result.error,
                last_run_at: new Date().toISOString()
              };
            }
            return v;
          })
        );
        setMetrics(updatedMetrics);
      }

      return result;
    } catch (err) {
      console.error('Error running validation:', err);
      throw err;
    } finally {
      setRunningRuleId(null);
    }
  }, [connectionId, tableName, validations]);

  // Load data when dependencies change
  useEffect(() => {
    if (connectionId && tableName) {
      loadValidations();
      loadHistory();
    }
  }, [connectionId, tableName, loadValidations, loadHistory]);

  return {
    validations,
    metrics,
    trends,
    loading,
    loadingHistory,
    runningValidation,
    runningRuleId,
    error,
    loadValidations,
    loadHistory,
    runAllValidations,
    runSingleValidation
  };
}