import { useState, useEffect, useCallback } from 'react';
import { useValidationResults } from '../contexts/ValidationResultsContext';
import validationService from '../services/validationService';

/**
 * Custom hook to manage validation rules list data
 * @param {string} tableName - The table name
 * @param {string} connectionId - The connection ID
 * @param {Array} initialValidations - Initial validations array (optional)
 * @returns {Object} Validation rules state and functions
 */
export const useValidationRulesList = (tableName, connectionId, initialValidations = []) => {
  const [validations, setValidations] = useState(initialValidations);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const { updateResultsAfterRun } = useValidationResults();

  // Load validation rules using the service
  const loadRules = useCallback(async (forceFresh = false) => {
    if (!tableName || !connectionId) return;

    setLoading(true);
    setError(null);

    try {
      const rules = await validationService.getRules(connectionId, tableName, { forceFresh });
      setValidations(rules);
      return rules;
    } catch (err) {
      console.error("Error loading validation rules:", err);
      setError(err.message || "Failed to load validation rules");
      return [];
    } finally {
      setLoading(false);
    }
  }, [tableName, connectionId]);

  // Run all validation rules
  const runAllRules = useCallback(async () => {
    if (!tableName || !connectionId) return;

    setLoading(true);

    try {
      const result = await validationService.runValidations(connectionId, tableName);

      // Update validation rules with results
      const updatedRules = validations.map(rule => {
        const matchingResult = result.results.find(r => r.rule_name === rule.rule_name);
        if (!matchingResult) return rule;

        return {
          ...rule,
          last_result: matchingResult.is_valid,
          actual_value: matchingResult.actual_value,
          error: matchingResult.error,
          last_run_at: new Date().toISOString()
        };
      });

      setValidations(updatedRules);

      // Update the validation results context
      if (updateResultsAfterRun) {
        updateResultsAfterRun(result.results, tableName);
      }

      return result;
    } catch (err) {
      setError(err.message || "Failed to run validations");
      throw err;
    } finally {
      setLoading(false);
    }
  }, [tableName, connectionId, validations, updateResultsAfterRun]);

  // Run a single validation rule
  const runSingleRule = useCallback(async (rule) => {
    if (!rule || !tableName || !connectionId) return;

    try {
      const result = await validationService.runValidations(connectionId, tableName);

      const matchingResult = result.results.find(
        r => r.rule_name === rule.rule_name || r.rule_id === rule.id
      );

      if (matchingResult) {
        // Update just the one rule
        setValidations(prev => prev.map(r => {
          if (r.rule_name === rule.rule_name || r.id === rule.id) {
            return {
              ...r,
              last_result: matchingResult.is_valid,
              actual_value: matchingResult.actual_value,
              error: matchingResult.error,
              last_run_at: new Date().toISOString()
            };
          }
          return r;
        }));
      }

      return matchingResult;
    } catch (err) {
      setError(err.message || "Failed to run validation");
      throw err;
    }
  }, [tableName, connectionId]);

  // Initial load on mount
  useEffect(() => {
    if (tableName && connectionId && !initialValidations.length) {
      loadRules();
    } else if (initialValidations.length > 0) {
      setValidations(initialValidations);
    }
  }, [tableName, connectionId, initialValidations.length, loadRules]);

  return {
    validations,
    setValidations,
    loading,
    error,
    loadRules,
    runAllRules,
    runSingleRule
  };
};