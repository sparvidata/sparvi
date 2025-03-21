// src/hooks/useValidationsData.js
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { validationsAPI } from '../api/enhancedApiService';

/**
 * Custom hook to fetch validation summary data
 * @param {string} connectionId - The connection ID
 * @param {Object} options - Additional options for the query
 * @returns {Object} Query result object
 */
export const useValidationsSummary = (connectionId, options = {}) => {
  const {
    enabled = !!connectionId,
    refetchInterval = false,
    ...queryOptions
  } = options;

  return useQuery({
    queryKey: ['validations-summary', connectionId],
    queryFn: () => validationsAPI.getSummary(connectionId, { forceFresh: false }),
    enabled: enabled,
    refetchInterval: refetchInterval,
    ...queryOptions,
    // Transform the data to ensure consistent structure
    select: (data) => {
      console.log("Validations summary data:", data);
      // Handle both direct and nested data structures
      const summary = data?.data || data;
      return {
        total_count: summary?.total_count || 0,
        passed_count: summary?.passed_count || 0,
        failed_count: summary?.failed_count || 0,
        not_run_count: summary?.not_run_count || 0
      };
    },
    // Don't refetch on window focus for validation data
    refetchOnWindowFocus: false,
    // Keep the data for 30 minutes
    cacheTime: 30 * 60 * 1000,
    // Consider it stale after 10 minutes
    staleTime: 10 * 60 * 1000,
  });
};

/**
 * Custom hook to fetch validations for a specific table
 * @param {string} tableName - The table name to fetch validations for
 * @param {Object} options - Additional options for the query
 * @returns {Object} Query result object
 */
export const useTableValidations = (tableName, options = {}) => {
  const {
    enabled = !!tableName,
    refetchInterval = false,
    queryFn, // Allow override of queryFn for dummy mode
    ...queryOptions
  } = options;

  const queryClient = useQueryClient();

  return useQuery({
    queryKey: ['table-validations', tableName],
    // Use provided queryFn or default to API call
    queryFn: queryFn || (async () => {
      console.log(`Fetching validations for table: ${tableName}`);

      // Get the validation rules
      const response = await validationsAPI.getRules(tableName);

      // Process the response to handle different formats
      let rules = [];

      if (response?.data?.rules) {
        rules = response.data.rules;
      } else if (response?.rules) {
        rules = response.rules;
      } else if (Array.isArray(response)) {
        rules = response;
      } else if (response?.results && Array.isArray(response.results)) {
        rules = response.results;
      }

      // Check if we have any validation history to load
      if (rules.length > 0) {
        try {
          // For the initial load, try to run validations to get latest results
          // This will make sure we have current results on page load

          const connectionId = queryClient.getQueryData(['current-connection-id']);

          if (connectionId) {
            console.log("Loading latest validation results on initial mount");
            const resultsResponse = await validationsAPI.runValidations(
              connectionId,
              tableName,
              null
            );

            if (resultsResponse?.results && Array.isArray(resultsResponse.results)) {
              // Create a map of results by rule_name
              const resultsByRuleName = {};
              resultsResponse.results.forEach(result => {
                if (result.rule_name) {
                  resultsByRuleName[result.rule_name] = result;
                }
              });

              // Merge the results with the rules
              rules = rules.map(rule => {
                const result = resultsByRuleName[rule.rule_name];
                if (result) {
                  return {
                    ...rule,
                    last_result: result.error ? null : result.is_valid,
                    actual_value: result.actual_value,
                    error: result.error,
                    last_run_at: new Date().toISOString()
                  };
                }
                return rule;
              });
            }
          }
        } catch (error) {
          console.log("Error loading initial validation results:", error);
          // Continue with the rules we have
        }
      }

      return rules;
    }),
    enabled,
    refetchInterval,
    ...queryOptions,
    select: (data) => {
      console.log("Validations data processed:", data);
      return data;
    },

    // Useful settings for validation data
    refetchOnWindowFocus: false,
    cacheTime: 10 * 60 * 1000, // 10 minutes
    staleTime: 5 * 60 * 1000,  // 5 minutes
  });
};

/**
 * Custom hook to run validations
 * @param {string} connectionId - The connection ID
 * @param {string} tableName - The table name
 * @returns {Object} Mutation result object
 */
export const useRunValidations = (connectionId, tableName) => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: () =>
      validationsAPI.runValidations(connectionId, tableName, null),
    onSuccess: () => {
      // Invalidate validations queries to trigger a refetch
      queryClient.invalidateQueries(['table-validations', tableName]);
      queryClient.invalidateQueries(['validations-summary', connectionId]);
    }
  });
};

/**
 * Custom hook to generate default validations
 * @param {string} connectionId - The connection ID
 * @param {string} tableName - The table name
 * @returns {Object} Mutation result object
 */
export const useGenerateValidations = (connectionId, tableName) => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: () =>
      validationsAPI.generateDefaultValidations(connectionId, tableName, null),
    onSuccess: () => {
      // Invalidate validations queries to trigger a refetch
      queryClient.invalidateQueries(['table-validations', tableName]);
      queryClient.invalidateQueries(['validations-summary', connectionId]);
    }
  });
};

/**
 * Custom hook to create, update or delete validation rules
 * @param {string} tableName - The table name
 * @returns {Object} Mutation functions for CRUD operations
 */
export const useValidationRuleMutations = (tableName) => {
  const queryClient = useQueryClient();

  // Create validation rule
  const createRule = useMutation({
    mutationFn: (rule) => validationsAPI.createRule(tableName, rule),
    onSuccess: () => {
      queryClient.invalidateQueries(['table-validations', tableName]);
    }
  });

  // Update validation rule
  const updateRule = useMutation({
    mutationFn: ({ ruleId, rule }) => validationsAPI.updateRule(ruleId, tableName, rule),
    onSuccess: () => {
      queryClient.invalidateQueries(['table-validations', tableName]);
    }
  });

  // Delete validation rule
  const deleteRule = useMutation({
    mutationFn: (ruleName) => validationsAPI.deleteRule(tableName, ruleName),
    onSuccess: () => {
      queryClient.invalidateQueries(['table-validations', tableName]);
    }
  });

  return {
    createRule,
    updateRule,
    deleteRule
  };
};