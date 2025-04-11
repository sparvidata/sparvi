import { validationsAPI } from '../api/enhancedApiService';

class ValidationService {
  // Get validation rules with results
  async getValidations(connectionId, tableName) {
    if (!connectionId || !tableName) {
      throw new Error('Connection ID and table name are required');
    }

    try {
      // Get validation rules
      const rules = await this.getRules(connectionId, tableName);

      // Then get the latest results for these rules
      const results = await this.getLatestResults(connectionId, tableName);

      // Merge rules with results
      return this.mergeRulesWithResults(rules, results);
    } catch (error) {
      console.error('Error fetching validations:', error);
      throw error;
    }
  }

  // Get validation rules
  async getRules(connectionId, tableName) {
    try {
      const response = await validationsAPI.getRules(tableName, { connectionId });

      // Handle different API response formats
      let rules = [];
      if (Array.isArray(response)) {
        rules = response;
      } else if (response?.rules) {
        rules = response.rules;
      } else if (response?.data?.rules) {
        rules = response.data.rules;
      }

      // Always filter out inactive rules
      rules = rules.filter(rule => rule.is_active !== false);

      return rules;
    } catch (error) {
      console.error('Error getting validation rules:', error);
      throw error;
    }
  }

  // Get latest validation results
  async getLatestResults(connectionId, tableName) {
    try {
      console.log(`Getting latest validation results for ${tableName}...`);
      const response = await validationsAPI.getLatestValidationResults(
        connectionId,
        tableName,
        { forceFresh: true } // Add forceFresh option to bypass cache
      );

      // Handle different API response formats
      if (response?.results) {
        return response.results;
      } else if (Array.isArray(response)) {
        return response;
      } else if (response?.data?.results) {
        return response.data.results;
      }

      return [];
    } catch (error) {
      console.error('Error getting latest validation results:', error);
      throw error;
    }
  }

  // Run all validations
  async runValidations(connectionId, tableName, connectionString = null) {
    try {
      console.log(`Running validations for connection ${connectionId}, table ${tableName}`);
      const response = await validationsAPI.runValidations(
        connectionId,
        tableName,
        connectionString  // Pass the connectionString parameter
      );

      // Extract results based on response format and normalize
      let results = [];
      if (response?.results) {
        results = response.results;
      } else if (Array.isArray(response)) {
        results = response;
      } else if (response?.data?.results) {
        results = response.data.results;
      }

      console.log(`Received ${results.length} validation results`);

      // Process the results to ensure they have consistent format
      const processedResults = results.map(result => ({
        rule_name: result.rule_name,
        is_valid: result.is_valid,
        actual_value: result.actual_value,
        expected_value: result.expected_value,
        error: result.error,
        run_at: result.run_at || new Date().toISOString(),
        execution_time_ms: result.execution_time_ms
      }));

      return processedResults;
    } catch (error) {
      console.error('Error running validations:', error);
      throw error;
    }
  }

  // Run a single validation
  async runSingleValidation(connectionId, tableName, validationRule) {
    try {
      // Run all validations (API doesn't support single rule execution)
      const results = await this.runValidations(connectionId, tableName);
      
      // Find the result for this specific rule
      return results.find(r => r.rule_name === validationRule.rule_name);
    } catch (error) {
      console.error('Error running single validation:', error);
      throw error;
    }
  }

  // Deactivate a validation rule
  async deactivateRule(connectionId, tableName, ruleName) {
    if (!connectionId || !tableName || !ruleName) {
      throw new Error('Missing required parameters for deactivating rule');
    }

    console.log(`Attempting to deactivate rule "${ruleName}" for table "${tableName}"`);

    try {
      const response = await validationsAPI.deactivateRule(
        tableName,
        ruleName,
        connectionId
      );

      console.log("Deactivation API response:", response);
      return response;
    } catch (error) {
      console.error('Error deactivating rule:', error);
      throw error;
    }
  }

  // Generate default validations
  async generateDefaultValidations(connectionId, tableName) {
    try {
      return await validationsAPI.generateDefaultValidations(
        connectionId,
        tableName
      );
    } catch (error) {
      console.error('Error generating default validations:', error);
      throw error;
    }
  }

  // Helper to merge rules with their results
  mergeRulesWithResults(rules, results) {
    if (!rules || !results) return rules;
    
    const resultsMap = {};
    results.forEach(result => {
      if (result.rule_name) {
        resultsMap[result.rule_name] = result;
      }
    });
    
    return rules.map(rule => {
      const result = resultsMap[rule.rule_name];
      if (result) {
        return {
          ...rule,
          last_result: result.error ? null : result.is_valid,
          actual_value: result.actual_value,
          error: result.error,
          last_run_at: result.run_at || new Date().toISOString(),
          execution_time_ms: result.execution_time_ms
        };
      }
      return rule;
    });
  }

  // Calculate validation metrics
  calculateMetrics(validations) {
    const total = validations.length;
    const passed = validations.filter(v => v.last_result === true).length;
    const failed = validations.filter(v => v.last_result === false).length;
    const errored = validations.filter(v => v.error).length;
    const notRun = validations.filter(v => v.last_result === undefined || v.last_result === null).length;
    
    // Calculate health score (only for rules that have been run)
    const runCount = passed + failed;
    const healthScore = runCount > 0 ? Math.round((passed / runCount) * 100) : 0;
    
    return {
      total,
      passed,
      failed,
      errored,
      notRun,
      healthScore,
      lastRunAt: this.getLastRunTimestamp(validations)
    };
  }

  // Get the most recent run timestamp
  getLastRunTimestamp(validations) {
    const timestamps = validations
      .filter(v => v.last_run_at)
      .map(v => new Date(v.last_run_at).getTime());
    
    return timestamps.length > 0 ? new Date(Math.max(...timestamps)) : null;
  }

  // Process validation history into trend data
  processTrendData(history, days = 30) {
    if (!history || !history.length) return [];
    
    // Group by date
    const byDate = {};
    
    history.forEach(item => {
      const date = item.run_at ? new Date(item.run_at).toISOString().split('T')[0] : null;
      if (!date) return;
      
      if (!byDate[date]) {
        byDate[date] = {
          date,
          total: 0,
          passed: 0,
          failed: 0,
          error: 0,
          health_score: 0
        };
      }
      
      byDate[date].total++;
      
      if (item.error) {
        byDate[date].error++;
      } else if (item.is_valid === true) {
        byDate[date].passed++;
      } else if (item.is_valid === false) {
        byDate[date].failed++;
      }
    });
    
    // Calculate health scores and convert to array
    return Object.values(byDate)
      .map(day => {
        const validResults = day.passed + day.failed;
        if (validResults > 0) {
          day.health_score = Math.round((day.passed / validResults) * 100);
        }
        return day;
      })
      .sort((a, b) => a.date.localeCompare(b.date));
  }
}

export default new ValidationService();