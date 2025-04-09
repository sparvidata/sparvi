import { validationsAPI } from '../api/enhancedApiService';
import { processValidationResults } from '../utils/validationResultsProcessor';

/**
 * Service for handling validation-related API calls with better error handling and data processing
 */
export const validationService = {
  /**
   * Get the latest validation results for a table
   * @param {string} connectionId - Connection ID
   * @param {string} tableName - Table name
   * @returns {Promise<Object>} Latest validation results
   */
  getLatestResults: async (connectionId, tableName) => {
    if (!connectionId || !tableName) {
      console.warn('Missing required parameters for getLatestResults');
      return { results: [], metrics: null };
    }

    try {
      console.log(`Fetching latest validation results for ${tableName} with connection ${connectionId}`);

      const response = await validationsAPI.getLatestValidationResults(
        connectionId,
        tableName,
        { forceFresh: true } // Force fresh data to avoid stale cache
      );

      // Extract the results array based on response format
      let resultsArray = [];
      if (response?.results && Array.isArray(response.results)) {
        resultsArray = response.results;
      } else if (Array.isArray(response)) {
        resultsArray = response;
      }

      console.log(`Received ${resultsArray.length} validation results`);

      // Process the results
      const processed = processValidationResults(resultsArray);

      return {
        results: resultsArray,
        processed: processed.processed,
        metrics: processed.metrics,
        tableName,
        connectionId
      };
    } catch (error) {
      console.error(`Error fetching validation results: ${error.message}`);
      throw error;
    }
  },

  /**
   * Run validations for a table
   * @param {string} connectionId - Connection ID
   * @param {string} tableName - Table name
   * @returns {Promise<Object>} Validation results
   */
  runValidations: async (connectionId, tableName) => {
    if (!connectionId || !tableName) {
      console.warn('Missing required parameters for runValidations');
      return { results: [], success: false };
    }

    try {
      console.log(`Running validations for ${tableName} with connection ${connectionId}`);

      const response = await validationsAPI.runValidations(
        connectionId,
        tableName,
        null,
        { timeout: 60000 } // 1 minute timeout
      );

      // Extract results array
      let resultsArray = [];
      if (response?.results && Array.isArray(response.results)) {
        resultsArray = response.results;
      } else if (Array.isArray(response)) {
        resultsArray = response;
      }

      console.log(`Received ${resultsArray.length} validation results from execution`);

      return {
        results: resultsArray,
        success: true
      };
    } catch (error) {
      console.error(`Error running validations: ${error.message}`);
      throw error;
    }
  }
};

export default validationService;