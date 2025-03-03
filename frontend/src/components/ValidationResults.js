import React, { useState, useEffect } from 'react';
import {
  fetchValidations,
  addValidationRule,
  deleteValidationRule,
  runValidations,
  generateDefaultValidations
} from '../api';
import AuthHandler from "../auth/AuthHandler";

function ValidationResults({ tableName, connectionString }) {
  const [rules, setRules] = useState([]);
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [showGeneratingSpinner, setShowGeneratingSpinner] = useState(false);
  const [error, setError] = useState(null);
  const [successMessage, setSuccessMessage] = useState(null);
  const [showGuideModal, setShowGuideModal] = useState(false);

  const [newRule, setNewRule] = useState({
    name: '',
    description: '',
    query: '',
    operator: 'equals',
    expected_value: ''
  });

  const token = localStorage.getItem("token");

  // Function to toggle the guide modal
  const toggleGuideModal = () => setShowGuideModal(!showGuideModal);

  // Load existing validation rules when component mounts
  useEffect(() => {
    const loadRules = async () => {
      if (!tableName) {
        console.log("No table name provided, skipping rules loading");
        return;
      }

      try {
        setLoading(true);
        console.log("Fetching validation rules for table:", tableName);
        const response = await fetchValidations(tableName);
        console.log("Validation rules response:", response);
        setRules(response.rules || []);
        console.log("Set rules state:", response.rules);
      } catch (err) {
        console.error("Error loading validation rules:", err);
        setError("Failed to load validation rules");
      } finally {
        setLoading(false);
      }
    };

    loadRules();
  }, [tableName]);

  // Handle form submission to add a new rule
  const handleAddRule = async (e) => {
    e.preventDefault();

    // Basic validation
    if (!newRule.name || !newRule.query || !newRule.expected_value) {
      setError("Please fill in all required fields");
      return;
    }

    // Parse expected value based on operator
    let parsedExpectedValue;
    try {
      if (newRule.operator === 'between') {
        // Expected format for 'between' is [min, max]
        parsedExpectedValue = JSON.parse(newRule.expected_value);
        if (!Array.isArray(parsedExpectedValue) || parsedExpectedValue.length !== 2) {
          setError("For 'between' operator, expected value should be in format [min, max]");
          return;
        }
      } else if (newRule.operator === 'equals') {
        // Try to parse as JSON, but keep as string if that fails
        try {
          parsedExpectedValue = JSON.parse(newRule.expected_value);
        } catch {
          parsedExpectedValue = newRule.expected_value;
        }
      } else {
        // For greater_than and less_than, convert to number
        parsedExpectedValue = Number(newRule.expected_value);
        if (isNaN(parsedExpectedValue)) {
          setError("Expected value must be a number for this operator");
          return;
        }
      }
    } catch (err) {
      setError(`Invalid expected value format: ${err.message}`);
      return;
    }

    try {
      setLoading(true);
      setError(null);

      console.log("Adding validation rule with:");
      console.log("Table name:", tableName);
      console.log("Rule data:", newRule);

      const ruleToAdd = {
        ...newRule,
        expected_value: parsedExpectedValue
      };

      await addValidationRule(tableName, ruleToAdd);

      // Refresh rules list
      const response = await fetchValidations(tableName);
      setRules(response.rules || []);

      // Reset form
      setNewRule({
        name: '',
        description: '',
        query: '',
        operator: 'equals',
        expected_value: ''
      });

      setSuccessMessage("Rule added successfully");
      setTimeout(() => setSuccessMessage(null), 3000);
    } catch (err) {
      console.error("Error adding validation rule:", err);
      setError(err.response?.data?.error || "Failed to add validation rule");
    } finally {
      setLoading(false);
    }
  };

  // Handle rule deletion
  const handleDeleteRule = async (ruleName) => {
    if (!window.confirm(`Are you sure you want to delete the rule "${ruleName}"?`)) {
      return;
    }

    try {
      setLoading(true);
      await deleteValidationRule(tableName, ruleName);

      // Refresh rules list
      const response = await fetchValidations(tableName);
      setRules(response.rules || []);

      setSuccessMessage("Rule deleted successfully");
      setTimeout(() => setSuccessMessage(null), 3000);
    } catch (err) {
      console.error("Error deleting validation rule:", err);
      setError(err.response?.data?.error || "Failed to delete validation rule");
    } finally {
      setLoading(false);
    }
  };

  const handleRunValidations = async () => {
    if (!connectionString || !tableName) {
      setError("Connection string and table name are required to run validations");
      return;
    }

    try {
      setLoading(true);
      setError(null);
      console.log("Running validations with:", { connectionString, tableName });

      const response = await runValidations(connectionString, tableName);
      console.log("Validation run response:", response);

      // Check if we got results back
      if (response.results) {
        console.log(`Received ${response.results.length} validation results`);
        setResults(response.results);

        // If you have a visualization component for results, make sure it's updating
        console.log("Updated state with new results");
      } else {
        console.warn("No results returned from validation run");
        setResults([]);
      }

      setSuccessMessage("Validations completed");
      setTimeout(() => setSuccessMessage(null), 3000);
    } catch (err) {
      console.error("Error running validations:", err);
      console.error("Error details:", err.response?.data || err.message);
      setError(err.response?.data?.error || "Failed to run validations");
    } finally {
      setLoading(false);
    }
  };

// Generate default validation rules
const handleGenerateDefaults = async () => {
  if (!connectionString || !tableName) {
    setError("Connection string and table name are required");
    console.error("Missing required parameters:", { connectionString, tableName });
    return;
  }

  try {
    setShowGeneratingSpinner(true);
    console.log("Calling generateDefaultValidations with:", {
      connectionString,
      tableName
    });

    // Use the API function directly instead of making a direct fetch call
    const response = await generateDefaultValidations(connectionString, tableName);

    console.log("Default validations response:", response);

    // Refresh rules list
    const rulesResponse = await fetchValidations(tableName);
    setRules(rulesResponse.rules || []);

    setSuccessMessage(response.message || `Added ${response.count} default validation rules`);
    setTimeout(() => setSuccessMessage(null), 3000);
  } catch (err) {
    console.error("Error generating default validations:", err);
    setError(err.message || "Failed to generate default validations");
  } finally {
    setLoading(false);
    setShowGeneratingSpinner(false);
  }
};

  // Display existing validation results
  const renderValidationResults = () => {
    console.log("Rendering validation results:", results);
    if (loading && !showGeneratingSpinner && !results.length) {
      return (
        <div className="d-flex justify-content-center my-3">
          <div className="spinner-border text-primary" role="status">
            <span className="visually-hidden">Loading...</span>
          </div>
        </div>
      );
    }

    if (error) {
      return (
        <div className="alert alert-danger">
          <i className="bi bi-exclamation-triangle-fill me-2"></i>
          {error}
        </div>
      );
    }

    if (successMessage) {
      return (
        <div className="alert alert-success">
          <i className="bi bi-check-circle-fill me-2"></i>
          {successMessage}
        </div>
      );
    }

    if (results.length === 0 && rules.length === 0) {
      return (
        <div className="alert alert-info">
          <i className="bi bi-info-circle-fill me-2"></i>
          No validation rules have been created yet. Add a rule below or click "Generate Default Rules" to create rules automatically.
        </div>
      );
    }

    if (results.length === 0 && rules.length > 0) {
      return (
        <div>
          <div className="alert alert-info">
            <i className="bi bi-info-circle-fill me-2"></i>
            {rules.length} rules defined. Click "Run Validations" to check your data quality.
          </div>

          <div className="table-responsive mb-3">
            <table className="table table-hover">
              <thead>
                <tr>
                  <th>Rule Name</th>
                  <th>Description</th>
                  <th>Query</th>
                  <th>Condition</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {rules.map((rule) => (
                  <tr key={rule.rule_name}>
                    <td>{rule.rule_name}</td>
                    <td>{rule.description}</td>
                    <td><code className="small">{rule.query}</code></td>
                    <td>
                      <span className="badge bg-secondary">
                        {rule.operator} {JSON.stringify(rule.expected_value)}
                      </span>
                    </td>
                    <td>
                      <button
                        className="btn btn-sm btn-outline-danger"
                        onClick={() => handleDeleteRule(rule.rule_name)}
                      >
                        <i className="bi bi-trash"></i>
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      );
    }

    return (
      <div>
        <div className="table-responsive">
          <table className="table table-striped">
            <thead>
              <tr>
                <th>Rule</th>
                <th>Status</th>
                <th>Expected</th>
                <th>Actual</th>
                <th>Description</th>
              </tr>
            </thead>
            <tbody>
              {results.map((result, idx) => (
                <tr key={idx}>
                  <td>{result.rule_name}</td>
                  <td>
                    {result.is_valid ? (
                      <span className="badge bg-success">PASS</span>
                    ) : (
                      <span className="badge bg-danger">FAIL</span>
                    )}
                  </td>
                  <td><code>{JSON.stringify(result.expected_value)}</code></td>
                  <td><code>{JSON.stringify(result.actual_value)}</code></td>
                  <td>{result.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <h6 className="mt-4">All Rules ({rules.length})</h6>
        <div className="table-responsive mb-3">
          <table className="table table-sm table-hover">
            <thead>
              <tr>
                <th>Rule Name</th>
                <th>Description</th>
                <th>Query</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {rules.map((rule) => (
                <tr key={rule.rule_name}>
                  <td>{rule.rule_name}</td>
                  <td>{rule.description}</td>
                  <td><code className="small">{rule.query}</code></td>
                  <td>
                    <button
                      className="btn btn-sm btn-outline-danger"
                      onClick={() => handleDeleteRule(rule.rule_name)}
                    >
                      <i className="bi bi-trash"></i>
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  // Form for adding new validation rules
  const renderNewRuleForm = () => {
    return (
      <div className="card mb-4">
        <div className="card-header">
          <h6 className="mb-0">Add New Validation Rule</h6>
        </div>
        <div className="card-body">
          <form onSubmit={handleAddRule}>
            <div className="row">
              <div className="col-md-6 mb-3">
                <label htmlFor="ruleName" className="form-label">Rule Name</label>
                <input
                  type="text"
                  className="form-control"
                  id="ruleName"
                  value={newRule.name}
                  onChange={(e) => setNewRule({...newRule, name: e.target.value})}
                  placeholder="e.g., check_positive_salaries"
                  required
                />
              </div>

              <div className="col-md-6 mb-3">
                <label htmlFor="ruleDescription" className="form-label">Description</label>
                <input
                  type="text"
                  className="form-control"
                  id="ruleDescription"
                  value={newRule.description}
                  onChange={(e) => setNewRule({...newRule, description: e.target.value})}
                  placeholder="e.g., Ensure all salaries are positive"
                />
              </div>
            </div>

            <div className="mb-3">
              <label htmlFor="ruleQuery" className="form-label">SQL Query</label>
              <textarea
                className="form-control font-monospace"
                id="ruleQuery"
                rows="3"
                value={newRule.query}
                onChange={(e) => setNewRule({...newRule, query: e.target.value})}
                placeholder="e.g., SELECT COUNT(*) FROM employees WHERE salary <= 0"
                required
              ></textarea>
              <div className="form-text">
                The query should return a single value that will be compared with the expected value.
              </div>
            </div>

            <div className="row">
              <div className="col-md-6 mb-3">
                <label htmlFor="ruleOperator" className="form-label">Operator</label>
                <select
                  className="form-select"
                  id="ruleOperator"
                  value={newRule.operator}
                  onChange={(e) => setNewRule({...newRule, operator: e.target.value})}
                  required
                >
                  <option value="equals">Equals (=)</option>
                  <option value="greater_than">Greater Than (&gt;)</option>
                  <option value="less_than">Less Than (&lt;)</option>
                  <option value="between">Between</option>
                </select>
              </div>

              <div className="col-md-6 mb-3">
                <label htmlFor="ruleExpectedValue" className="form-label">Expected Value</label>
                <input
                  type="text"
                  className="form-control"
                  id="ruleExpectedValue"
                  value={newRule.expected_value}
                  onChange={(e) => setNewRule({...newRule, expected_value: e.target.value})}
                  placeholder={newRule.operator === 'between' ? '[min, max]' : 'e.g., 0'}
                  required
                />
                <div className="form-text">
                  {newRule.operator === 'between'
                    ? 'For between operator, use JSON array format: [min, max]'
                    : newRule.operator === 'equals'
                      ? 'For boolean values, use true or false (lowercase)'
                      : 'Enter a numeric value'}
                </div>
              </div>
            </div>

            <div className="d-flex justify-content-end">
              <button
                type="submit"
                className="btn btn-primary"
                disabled={loading}
              >
                {loading ? (
                  <>
                    <span className="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
                    Adding...
                  </>
                ) : (
                  <>
                    <i className="bi bi-plus-circle me-1"></i>
                    Add Rule
                  </>
                )}
              </button>
            </div>
          </form>
        </div>
      </div>
    );
  };

  // Render action buttons for running validations and generating defaults
  const renderActionButtons = () => {
    return (
      <div className="d-grid gap-2 d-md-flex justify-content-md-end mt-4">
        <button
          className="btn btn-outline-info me-md-2"
          onClick={toggleGuideModal}
          aria-label="Show validations guide"
        >
          <i className="bi bi-question-circle me-1"></i>
          Guide
        </button>
        <button
          className="btn btn-outline-primary me-md-2"
          onClick={handleGenerateDefaults}
          disabled={loading}
        >
          {loading && showGeneratingSpinner ? (
            <>
              <span className="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
              Generating...
            </>
          ) : (
            <>
              <i className="bi bi-magic me-1"></i>
              Generate Default Rules
            </>
          )}
        </button>
        <button
          className="btn btn-success"
          onClick={handleRunValidations}
          disabled={loading || rules.length === 0}
        >
          {loading && !showGeneratingSpinner ? (
            <>
              <span className="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
              Running...
            </>
          ) : (
            <>
              <i className="bi bi-play-fill me-1"></i>
              Run Validations
            </>
          )}
        </button>
      </div>
    );
  };

  // Render the validation guide modal
  const renderValidationGuideModal = () => {
    return (
      <>
        <div className={`modal fade ${showGuideModal ? 'show d-block' : 'd-none'}`} tabIndex="-1" role="dialog">
          <div className="modal-dialog modal-lg">
            <div className="modal-content">
              <div className="modal-header">
                <h5 className="modal-title">Default Validations Guide</h5>
                <button type="button" className="btn-close" aria-label="Close" onClick={toggleGuideModal}></button>
              </div>
              <div className="modal-body">
                <h3>What are Default Validations?</h3>
                <p>Default validations are pre-configured data quality checks that are automatically generated based on
                  your table structure.</p>

                <h4>Types of Default Validations</h4>
                <div className="mb-4">
                  <h5>Basic Table Validations</h5>
                  <ul>
                    <li><strong>Empty table check</strong> - Ensures your table has at least one row</li>
                    <li><strong>Primary key uniqueness</strong> - Verifies that primary keys have no duplicates</li>
                    <li><strong>Reference table size</strong> - For lookup tables, checks they have a reasonable number
                      of rows
                    </li>
                  </ul>
                </div>

                <div className="mb-4">
                  <h5>Column-Level Validations</h5>
                  <ul>
                    <li><strong>NULL value checks</strong> - For required columns, verifies there are no NULL values
                    </li>
                    <li><strong>Empty string checks</strong> - For required string columns, ensures no empty strings
                    </li>
                    <li><strong>Negative value checks</strong> - For numeric columns, checks for negative values</li>
                    <li><strong>Future date checks</strong> - Verifies there are no future dates where inappropriate
                    </li>
                    <li><strong>Max length checks</strong> - For varchar columns, ensures values don't exceed defined
                      max length
                    </li>
                  </ul>
                </div>

                <div className="mb-4">
                  <h5>Statistical Validations</h5>
                  <ul>
                    <li><strong>Outlier detection</strong> - Identifies extreme outliers (> 3 standard deviations)</li>
                  </ul>
                </div>

                <h4>Examples</h4>
                <div className="border p-3 mb-3 bg-light">
                  <h6>Check for NULL values in a required column</h6>
                  <pre className="mb-0"><code>SELECT COUNT(*) FROM employees WHERE last_name IS NULL</code></pre>
                </div>

                <div className="border p-3 mb-3 bg-light">
                  <h6>Check for negative values in a numeric column</h6>
                  <pre className="mb-0">
                    <code>{`SELECT COUNT(*) FROM orders WHERE quantity < 0`}</code>
                  </pre>
                </div>

                <h4>Best Practices</h4>
                <ul>
                  <li>Generate default validations when you first connect to a table</li>
                  <li>Review the generated rules to ensure they match your business expectations</li>
                  <li>Supplement with custom rules for business-specific requirements</li>
                  <li>Re-run validations regularly to monitor data quality</li>
                </ul>

                <div className="alert alert-info mt-3">
                  <i className="bi bi-info-circle-fill me-2"></i>
                  <strong>Want more details?</strong> Visit the <a href="/docs/validations" target="_blank">full
                  documentation</a> for comprehensive information about validation rules.
                </div>
              </div>
              <div className="modal-footer">
                <button type="button" className="btn btn-secondary" onClick={toggleGuideModal}>Close</button>
              </div>
            </div>
          </div>
        </div>
        {showGuideModal && <div className="modal-backdrop fade show" onClick={toggleGuideModal}></div>}
      </>
    );
  };

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header">
        <h5 className="mb-0">Validation Rules</h5>
      </div>
      <div className="card-body">
        {renderValidationResults()}

        {renderActionButtons()}

        <hr />

        {renderNewRuleForm()}
      </div>

      {/* Render the guide modal */}
      {renderValidationGuideModal()}
    </div>
  );
}

export default ValidationResults;