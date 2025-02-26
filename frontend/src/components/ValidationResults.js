import React, { useState } from 'react';

function ValidationResults({ results = [] }) {
  const [newRule, setNewRule] = useState({
    name: '',
    description: '',
    query: '',
    operator: 'equals',
    expected_value: ''
  });

  // Display existing validation results
  const renderValidationResults = () => {
    if (results.length === 0) {
      return (
        <div className="alert alert-info">
          <i className="bi bi-info-circle-fill me-2"></i>
          No validation rules have been run yet.
        </div>
      );
    }

    return (
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
          <form>
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
                  placeholder="e.g., 0 or [0, 10] for between"
                />
                <div className="form-text">
                  For 'between' operator, use format: [min, max]
                </div>
              </div>
            </div>

            <div className="d-flex justify-content-end">
              <button type="button" className="btn btn-primary">
                <i className="bi bi-plus-circle me-1"></i>
                Add Rule
              </button>
            </div>
          </form>
        </div>
      </div>
    );
  };

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header">
        <h5 className="mb-0">Validation Rules</h5>
      </div>
      <div className="card-body">
        {renderValidationResults()}

        <div className="d-grid gap-2 d-md-flex justify-content-md-end mt-4">
          <button className="btn btn-success me-md-2">
            <i className="bi bi-play-fill me-1"></i>
            Run Validations
          </button>
        </div>

        <hr />

        {renderNewRuleForm()}
      </div>
    </div>
  );
}

export default ValidationResults;