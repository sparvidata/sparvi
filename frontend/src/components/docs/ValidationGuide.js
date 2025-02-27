// frontend/src/components/docs/ValidationGuide.js
import React from 'react';

function ValidationGuide() {
  return (
    <div className="documentation-page">
      <div className="alert alert-primary mb-4">
        <i className="bi bi-info-circle-fill me-2"></i>
        <strong>Quick Start:</strong> To add validation rules automatically, go to the Validations tab and click "Generate Default Rules".
      </div>

      <h2 id="introduction">Default Validations in Sparvi</h2>
      <p>
        Sparvi provides a set of default validation rules that can be applied to any table.
        These rules help you quickly establish baseline data quality checks without having to write them from scratch.
      </p>

      <h3 id="using-default-validations">Using Default Validations</h3>
      <ol>
        <li>Navigate to the "Validations" tab in the Sparvi dashboard</li>
        <li>Click the "Generate Default Rules" button</li>
        <li>Sparvi will analyze your table structure and create appropriate validation rules</li>
        <li>You can then run these rules to check your data quality</li>
      </ol>

      <h3 id="types-of-validations">Types of Default Validations</h3>
      <p>
        Sparvi automatically generates the following types of validation rules based on your table structure:
      </p>

      <h4 id="basic-table-validations">Basic Table Validations</h4>
      <div className="table-responsive mb-4">
        <table className="table table-bordered">
          <thead className="table-light">
            <tr>
              <th>Validation</th>
              <th>Description</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><strong>Empty table check</strong></td>
              <td>Ensures your table has at least one row</td>
            </tr>
            <tr>
              <td><strong>Primary key uniqueness</strong></td>
              <td>Verifies that primary keys have no duplicates</td>
            </tr>
            <tr>
              <td><strong>Reference table size</strong></td>
              <td>For tables that appear to be lookup/reference tables (based on naming), checks that they have a reasonable number of rows</td>
            </tr>
          </tbody>
        </table>
      </div>

      <h4 id="column-level-validations">Column-Level Validations</h4>
      <div className="table-responsive mb-4">
        <table className="table table-bordered">
          <thead className="table-light">
            <tr>
              <th>Validation</th>
              <th>Description</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><strong>NULL value checks</strong></td>
              <td>For columns marked as NOT NULL in the schema, verifies there are no NULL values</td>
            </tr>
            <tr>
              <td><strong>Empty string checks</strong></td>
              <td>For required string columns, ensures there are no empty strings</td>
            </tr>
            <tr>
              <td><strong>Negative value checks</strong></td>
              <td>For numeric columns, checks for negative values (excluding columns where negative values make sense)</td>
            </tr>
            <tr>
              <td><strong>Future date checks</strong></td>
              <td>For date/timestamp columns, verifies there are no future dates where inappropriate</td>
            </tr>
            <tr>
              <td><strong>Max length checks</strong></td>
              <td>For varchar columns, ensures values don't exceed the defined maximum length</td>
            </tr>
            <tr>
              <td><strong>Outlier detection</strong></td>
              <td>For numeric columns, identifies extreme outliers (> 3 standard deviations from the mean)</td>
            </tr>
          </tbody>
        </table>
      </div>

      <h3 id="how-default-validations-work">How Default Validations Work</h3>
      <div className="mb-4">
        <ol>
          <li>
            <strong>Schema Analysis</strong>: Sparvi analyzes your table schema to understand column data types,
            constraints, and naming patterns
          </li>
          <li>
            <strong>Rule Generation</strong>: Based on this analysis, it creates tailored SQL queries to validate
            your data
          </li>
          <li>
            <strong>Smart Defaults</strong>: Some rules are only applied where appropriate (e.g., negative value
            checks don't apply to columns likely to have negative values like "balance")
          </li>
          <li>
            <strong>Performance Optimization</strong>: Rules are designed to be efficient and not impact database
            performance significantly
          </li>
        </ol>
      </div>

      <h3 id="customizing-validations">Customizing Default Validations</h3>
      <p>
        After generating default validations, you can:
      </p>
      <ul>
        <li>Delete any rules that don't make sense for your specific use case</li>
        <li>Modify rules by creating new ones with adjusted parameters</li>
        <li>Add additional custom rules to supplement the defaults</li>
      </ul>

      <h3 id="example-queries">Examples</h3>
      <p>
        Here are some examples of the SQL queries generated by default validations:
      </p>

      <div className="card mb-3">
        <div className="card-header">
          Check for NULL values in a required column
        </div>
        <div className="card-body bg-light">
          <pre className="mb-0">
            <code>{`SELECT COUNT(*) FROM employees WHERE last_name IS NULL`}</code>
          </pre>
        </div>
      </div>

      <div className="card mb-3">
        <div className="card-header">
          Check for negative values in a numeric column
        </div>
        <div className="card-body bg-light">
          <pre className="mb-0">
            <code>{`SELECT COUNT(*) FROM orders WHERE quantity < 0`}</code>
          </pre>
        </div>
      </div>

      <div className="card mb-3">
        <div className="card-header">
          Check for future dates in a creation timestamp
        </div>
        <div className="card-body bg-light">
          <pre className="mb-0">
            <code>{`SELECT COUNT(*) FROM users WHERE created_at > CURRENT_DATE`}</code>
          </pre>
        </div>
      </div>

      <div className="card mb-3">
        <div className="card-header">
          Check for outliers in a numeric column
        </div>
        <div className="card-body bg-light">
          <pre className="mb-0">
            <code>{`WITH stats AS (
    SELECT 
        AVG(salary) as avg_val,
        STDDEV(salary) as stddev_val
    FROM employees
    WHERE salary IS NOT NULL
)
SELECT COUNT(*) FROM employees, stats
WHERE salary > stats.avg_val + 3 * stats.stddev_val
   OR salary < stats.avg_val - 3 * stats.stddev_val`}</code>
          </pre>
        </div>
      </div>

      <h3 id="best-practices">Best Practices</h3>
      <div className="alert alert-success mb-4">
        <h5><i className="bi bi-lightbulb me-2"></i>Recommended Approach</h5>
        <ul className="mb-0">
          <li>Generate default validations when you first connect to a table to establish a baseline</li>
          <li>Review the generated rules to ensure they match your business expectations</li>
          <li>Supplement default validations with custom rules for business-specific requirements</li>
          <li>Re-run validations regularly to monitor data quality over time</li>
          <li>Set up alerts for validation failures to catch data issues early</li>
        </ul>
      </div>

      <h3 id="troubleshooting">Troubleshooting</h3>
      <p>
        If you encounter issues with validation rules:
      </p>
      <ul>
        <li>
          <strong>Rule fails to execute</strong>: Ensure your SQL query is compatible with your database dialect.
          Some functions like <code>STDDEV</code> may have different names in different databases.
        </li>
        <li>
          <strong>Too many false positives</strong>: Adjust the threshold or expected value, or delete the rule if it's
          not applicable to your data model.
        </li>
        <li>
          <strong>Rules are too strict</strong>: For outlier detection, you might want to increase the standard deviation
          threshold from 3 to a higher value.
        </li>
        <li>
          <strong>Database performance concerns</strong>: Schedule validation runs during off-peak hours if they impact
          performance.
        </li>
      </ul>

      <hr className="my-5" />

      <h3 id="custom-validations">Creating Custom Validations</h3>
      <p>
        While default validations cover many common scenarios, you'll often need to create custom validations specific to your business rules.
        Here are some examples of custom validations you might want to add:
      </p>

      <div className="table-responsive mb-4">
        <table className="table table-bordered">
          <thead className="table-light">
            <tr>
              <th>Validation Type</th>
              <th>Example Query</th>
              <th>Description</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Referential Integrity</td>
              <td>
                <code>{`SELECT COUNT(*) FROM orders WHERE customer_id NOT IN (SELECT id FROM customers)`}</code>
              </td>
              <td>Ensures all foreign key values exist in the parent table, even if not enforced by the database</td>
            </tr>
            <tr>
              <td>Value Distribution</td>
              <td>
                <code>{`SELECT (COUNT(*) FILTER(WHERE status = 'completed') * 100.0 / COUNT(*)) FROM orders`}</code>
              </td>
              <td>Checks if the percentage of completed orders is within expected range</td>
            </tr>
            <tr>
              <td>Data Freshness</td>
              <td>
                <code>{`SELECT COUNT(*) FROM transactions WHERE created_at < CURRENT_DATE - INTERVAL '7 days'`}</code>
              </td>
              <td>Verifies that data has been updated recently</td>
            </tr>
            <tr>
              <td>Pattern Matching</td>
              <td>
                <code>{`SELECT COUNT(*) FROM users WHERE email NOT LIKE '%@%.%'`}</code>
              </td>
              <td>Ensures email addresses follow a basic pattern</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="mb-5">
        <a href="/dashboard" className="btn btn-primary">
          <i className="bi bi-arrow-left me-2"></i>
          Back to Dashboard
        </a>
      </div>
    </div>
  );
}

export default ValidationGuide;