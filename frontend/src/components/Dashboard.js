import React, { useEffect, useState } from 'react';
import { fetchProfile } from '../api';
import TrendChart from './TrendChart';
import AnomalyList from './AnomalyList';
import SchemaShift from './SchemaShift';
import ValidationResults from './ValidationResults';
import AlertsPanel from './AlertsPanel';
import ConnectionForm from './ConnectionForm';
import { Tabs, Tab } from 'react-bootstrap';

function Dashboard() {
  const [profileData, setProfileData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [connectionString, setConnectionString] = useState(
    localStorage.getItem('connectionString') || "duckdb:///C:/Users/mhard/PycharmProjects/HawkDB/backend/my_database.duckdb"
  );
  const [tableName, setTableName] = useState(localStorage.getItem('tableName') || "employees");
  const [activeTab, setActiveTab] = useState('overview');

  const token = localStorage.getItem("token");

  useEffect(() => {
    const getData = async () => {
      try {
        setLoading(true);
        setError(null);
        console.log('DEBUG: Fetching profile data...');
        const data = await fetchProfile(token, connectionString, tableName);
        console.log('DEBUG: Received profile data:', data);
        setProfileData(data);

        // Save connection info to localStorage
        localStorage.setItem('connectionString', connectionString);
        localStorage.setItem('tableName', tableName);
      } catch (error) {
        console.error('DEBUG: Error fetching profile data:', error);
        setError(error.response?.data?.error || 'Failed to fetch profile data');
      } finally {
        setLoading(false);
      }
    };

    if (token && connectionString && tableName) {
      getData();
    } else {
      setLoading(false);
    }
  }, [token, connectionString, tableName]);

  const handleRefresh = () => {
    // Re-fetch data when the refresh button is clicked
    if (token && connectionString && tableName) {
      setProfileData(null);
      fetchProfile(token, connectionString, tableName)
        .then(data => setProfileData(data))
        .catch(err => setError(err.response?.data?.error || 'Failed to refresh data'));
    }
  };

  const handleConnectionSubmit = (newConnection, newTable) => {
    setConnectionString(newConnection);
    setTableName(newTable);
  };

  const handleLogout = () => {
    localStorage.removeItem("token");
    window.location.href = "/login";
  };

  return (
    <div className="container-fluid mt-3">
      <div className="d-flex justify-content-between align-items-center mb-4">
        <h2>
          <i className="bi bi-database-check me-2"></i>
          Sparvi Data Profiler
        </h2>
        <div>
          <button className="btn btn-outline-secondary me-2" onClick={handleRefresh}>
            <i className="bi bi-arrow-clockwise me-1"></i> Refresh
          </button>
          <button className="btn btn-outline-danger" onClick={handleLogout}>
            <i className="bi bi-box-arrow-right me-1"></i> Logout
          </button>
        </div>
      </div>

      <ConnectionForm
        initialConnection={connectionString}
        initialTable={tableName}
        onSubmit={handleConnectionSubmit}
      />

      {error && (
        <div className="alert alert-danger mt-3" role="alert">
          <i className="bi bi-exclamation-triangle-fill me-2"></i>
          {error}
        </div>
      )}

      {loading ? (
        <div className="d-flex justify-content-center mt-5">
          <div className="spinner-border text-primary" role="status">
            <span className="visually-hidden">Loading...</span>
          </div>
        </div>
      ) : profileData ? (
        <>
          {/* Profile Overview Card */}
          <div className="card mb-4 mt-4 shadow-sm">
            <div className="card-header bg-primary text-white">
              <h5 className="mb-0">Profile Overview: {profileData.table}</h5>
            </div>
            <div className="card-body">
              <div className="row">
                <div className="col-md-3">
                  <div className="border rounded p-3 text-center mb-3">
                    <h6>Row Count</h6>
                    <h3>{profileData.row_count.toLocaleString()}</h3>
                  </div>
                </div>
                <div className="col-md-3">
                  <div className="border rounded p-3 text-center mb-3">
                    <h6>Duplicate Rows</h6>
                    <h3>{profileData.duplicate_count ? profileData.duplicate_count.toLocaleString() : 0}</h3>
                  </div>
                </div>
                <div className="col-md-3">
                  <div className="border rounded p-3 text-center mb-3">
                    <h6>Columns</h6>
                    <h3>{Object.keys(profileData.completeness).length}</h3>
                  </div>
                </div>
                <div className="col-md-3">
                  <div className="border rounded p-3 text-center mb-3">
                    <h6>Last Updated</h6>
                    <p className="mb-0">{new Date(profileData.timestamp).toLocaleString()}</p>
                  </div>
                </div>
              </div>

              {/* Alert badges */}
              {profileData.anomalies && profileData.anomalies.length > 0 && (
                <div className="alert alert-warning mt-3">
                  <i className="bi bi-exclamation-triangle-fill me-2"></i>
                  <strong>{profileData.anomalies.length} anomalies detected</strong>
                </div>
              )}

              {profileData.schema_shifts && profileData.schema_shifts.length > 0 && (
                <div className="alert alert-danger mt-3">
                  <i className="bi bi-exclamation-octagon-fill me-2"></i>
                  <strong>{profileData.schema_shifts.length} schema shifts detected</strong>
                </div>
              )}
            </div>
          </div>

          {/* Tabs for different sections */}
          <Tabs
            activeKey={activeTab}
            onSelect={(k) => setActiveTab(k)}
            className="mb-4"
          >
            <Tab eventKey="overview" title="Data Overview">
              <div className="row">
                <div className="col-lg-6">
                  {/* Data Completeness */}
                  <div className="card mb-4 shadow-sm">
                    <div className="card-header">
                      <h5 className="mb-0">Data Completeness</h5>
                    </div>
                    <div className="card-body">
                      <div className="table-responsive">
                        <table className="table table-striped table-hover">
                          <thead>
                            <tr>
                              <th>Column</th>
                              <th>Nulls</th>
                              <th>Null %</th>
                              <th>Distinct Count</th>
                              <th>Distinct %</th>
                            </tr>
                          </thead>
                          <tbody>
                            {profileData.completeness && Object.entries(profileData.completeness).map(([col, metrics]) => (
                              <tr key={col}>
                                <td>{col}</td>
                                <td>{metrics.nulls}</td>
                                <td>{metrics.null_percentage}%</td>
                                <td>{metrics.distinct_count}</td>
                                <td>{metrics.distinct_percentage}%</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="col-lg-6">
                  {/* Frequent Values */}
                  <div className="card mb-4 shadow-sm">
                    <div className="card-header">
                      <h5 className="mb-0">Frequent Values</h5>
                    </div>
                    <div className="card-body">
                      <div className="table-responsive">
                        <table className="table table-striped table-hover">
                          <thead>
                            <tr>
                              <th>Column</th>
                              <th>Value</th>
                              <th>Frequency</th>
                              <th>Percentage</th>
                            </tr>
                          </thead>
                          <tbody>
                            {profileData.frequent_values && Object.entries(profileData.frequent_values).map(([col, freq]) => (
                              <tr key={col}>
                                <td>{col}</td>
                                <td>{freq.value !== null ? freq.value.toString() : 'NULL'}</td>
                                <td>{freq.frequency}</td>
                                <td>{freq.percentage}%</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              {/* Outliers */}
              {profileData.outliers && Object.keys(profileData.outliers).length > 0 && (
                <div className="card mb-4 shadow-sm">
                  <div className="card-header">
                    <h5 className="mb-0">Outliers Detected</h5>
                  </div>
                  <div className="card-body">
                    <div className="row">
                      {Object.entries(profileData.outliers).map(([col, values]) => (
                        <div className="col-md-4" key={col}>
                          <div className="card mb-3">
                            <div className="card-header bg-warning-subtle">
                              <strong>{col}</strong>
                            </div>
                            <div className="card-body">
                              <small>Outlier values:</small>
                              <ul className="small">
                                {values.map((val, idx) => (
                                  <li key={idx}>{val}</li>
                                ))}
                              </ul>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              )}
            </Tab>

            <Tab eventKey="statistics" title="Numeric Statistics">
              {/* Numeric Statistics */}
              {profileData.numeric_stats && Object.keys(profileData.numeric_stats).length > 0 && (
                <div className="card mb-4 shadow-sm">
                  <div className="card-header">
                    <h5 className="mb-0">Numeric Statistics</h5>
                  </div>
                  <div className="card-body">
                    <div className="table-responsive">
                      <table className="table table-striped table-hover">
                        <thead>
                          <tr>
                            <th>Column</th>
                            <th>Min</th>
                            <th>Max</th>
                            <th>Average</th>
                            <th>Median</th>
                            <th>Sum</th>
                            <th>StdDev</th>
                          </tr>
                        </thead>
                        <tbody>
                          {Object.entries(profileData.numeric_stats).map(([col, stats]) => (
                            <tr key={col}>
                              <td>{col}</td>
                              <td>{stats.min}</td>
                              <td>{stats.max}</td>
                              <td>{typeof stats.avg === 'number' ? stats.avg.toFixed(2) : stats.avg}</td>
                              <td>{typeof stats.median === 'number' ? stats.median.toFixed(2) : stats.median}</td>
                              <td>{typeof stats.sum === 'number' ? stats.sum.toLocaleString() : stats.sum}</td>
                              <td>{typeof stats.stdev === 'number' ? stats.stdev.toFixed(2) : stats.stdev}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              )}

              {/* Text Length Statistics */}
              {profileData.text_length_stats && Object.keys(profileData.text_length_stats).length > 0 && (
                <div className="card mb-4 shadow-sm">
                  <div className="card-header">
                    <h5 className="mb-0">Text Length Statistics</h5>
                  </div>
                  <div className="card-body">
                    <div className="table-responsive">
                      <table className="table table-striped table-hover">
                        <thead>
                          <tr>
                            <th>Column</th>
                            <th>Min Length</th>
                            <th>Max Length</th>
                            <th>Avg Length</th>
                            {profileData.text_patterns && <th>Patterns</th>}
                          </tr>
                        </thead>
                        <tbody>
                          {Object.entries(profileData.text_length_stats).map(([col, stats]) => (
                            <tr key={col}>
                              <td>{col}</td>
                              <td>{stats.min_length}</td>
                              <td>{stats.max_length}</td>
                              <td>{typeof stats.avg_length === 'number' ? stats.avg_length.toFixed(1) : stats.avg_length}</td>
                              {profileData.text_patterns && (
                                <td>
                                  {profileData.text_patterns[col] && (
                                    <ul className="list-unstyled mb-0">
                                      {profileData.text_patterns[col].email_pattern_count > 0 && (
                                        <li><span className="badge bg-info">Email: {profileData.text_patterns[col].email_pattern_count}</span></li>
                                      )}
                                      {profileData.text_patterns[col].numeric_pattern_count > 0 && (
                                        <li><span className="badge bg-secondary">Numeric: {profileData.text_patterns[col].numeric_pattern_count}</span></li>
                                      )}
                                      {profileData.text_patterns[col].date_pattern_count > 0 && (
                                        <li><span className="badge bg-warning">Date: {profileData.text_patterns[col].date_pattern_count}</span></li>
                                      )}
                                    </ul>
                                  )}
                                </td>
                              )}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              )}
            </Tab>

            <Tab eventKey="trends" title="Trends & Changes">
              <div className="row">
                <div className="col-md-6">
                  {/* Row Count Trend */}
                  <TrendChart
                    title="Row Count Trend"
                    labels={profileData.trends?.timestamps || []}
                    datasets={[{
                      label: 'Row Count',
                      data: profileData.trends?.row_counts || [],
                      borderColor: 'rgba(0, 123, 255, 1)',
                      fill: false
                    }]}
                  />
                </div>

                <div className="col-md-6">
                  {/* Null Rate Trends */}
                  <TrendChart
                    title="Null Rate Trends (%)"
                    labels={profileData.trends?.timestamps || []}
                    datasets={Object.entries(profileData.trends?.null_rates || {}).map(([column, values], index) => ({
                      label: column,
                      data: values,
                      borderColor: `hsl(${index * 30}, 70%, 50%)`,
                      fill: false
                    }))}
                  />
                </div>
              </div>

              <div className="row mt-4">
                <div className="col-md-12">
                  {/* Schema Shifts */}
                  <SchemaShift shifts={profileData.schema_shifts} />
                </div>
              </div>
            </Tab>

            <Tab eventKey="anomalies" title="Anomalies & Alerts">
              <div className="row">
                <div className="col-md-12">
                  {/* Anomalies */}
                  <AnomalyList anomalies={profileData.anomalies} />
                </div>
              </div>

              <div className="row mt-4">
                <div className="col-md-12">
                  {/* Alerts */}
                  <AlertsPanel alerts={profileData.alerts} />
                </div>
              </div>
            </Tab>

            <Tab eventKey="validations" title="Validations">
              <div className="row">
                <div className="col-md-12">
                  {/* Validation Results */}
                  <ValidationResults results={profileData.validation_results} />
                </div>
              </div>
            </Tab>

            <Tab eventKey="samples" title="Sample Data">
              {/* Sample Data */}
              {profileData.samples && profileData.samples.length > 0 && (
                <div className="card mb-4 shadow-sm">
                  <div className="card-header">
                    <h5 className="mb-0">Sample Data (Top 100)</h5>
                  </div>
                  <div className="card-body">
                    <div className="table-responsive">
                      <table className="table table-striped table-sm table-hover">
                        <thead>
                          <tr>
                            {Object.keys(profileData.samples[0]).map((key) => (
                              <th key={key}>{key}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {profileData.samples.map((row, idx) => (
                            <tr key={idx}>
                              {Object.values(row).map((value, index) => (
                                <td key={index}>{value !== null ? value.toString() : 'NULL'}</td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              )}
            </Tab>
          </Tabs>
        </>
      ) : (
        <div className="alert alert-info mt-4">
          <i className="bi bi-info-circle-fill me-2"></i>
          Enter connection details and click "Connect" to profile your data.
        </div>
      )}
    </div>
  );
}

export default Dashboard;