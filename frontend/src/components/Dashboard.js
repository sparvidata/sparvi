import React, { useEffect, useState, useCallback } from 'react';
import { directFetchProfile } from '../profile-api';
import { fetchProfileHistory } from '../api'; // Import the new function
import TrendChart from './TrendChart';
import AnomalyList from './AnomalyList';
import SchemaShift from './SchemaShift';
import ValidationResults from './ValidationResults';
import AlertsPanel from './AlertsPanel';
import ConnectionForm from './ConnectionForm';
import ProfileHistory from './ProfileHistory'; // Import the new component
import { Tabs, Tab } from 'react-bootstrap';

function Dashboard({ onStoreRefreshHandler }) {
  const [profileData, setProfileData] = useState(null);
  const [profileHistory, setProfileHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [error, setError] = useState(null);
  const [connectionString, setConnectionString] = useState(
    localStorage.getItem('connectionString') || "duckdb:///C:/Users/mhard/PycharmProjects/sparvidata/backend/my_database.duckdb"
  );
  const [tableName, setTableName] = useState(localStorage.getItem('tableName') || "employees");
  const [activeTab, setActiveTab] = useState('overview');

  console.log("Dashboard rendered with:", { connectionString, tableName });

  // Define the refresh handler
  const handleRefresh = async () => {
    console.log("Refresh button clicked");
    // Re-fetch data when the refresh button is clicked
    if (connectionString && tableName) {
      console.log("Refreshing data for:", { connectionString, tableName });
      setProfileData(null);
      try {
        const data = await directFetchProfile(connectionString, tableName);
        console.log("Refresh successful, data received:", data);
        setProfileData(data);

        // Also refresh history data
        await loadProfileHistory(tableName);
      } catch (err) {
        console.error("Refresh failed:", err);
        setError(err.response?.data?.error || 'Failed to refresh data');
      }
    } else {
      console.log("Cannot refresh: missing connection string or table name");
    }
  };

  // Function to load profile history
  const loadProfileHistory = async (table) => {
    if (!table) return;

    try {
      setHistoryLoading(true);
      const response = await fetchProfileHistory(table, 15); // Get up to 15 history items
      console.log("Profile history loaded:", response.history);
      setProfileHistory(response.history || []);
    } catch (err) {
      console.error("Error loading profile history:", err);
      // Don't set error state here to avoid interfering with the main profile display
    } finally {
      setHistoryLoading(false);
    }
  };

  // Share the refresh handler with the parent component
  useEffect(() => {
    if (onStoreRefreshHandler) {
      onStoreRefreshHandler(handleRefresh);
    }
  }, [onStoreRefreshHandler, handleRefresh]);

  // Initial data loading
  useEffect(() => {
    const getData = async () => {
      console.log("getData called with:", { connectionString, tableName });
      try {
        setLoading(true);
        setError(null);

        console.log('Fetching profile data from backend...');
        const data = await directFetchProfile(connectionString, tableName);
        console.log('Profile data received:', data);
        setProfileData(data);

        // Also load profile history
        await loadProfileHistory(tableName);

        // Save connection info to localStorage
        localStorage.setItem('connectionString', connectionString);
        localStorage.setItem('tableName', tableName);
        console.log('Connection info saved to localStorage');
      } catch (error) {
        console.error('Error fetching profile data:', error);
        if (error.response) {
          console.error('Error response data:', error.response.data);
          console.error('Error response status:', error.response.status);
        } else if (error.request) {
          console.error('No response received:', error.request);
        } else {
          console.error('Error message:', error.message);
        }
        setError(error.response?.data?.error || 'Failed to fetch profile data');
      } finally {
        setLoading(false);
      }
    };

    if (connectionString && tableName) {
      console.log('Connection string and table name available, calling getData()');
      getData();
    } else {
      console.log('Missing connection string or table name, not loading data');
      setLoading(false);
    }
  }, [connectionString, tableName]);

  const handleConnectionSubmit = (newConnection, newTable) => {
    console.log('Connection form submitted:', { newConnection, newTable });

    // Make sure the new values are different from the current ones
    if (newConnection !== connectionString || newTable !== tableName) {
      console.log('Setting new connection and table values');
      setConnectionString(newConnection);
      setTableName(newTable);
    } else {
      console.log('Connection and table values unchanged, refreshing anyway');
      handleRefresh();
    }
  };

  // When the tab changes to "trends", ensure we have the history data
  useEffect(() => {
    if (activeTab === 'trends' && tableName && profileHistory.length === 0 && !historyLoading) {
      loadProfileHistory(tableName);
    }
  }, [activeTab, tableName, profileHistory.length, historyLoading]);

  return (
    <div className="container-fluid mt-3">
      <div className="d-flex justify-content-between align-items-center mb-4">
        <h2>
          Sparvi Data Profiler
        </h2>
        <div>
          <button className="btn btn-outline-secondary" onClick={handleRefresh} disabled={loading}>
            {loading ? (
              <>
                <span className="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
                Loading...
              </>
            ) : (
              <>
                <i className="bi bi-arrow-clockwise me-1"></i> Refresh
              </>
            )}
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

              {/* Date Range Statistics - Add this new section */}
              {profileData.date_stats && Object.keys(profileData.date_stats).length > 0 && (
                <div className="card mb-4 shadow-sm">
                  <div className="card-header">
                    <h5 className="mb-0">Date Range Statistics</h5>
                  </div>
                  <div className="card-body">
                    <div className="table-responsive">
                      <table className="table table-striped table-hover">
                        <thead>
                          <tr>
                            <th>Date Column</th>
                            <th>Min Date</th>
                            <th>Max Date</th>
                            <th>Distinct Count</th>
                            <th>Date Range</th>
                          </tr>
                        </thead>
                        <tbody>
                          {Object.entries(profileData.date_stats).map(([col, stats]) => {
                            // Calculate date difference if both min and max exist
                            let dateRange = '';
                            if (stats.min_date && stats.max_date) {
                              const minDate = new Date(stats.min_date);
                              const maxDate = new Date(stats.max_date);
                              const diffTime = Math.abs(maxDate - minDate);
                              const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                              dateRange = `${diffDays} days`;
                            }

                            return (
                              <tr key={col}>
                                <td>{col}</td>
                                <td>{stats.min_date ? new Date(stats.min_date).toLocaleDateString() : 'N/A'}</td>
                                <td>{stats.max_date ? new Date(stats.max_date).toLocaleDateString() : 'N/A'}</td>
                                <td>{stats.distinct_count}</td>
                                <td>{dateRange || 'N/A'}</td>
                              </tr>
                            );
                          })}
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
              {!profileData?.trends?.timestamps || profileData.trends.timestamps.length <= 1 ? (
                <div className="alert alert-info mt-3">
                  <i className="bi bi-info-circle-fill me-2"></i>
                  <strong>Insufficient historical data available.</strong> Run the profiler multiple times to generate trend data.
                  Current run count: {profileData?.trends?.timestamps?.length || 0}/2 minimum required.
                </div>
              ) : (
                <>
                  <div className="row">
                    <div className="col-md-6">
                      {/* Row Count Trend */}
                      <TrendChart
                        title="Row Count Trend"
                        subtitle={`Current: ${profileData.row_count.toLocaleString()}`}
                        labels={profileData.trends?.formatted_timestamps || profileData.trends?.timestamps || []}
                        datasets={[{
                          label: 'Row Count',
                          data: profileData.trends?.row_counts || [],
                          borderColor: 'rgba(0, 123, 255, 1)',
                          backgroundColor: 'rgba(0, 123, 255, 0.1)',
                          fill: true
                        }]}
                      />
                    </div>

                    <div className="col-md-6">
                      {/* Duplicate Count Trend */}
                      <TrendChart
                        title="Duplicate Rows Trend"
                        subtitle={`Current: ${(profileData.duplicate_count || 0).toLocaleString()}`}
                        labels={profileData.trends?.formatted_timestamps || profileData.trends?.timestamps || []}
                        datasets={[{
                          label: 'Duplicate Rows',
                          data: profileData.trends?.duplicate_counts || [],
                          borderColor: 'rgba(220, 53, 69, 1)',
                          backgroundColor: 'rgba(220, 53, 69, 0.1)',
                          fill: true
                        }]}
                      />
                    </div>
                  </div>

                  <div className="row mt-3">
                    <div className="col-md-12">
                      {/* Null Rate Trends */}
                      <TrendChart
                        title="Null Rate Trends (%)"
                        height={400}
                        labels={profileData.trends?.formatted_timestamps || profileData.trends?.timestamps || []}
                        datasets={Object.entries(profileData.trends?.null_rates || {})
                          .filter(([column, values]) => values.some(v => v > 0)) // Only show columns with some nulls
                          .map(([column, values], index) => ({
                            label: column,
                            data: values,
                            borderColor: `hsl(${index * 30}, 70%, 50%)`,
                            backgroundColor: `hsla(${index * 30}, 70%, 50%, 0.1)`,
                            fill: false
                          }))}
                      />
                    </div>
                  </div>

                  {profileData.trends?.validation_success_rates?.some(rate => rate !== null) && (
                    <div className="row mt-3">
                      <div className="col-md-12">
                        {/* Validation Success Rate Trend */}
                        <TrendChart
                          title="Validation Success Rate (%)"
                          labels={profileData.trends?.formatted_timestamps || profileData.trends?.timestamps || []}
                          datasets={[{
                            label: 'Success Rate',
                            data: profileData.trends?.validation_success_rates || [],
                            borderColor: 'rgba(40, 167, 69, 1)',
                            backgroundColor: 'rgba(40, 167, 69, 0.1)',
                            fill: true
                          }]}
                        />
                      </div>
                    </div>
                  )}
                </>
              )}

              {/* Profile History Table */}
              {historyLoading ? (
                <div className="d-flex justify-content-center my-4">
                  <div className="spinner-border text-primary" role="status">
                    <span className="visually-hidden">Loading history...</span>
                  </div>
                </div>
              ) : (
                <ProfileHistory history={profileHistory} />
              )}

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
                  <ValidationResults
                    tableName={tableName}
                    connectionString={connectionString}
                  />
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