import React, { useEffect, useState, useCallback, useRef } from 'react';
import { directFetchProfile } from '../profile-api';
import { fetchProfileHistory } from '../api';
import TrendChart from './TrendChart';
import AnomalyList from './AnomalyList';
import SchemaShift from './SchemaShift';
import ValidationResults from './ValidationResults';
import AlertsPanel from './AlertsPanel';
import ConnectionForm from './ConnectionForm';
import HistoryTab from './HistoryTab';
import { Tabs, Tab } from 'react-bootstrap';
import { fetchDataPreview } from '../api';


function Dashboard({ onStoreRefreshHandler }) {
  const [profileData, setProfileData] = useState(null);
  const [profileHistory, setProfileHistory] = useState([]);
  const [activeProfileIndex, setActiveProfileIndex] = useState(0); // Default to latest profile (index 0)
  const [selectedProfileData, setSelectedProfileData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [error, setError] = useState(null);
  const [connectionString, setConnectionString] = useState(
    localStorage.getItem('connectionString') || "duckdb:///C:/Users/mhard/PycharmProjects/sparvidata/backend/my_database.duckdb"
  );
  const [tableName, setTableName] = useState(localStorage.getItem('tableName') || "employees");
  const [activeTab, setActiveTab] = useState('overview');
  const [sampleData, setSampleData] = useState([]);
  const [loadingSamples, setLoadingSamples] = useState(false);
  const [previewError, setPreviewError] = useState(null);
  const initialLoadComplete = useRef(false);
  const [previewSettings, setPreviewSettings] = useState({
    maxRows: 50,
    restrictedColumns: []
  });

  console.log("Dashboard rendered with:", { connectionString, tableName });

  // Function to handle profile selection from history
  const handleSelectProfile = (index) => {
    setActiveProfileIndex(index);
    // Set the selected profile data to display in other tabs
    setSelectedProfileData(profileHistory[index]);
  };

  // Function to fetch sample data on-demand
  const fetchSampleData = async () => {
    if (!connectionString || !tableName) {
      setPreviewError("Connection string and table name are required");
      return;
    }

    try {
      setLoadingSamples(true);
      setPreviewError(null);

      const response = await fetchDataPreview(
        connectionString,
        tableName,
        previewSettings.maxRows
      );

      setSampleData(response.preview_data || []);

      // Update preview settings
      setPreviewSettings({
        maxRows: response.preview_max || 50,
        restrictedColumns: response.restricted_columns || [],
        allColumns: response.all_columns || []
      });
    } catch (error) {
      console.error("Error fetching sample data:", error);
      setPreviewError(error.response?.data?.error || "Failed to load data preview");
      setSampleData([]);
    } finally {
      setLoadingSamples(false);
    }
  };

  // Define the profile data handler
  const handleProfileData = useCallback(async () => {
    console.log("Profile Data function called");
    if (connectionString && tableName) {
      console.log("Profiling data for:", { connectionString, tableName });
      setProfileData(null);
      setLoading(true);
      setError(null);

      try {
        const data = await directFetchProfile(connectionString, tableName);
        console.log("Profile successful, data received:", data);
        setProfileData(data);

        // Also refresh history data
        await loadProfileHistory(tableName);

        // Reset to showing the latest profile after profiling
        setActiveProfileIndex(0);

      } catch (err) {
        console.error("Profile failed:", err);
        setError(err.response?.data?.error || 'Failed to profile data');
      } finally {
        setLoading(false);
      }
    } else {
      console.log("Cannot profile: missing connection string or table name");
      setError("Connection string and table name are required to profile data");
    }
  }, [connectionString, tableName]); // Dependencies

  useEffect(() => {
    // This function handles all initial loading
    async function initialLoad() {
      // EXTENSIVE DEBUGGING
      console.log("%c[Dashboard] === INITIAL LOAD STARTED ===", "background: #333; color: yellow; font-size: 12px");
      console.log("%c[Dashboard] Component just mounted", "color: blue");

      // Only run this once
      if (initialLoadComplete.current) {
        console.log("[Dashboard] Initial load already completed, skipping");
        return;
      }

      console.log("%c[Dashboard] Setting initialLoadComplete to true", "color: magenta");
      initialLoadComplete.current = true;
      setLoading(true);

      // Connections
      const storedConnString = connectionString;
      const storedTable = tableName;

      console.log("%c[Dashboard] Connection info from localStorage:", "color: green", {
        storedConnString: storedConnString ? (storedConnString.substring(0, 20) + "...") : null,
        storedTable
      });

      if (storedConnString && storedTable) {
        console.log("%c[Dashboard] Setting connection state values", "color: orange");
        setConnectionString(storedConnString);
        setTableName(storedTable);

        try {
          // First try to load profile history (which is faster and doesn't trigger a profile run)
          console.log("%c[Dashboard] About to call loadProfileHistory()", "background: #ff9; color: black");
          const historyResult = await loadProfileHistory(storedTable);
          console.log("%c[Dashboard] loadProfileHistory result:", "background: #ff9; color: black", historyResult);

          if (historyResult && historyResult.success) {
            console.log("%c[Dashboard] History loaded successfully with items:", "color: green", historyResult.data.length);
            // If we loaded history successfully, we can skip the profile run for now
            setLoading(false);
          } else {
            // If we couldn't load history, try to directly fetch profile data
            console.log("%c[Dashboard] No history found, fetching fresh profile...", "color: orange");
            try {
              console.log("%c[Dashboard] About to call directFetchProfile", "background: #ccf; color: black");
              const data = await directFetchProfile(storedConnString, storedTable);
              console.log("%c[Dashboard] directFetchProfile returned data", "color: green", data ? "✅" : "❌");
              setProfileData(data);
            } catch (profileErr) {
              console.error("%c[Dashboard] Failed to fetch initial profile:", "color: red", profileErr);
              setError("Failed to load initial profile data. Try clicking 'Profile Data'.");
            } finally {
              console.log("%c[Dashboard] Setting loading to false after profile attempt", "color: purple");
              setLoading(false);
            }
          }
        } catch (err) {
          console.error("%c[Dashboard] Error in initial load:", "color: red", err);
          setError("Error loading initial data");
          setLoading(false);
        }
      } else {
        console.log("%c[Dashboard] No stored connection info found, setting loading = false", "color: orange");
        setLoading(false);
      }

      console.log("%c[Dashboard] === INITIAL LOAD COMPLETED ===", "background: #333; color: yellow; font-size: 12px");
    }

    // IMMEDIATELY call initialLoad instead of waiting for next tick
    console.log("%c[Dashboard] useEffect mounted, calling initialLoad immediately", "color: red");
    initialLoad();

    // Set up the refresh handler for the parent component
    if (onStoreRefreshHandler) {
      console.log("[Dashboard] Setting up refresh handler with parent");
      onStoreRefreshHandler(handleProfileData);
    }

    // Cleanup function
    return () => {
      console.log("[Dashboard] Component unmounting, cleaning up");
    };
  }, []); // Empty dependency array - run once on mount

  console.log("Component defined");

  // Function to load profile history
  const loadProfileHistory = async (table) => {
    console.log("%c[Dashboard] loadProfileHistory called with table:", "background: cyan; color: black", table);

    if (!table) {
      console.log("%c[Dashboard] No table provided to loadProfileHistory, returning early", "color: red");
      return { success: false, reason: "no_table" };
    }

    try {
      console.log(`%c[Dashboard] Fetching profile history for table ${table}`, "color: blue");
      setHistoryLoading(true);

      console.log("%c[Dashboard] About to call fetchProfileHistory API", "color: purple");
      const response = await fetchProfileHistory(table, 15);

      console.log("%c[Dashboard] fetchProfileHistory returned:", "color: purple", {
        hasHistory: !!response.history,
        itemCount: response.history?.length || 0
      });

      if (response.history && response.history.length > 0) {
        console.log("%c[Dashboard] Setting profile history state", "color: green");
        setProfileHistory(response.history);

        // Set the selected profile data to the most recent one by default
        const latestProfile = response.history[0];
        console.log("%c[Dashboard] Setting latest profile as display data", "color: green", {
          timestamp: latestProfile.timestamp,
          table: latestProfile.table
        });

        setSelectedProfileData(latestProfile);
        setActiveProfileIndex(0);

        console.log("%c[Dashboard] Returning success=true from loadProfileHistory", "background: green; color: white");
        return { success: true, data: response.history };
      } else {
        console.log("%c[Dashboard] No profile history found", "color: orange");
        setProfileHistory([]);
        setSelectedProfileData(null);
        console.log("%c[Dashboard] Returning success=false from loadProfileHistory", "background: orange; color: black");
        return { success: false, reason: "no_history" };
      }
    } catch (err) {
      console.error("%c[Dashboard] Error loading profile history:", "color: red", err);
      console.log("%c[Dashboard] Returning success=false with error from loadProfileHistory", "background: red; color: white");
      return { success: false, reason: "error", error: err };
    } finally {
      console.log("%c[Dashboard] Setting historyLoading=false", "color: blue");
      setHistoryLoading(false);
    }
  };

  const handleConnectionSubmit = (newConnection, newTable) => {
    console.log('[Dashboard] Connection form submitted:', { newConnection, newTable });

    // Make sure the new values are different from the current ones
    if (newConnection !== connectionString || newTable !== tableName) {
      console.log('[Dashboard] Setting new connection and table values');
      setConnectionString(newConnection);
      setTableName(newTable);

      // Save connection info to localStorage
      localStorage.setItem('connectionString', newConnection);
      localStorage.setItem('tableName', newTable);

      // Reset active profile index when changing tables
      setActiveProfileIndex(0);

      // Reset profileData to indicate no profile run yet
      setProfileData(null);

      // Still load profile history for the new table
      loadProfileHistory(newTable);
    }
  };

  // Get the active profile data for display
  const displayData = selectedProfileData || profileData;

  useEffect(() => {
    console.log('[Dashboard] Display data updated:', displayData ? 'data available' : 'no data');
  }, [displayData]);

  return (
    <div className="container-fluid mt-3">
      <div className="d-flex justify-content-between align-items-center mb-4">
        <h2>
          Data Profiler
        </h2>
        <div>
          <button className="btn btn-primary" onClick={handleProfileData} disabled={loading || !connectionString || !tableName}>
            {loading ? (
              <>
                <span className="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>
                Profiling...
              </>
            ) : (
              <>
                <i className="bi bi-lightning-charge me-1"></i> Profile Data
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
      ) : displayData ? (
        <>
          {/* Profile Overview Card */}
          <div className="card mb-4 mt-4 shadow-sm">
            <div className="card-header bg-primary text-white">
              <div className="d-flex justify-content-between align-items-center">
                <h5 className="mb-0">Profile Overview: {displayData.table}</h5>
                {activeProfileIndex > 0 && (
                  <span className="badge bg-warning">
                    <i className="bi bi-clock-history me-1"></i>
                    Viewing Historical Data
                  </span>
                )}
              </div>
            </div>
            <div className="card-body">
              <div className="row">
                <div className="col-md-3">
                  <div className="border rounded p-3 text-center mb-3">
                    <h6>Row Count</h6>
                    <h3>{displayData.row_count.toLocaleString()}</h3>
                  </div>
                </div>
                <div className="col-md-3">
                  <div className="border rounded p-3 text-center mb-3">
                    <h6>Duplicate Rows</h6>
                    <h3>{(displayData.duplicate_count || 0).toLocaleString()}</h3>
                  </div>
                </div>
                <div className="col-md-3">
                  <div className="border rounded p-3 text-center mb-3">
                    <h6>Columns</h6>
                    <h3>{Object.keys(displayData.completeness).length}</h3>
                  </div>
                </div>
                <div className="col-md-3">
                  <div className="border rounded p-3 text-center mb-3">
                    <h6>Profile Date</h6>
                    <p className="mb-0">{new Date(displayData.timestamp).toLocaleString()}</p>
                  </div>
                </div>
              </div>

              {/* Alert badges */}
              {displayData.anomalies && displayData.anomalies.length > 0 && (
                <div className="alert alert-warning mt-3">
                  <i className="bi bi-exclamation-triangle-fill me-2"></i>
                  <strong>{displayData.anomalies.length} anomalies detected</strong>
                </div>
              )}

              {displayData.schema_shifts && displayData.schema_shifts.length > 0 && (
                <div className="alert alert-danger mt-3">
                  <i className="bi bi-exclamation-octagon-fill me-2"></i>
                  <strong>{displayData.schema_shifts.length} schema shifts detected</strong>
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
            {/* Add History tab as the first tab */}
            <Tab eventKey="history" title="History">
              <HistoryTab
                profileHistory={profileHistory}
                historyLoading={historyLoading}
                activeProfileIndex={activeProfileIndex}
                onSelectProfile={handleSelectProfile}
              />
            </Tab>

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
                            {displayData.completeness && Object.entries(displayData.completeness).map(([col, metrics]) => (
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
                            {displayData.frequent_values && Object.entries(displayData.frequent_values).map(([col, freq]) => (
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
              {displayData.outliers && Object.keys(displayData.outliers).length > 0 && (
                <div className="card mb-4 shadow-sm">
                  <div className="card-header">
                    <h5 className="mb-0">Outliers Detected</h5>
                  </div>
                  <div className="card-body">
                    <div className="row">
                      {Object.entries(displayData.outliers).map(([col, values]) => (
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
              {displayData.numeric_stats && Object.keys(displayData.numeric_stats).length > 0 && (
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
                          {Object.entries(displayData.numeric_stats).map(([col, stats]) => (
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

              {/* Date Range Statistics */}
              {displayData.date_stats && Object.keys(displayData.date_stats).length > 0 && (
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
                          {Object.entries(displayData.date_stats).map(([col, stats]) => {
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
              {displayData.text_length_stats && Object.keys(displayData.text_length_stats).length > 0 && (
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
                            {displayData.text_patterns && <th>Patterns</th>}
                          </tr>
                        </thead>
                        <tbody>
                          {Object.entries(displayData.text_length_stats).map(([col, stats]) => (
                            <tr key={col}>
                              <td>{col}</td>
                              <td>{stats.min_length}</td>
                              <td>{stats.max_length}</td>
                              <td>{typeof stats.avg_length === 'number' ? stats.avg_length.toFixed(1) : stats.avg_length}</td>
                              {displayData.text_patterns && (
                                <td>
                                  {displayData.text_patterns[col] && (
                                    <ul className="list-unstyled mb-0">
                                      {displayData.text_patterns[col].email_pattern_count > 0 && (
                                        <li><span className="badge bg-info">Email: {displayData.text_patterns[col].email_pattern_count}</span></li>
                                      )}
                                      {displayData.text_patterns[col].numeric_pattern_count > 0 && (
                                        <li><span className="badge bg-secondary">Numeric: {displayData.text_patterns[col].numeric_pattern_count}</span></li>
                                      )}
                                      {displayData.text_patterns[col].date_pattern_count > 0 && (
                                        <li><span className="badge bg-warning">Date: {displayData.text_patterns[col].date_pattern_count}</span></li>
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
                  {/* Show a note when viewing historical data */}
                  {activeProfileIndex > 0 && (
                    <div className="alert alert-info mb-4">
                      <i className="bi bi-info-circle-fill me-2"></i>
                      <strong>Note:</strong> You are viewing a historical profile from {new Date(displayData.timestamp).toLocaleString()}.
                      The trend charts still show data from all profile runs over time.
                    </div>
                  )}

                  <div className="row">
                    <div className="col-md-6">
                      {/* Row Count Trend */}
                      <TrendChart
                        title="Row Count Trend"
                        subtitle={`Current: ${displayData.row_count.toLocaleString()}`}
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
                        subtitle={`Current: ${(displayData.duplicate_count || 0).toLocaleString()}`}
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

                  {/* Only render null rates chart if we have data */}
                  {profileData.trends?.null_rates && Object.keys(profileData.trends.null_rates).length > 0 && (
                    <div className="row mt-3">
                      <div className="col-md-12">
                        {/* Null Rate Trends */}
                        <TrendChart
                          title="Null Rate Trends (%)"
                          height={400}
                          labels={profileData.trends?.formatted_timestamps || profileData.trends?.timestamps || []}
                          datasets={Object.entries(profileData.trends.null_rates || {})
                            .map(([column, values], index) => ({
                              label: column,
                              data: values,
                              borderColor: `hsl(${index * 33 % 360}, 70%, 50%)`,
                              backgroundColor: `hsla(${index * 33 % 360}, 70%, 50%, 0.1)`,
                              borderWidth: 2,
                              pointRadius: 3,
                              fill: false,
                              spanGaps: true
                            }))}
                        />
                      </div>
                    </div>
                  )}

                  {/* Only render validation success rate chart if we have data */}
                  {profileData.trends?.validation_success_rates &&
                   profileData.trends.validation_success_rates.length > 0 &&
                   profileData.trends.validation_success_rates.some(rate => rate !== null) && (
                    <div className="row mt-3">
                      <div className="col-md-12">
                        <TrendChart
                          title="Validation Success Rate (%)"
                          labels={profileData.trends?.formatted_timestamps || profileData.trends?.timestamps || []}
                          datasets={[{
                            label: 'Success Rate',
                            data: profileData.trends.validation_success_rates || [],
                            borderColor: 'rgba(40, 167, 69, 1)',
                            backgroundColor: 'rgba(40, 167, 69, 0.1)',
                            fill: true
                          }]}
                        />
                      </div>
                    </div>
                  )}

                  <div className="row mt-4">
                    <div className="col-md-12">
                      {/* Schema Shifts */}
                      <SchemaShift shifts={displayData?.schema_shifts || []} />
                    </div>
                  </div>
                </>
              )}
            </Tab>

            <Tab eventKey="anomalies" title="Anomalies & Alerts">
              <div className="row">
                <div className="col-md-12">
                  {/* Anomalies */}
                  <AnomalyList anomalies={displayData.anomalies} />
                </div>
              </div>

              <div className="row mt-4">
                <div className="col-md-12">
                  {/* Alerts */}
                  <AlertsPanel alerts={displayData.alerts} />
                </div>
              </div>
            </Tab>

            <Tab eventKey="validations" title="Validations">
              <div className="row">
                <div className="col-md-12">
                  {activeProfileIndex > 0 ? (
                    <>
                      {/* Show historical validation results when viewing a historical profile */}
                      <div className="card mb-4 shadow-sm">
                        <div className="card-header bg-info-subtle">
                          <h5 className="mb-0">Historical Validation Results</h5>
                        </div>
                        <div className="card-body">
                          {displayData.validation_results && displayData.validation_results.length > 0 ? (
                            <>
                              <div className="alert alert-info mb-4">
                                <i className="bi bi-info-circle-fill me-2"></i>
                                <strong>Note:</strong> You are viewing validation results from a historical profile captured on {new Date(displayData.timestamp).toLocaleString()}.
                              </div>

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
                                    {displayData.validation_results.map((result, idx) => (
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
                            </>
                          ) : (
                            <div className="alert alert-warning">
                              <i className="bi bi-exclamation-triangle-fill me-2"></i>
                              No validation results were stored with this historical profile.
                            </div>
                          )}

                          <div className="mt-3 text-center">
                            <button
                              className="btn btn-primary"
                              onClick={() => {
                                setActiveProfileIndex(0);
                                setActiveTab('validations');
                              }}
                            >
                              <i className="bi bi-arrow-left-circle me-2"></i>
                              Switch to Current Profile
                            </button>
                          </div>
                        </div>
                      </div>
                    </>
                  ) : (
                    /* Current ValidationResults component for the latest profile */
                    <ValidationResults
                      tableName={tableName}
                      connectionString={connectionString}
                    />
                  )}
                </div>
              </div>
            </Tab>

            <Tab eventKey="samples" title="Sample Data">
              <div className="card mb-4 shadow-sm">
                <div className="card-header d-flex justify-content-between align-items-center">
                  <h5 className="mb-0">Sample Data Preview</h5>
                  <div>
                    <select
                      className="form-select form-select-sm me-2 d-inline-block"
                      style={{ width: 'auto' }}
                      value={previewSettings.maxRows}
                      onChange={(e) => setPreviewSettings({...previewSettings, maxRows: parseInt(e.target.value)})}
                    >
                      <option value="10">10 rows</option>
                      <option value="25">25 rows</option>
                      <option value="50">50 rows</option>
                    </select>
                    <button
                      className="btn btn-sm btn-primary"
                      onClick={fetchSampleData}
                      disabled={loadingSamples}
                    >
                      {loadingSamples ? (
                        <>
                          <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                          Loading...
                        </>
                      ) : (
                        <>
                          <i className="bi bi-eye me-1"></i> Load Preview
                        </>
                      )}
                    </button>
                  </div>
                </div>
                <div className="card-body">
                  {previewError && (
                    <div className="alert alert-danger">
                      <i className="bi bi-exclamation-triangle-fill me-2"></i>
                      {previewError}
                    </div>
                  )}

                  {previewSettings.restrictedColumns && previewSettings.restrictedColumns.length > 0 && (
                    <div className="alert alert-warning">
                      <i className="bi bi-shield-exclamation me-2"></i>
                      <strong>Restricted columns:</strong> Some columns are restricted from preview by your organization settings.
                      <div className="mt-1">
                        <span className="badge bg-secondary me-1">
                          {previewSettings.restrictedColumns.join('</span> <span className="badge bg-secondary me-1">')}
                        </span>
                      </div>
                    </div>
                  )}

                  {!sampleData.length && !loadingSamples && !previewError ? (
                    <div className="alert alert-info">
                      <i className="bi bi-info-circle me-2"></i>
                      Click "Load Preview" to see sample data. Data previews are not stored and are generated on-demand.
                    </div>
                  ) : (
                    <div className="table-responsive">
                      {sampleData.length > 0 && (
                        <table className="table table-striped table-sm table-hover">
                          <thead>
                            <tr>
                              {Object.keys(sampleData[0]).map((key) => (
                                <th key={key}>{key}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {sampleData.map((row, idx) => (
                              <tr key={idx}>
                                {Object.values(row).map((value, index) => (
                                  <td key={index}>{value !== null ? String(value) : 'NULL'}</td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      )}
                    </div>
                  )}

                  <div className="mt-3 small text-muted">
                    <i className="bi bi-shield-lock me-1"></i>
                    <strong>Privacy Notice:</strong> Data previews are generated on-demand and never stored in our system.
                    Maximum {previewSettings.maxRows} rows are displayed for privacy and performance. Preview access is logged
                    for audit purposes, but the actual data viewed is not recorded.
                  </div>
                </div>
              </div>
            </Tab>
          </Tabs>
        </>
      ) : connectionString && tableName ? (
        <div className="alert alert-info mt-4">
          <i className="bi bi-info-circle-fill me-2"></i>
          Connection established. Click the <strong>"Profile Data"</strong> button to analyze your data.
        </div>
      ) : (
        <div className="alert alert-info mt-4">
          <i className="bi bi-info-circle-fill me-2"></i>
          Enter connection details and click "Connect" to get started.
        </div>
      )}
    </div>
  );
}

export default Dashboard;