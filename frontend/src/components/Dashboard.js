import React, { useEffect, useState } from 'react';
import { fetchProfile } from '../api';
import TrendChart from './TrendChart';
import AnomalyList from './AnomalyList';
import SchemaShift from './SchemaShift';

function Dashboard() {
  const [profileData, setProfileData] = useState(null);
  const token = localStorage.getItem("token");
  console.log("DEBUG: Token from localStorage:", token);

  // Update these as needed for your environment:
  const connectionString = "duckdb:///C:/Users/mhard/PycharmProjects/HawkDB/backend/my_database.duckdb";
  const table = "employees";

  useEffect(() => {
    const getData = async () => {
      try {
        console.log('DEBUG: Fetching profile data...');
        const data = await fetchProfile(token, connectionString, table);
        console.log('DEBUG: Received profile data:', data);
        setProfileData(data);
      } catch (error) {
        console.error('DEBUG: Error fetching profile data:', error);
      }
    };
    getData();
  }, [token, connectionString, table]);

  return (
    <div className="container mt-5">
      <h2>Dashboard</h2>
      {profileData ? (
        <>
          {/* Overall Information */}
          <div className="card mb-3">
            <div className="card-body">
              <h5 className="card-title">Overall Information</h5>
              <p><strong>Table:</strong> {profileData.table}</p>
              <p><strong>Row Count:</strong> {profileData.row_count}</p>
              <p><strong>Timestamp:</strong> {profileData.timestamp}</p>
            </div>
          </div>

          {/* Data Completeness */}
          <div className="card mb-3">
            <div className="card-body">
              <h5 className="card-title">Data Completeness</h5>
              <table className="table table-bordered">
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Nulls</th>
                    <th>Blanks</th>
                    <th>Distinct Count</th>
                  </tr>
                </thead>
                <tbody>
                  {profileData.completeness && Object.entries(profileData.completeness).map(([col, metrics]) => (
                    <tr key={col}>
                      <td>{col}</td>
                      <td>{metrics.nulls}</td>
                      <td>{metrics.blanks}</td>
                      <td>{metrics.distinct_count}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Numeric Statistics */}
          {profileData.numeric_stats && Object.keys(profileData.numeric_stats).length > 0 && (
            <div className="card mb-3">
              <div className="card-body">
                <h5 className="card-title">Numeric Statistics</h5>
                <table className="table table-bordered">
                  <thead>
                    <tr>
                      <th>Column</th>
                      <th>Min</th>
                      <th>Max</th>
                      <th>Avg</th>
                      <th>Sum</th>
                      <th>Stddev</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(profileData.numeric_stats).map(([col, stats]) => (
                      <tr key={col}>
                        <td>{col}</td>
                        <td>{stats.min}</td>
                        <td>{stats.max}</td>
                        <td>{stats.avg}</td>
                        <td>{stats.sum}</td>
                        <td>{stats.stdev}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Text Length Statistics */}
          {profileData.text_length_stats && Object.keys(profileData.text_length_stats).length > 0 && (
            <div className="card mb-3">
              <div className="card-body">
                <h5 className="card-title">Text Length Statistics</h5>
                <table className="table table-bordered">
                  <thead>
                    <tr>
                      <th>Column</th>
                      <th>Min Length</th>
                      <th>Max Length</th>
                      <th>Avg Length</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(profileData.text_length_stats).map(([col, stats]) => (
                      <tr key={col}>
                        <td>{col}</td>
                        <td>{stats.min_length}</td>
                        <td>{stats.max_length}</td>
                        <td>{stats.avg_length}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Frequent Values */}
          {profileData.frequent_values && Object.keys(profileData.frequent_values).length > 0 && (
            <div className="card mb-3">
              <div className="card-body">
                <h5 className="card-title">Frequent Values</h5>
                <table className="table table-bordered">
                  <thead>
                    <tr>
                      <th>Column</th>
                      <th>Value</th>
                      <th>Frequency</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(profileData.frequent_values).map(([col, freq]) => (
                      <tr key={col}>
                        <td>{col}</td>
                        <td>{freq.value}</td>
                        <td>{freq.frequency}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Sample Data */}
          {profileData.samples && profileData.samples.length > 0 && (
            <div className="card mb-3">
              <div className="card-body">
                <h5 className="card-title">Sample Data (Top 100)</h5>
                <div className="table-responsive">
                  <table className="table table-bordered">
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
                            <td key={index}>{value}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}

          {/* Fuzzy Matches */}
          <div className="card mb-3">
            <div className="card-body">
              <h5 className="card-title">Fuzzy Matches</h5>
              <p>{profileData.fuzzy_matches}</p>
            </div>
          </div>

          {/* Optional: Render TrendChart, AnomalyList, SchemaShift as additional components */}
          <TrendChart data={profileData.trends} />
          <AnomalyList anomalies={profileData.anomalies} />
          <SchemaShift shifts={profileData.schema_shifts} />
        </>
      ) : (
        <p>Loading data...</p>
      )}
    </div>
  );
}

export default Dashboard;
