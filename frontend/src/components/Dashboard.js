// frontend/src/components/Dashboard.js
import React, { useEffect, useState } from 'react';
import { fetchProfile } from '../api';
import TrendChart from './TrendChart';
import AnomalyList from './AnomalyList';
import SchemaShift from './SchemaShift';

function Dashboard() {
  const [profileData, setProfileData] = useState(null);
  const token = localStorage.getItem('token');

  // Update the connection string to match your environment.
  const connectionString = "duckdb:///C:/Users/mhard/PycharmProjects/HawkDB/my_database.duckdb";
  const table = "employees";

  useEffect(() => {
    const connectionString = "duckdb:///C:/Users/mhard/PycharmProjects/HawkDB/my_database.duckdb";
    const table = "employees";

    fetchProfile(token, connectionString, table)
      .then((data) => {
        console.log("Profile Data:", data);
        setProfileData(data);
      })
      .catch((error) => console.error("Error fetching profile:", error));
  }, [token]);

  return (
    <div className="container mt-5">
      <h2>Dashboard</h2>
      {profileData ? (
        <>
          <div className="card mb-3">
            <div className="card-body">
              <h5 className="card-title">Overall Information</h5>
              <p><strong>Table:</strong> {profileData.table}</p>
              <p><strong>Row Count:</strong> {profileData.row_count}</p>
              <p><strong>Timestamp:</strong> {profileData.timestamp}</p>
            </div>
          </div>
          <div className="card mb-3">
            <div className="card-body">
              <h5 className="card-title">Data Completeness</h5>
              <table className="table table-bordered">
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Nulls</th>
                    <th>Blanks</th>
                  </tr>
                </thead>
                <tbody>
                  {profileData.completeness && Object.entries(profileData.completeness).map(([col, metrics]) => (
                    <tr key={col}>
                      <td>{col}</td>
                      <td>{metrics.nulls}</td>
                      <td>{metrics.blanks}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
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
