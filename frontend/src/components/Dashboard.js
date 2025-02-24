import React, { useEffect, useState } from "react";
import axios from "axios";
import TrendChart from "./TrendChart";
import AnomalyList from "./AnomalyList";
import SchemaShift from "./SchemaShift";

function Dashboard() {
  const [profileData, setProfileData] = useState(null);

  useEffect(() => {
    const fetchProfileData = async () => {
      try {
        const token = localStorage.getItem("token");
        // Adjust the endpoint URL as needed.
        const response = await axios.get("/api/profile", {
          headers: { Authorization: `Bearer ${token}` },
        });
        setProfileData(response.data);
      } catch (error) {
        console.error("Error fetching profile data", error);
      }
    };

    fetchProfileData();
  }, []);

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
