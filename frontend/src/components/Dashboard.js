import React, { useEffect, useState, useCallback} from 'react';
import { directFetchProfile } from '../profile-api';
import TrendChart from './TrendChart';
import AnomalyList from './AnomalyList';
import SchemaShift from './SchemaShift';
import ValidationResults from './ValidationResults';
import AlertsPanel from './AlertsPanel';
import ConnectionForm from './ConnectionForm';
import { Tabs, Tab } from 'react-bootstrap';

function Dashboard({ onStoreRefreshHandler }) {
  const [profileData, setProfileData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [connectionString, setConnectionString] = useState(
    localStorage.getItem('connectionString') || "duckdb:///C:/Users/mhard/PycharmProjects/sparvidata/backend/my_database.duckdb"
  );
  const [tableName, setTableName] = useState(localStorage.getItem('tableName') || "employees");
  const [activeTab, setActiveTab] = useState('overview');

  console.log("Dashboard rendered with:", { connectionString, tableName });

  // Define the refresh handler
  const handleRefresh = () => {
    console.log("Refresh button clicked");
    // Re-fetch data when the refresh button is clicked
    if (connectionString && tableName) {
      console.log("Refreshing data for:", { connectionString, tableName });
      setProfileData(null);
      directFetchProfile(connectionString, tableName)
        .then(data => {
          console.log("Refresh successful, data received:", data);
          setProfileData(data);
        })
        .catch(err => {
          console.error("Refresh failed:", err);
          setError(err.response?.data?.error || 'Failed to refresh data');
        });
    } else {
      console.log("Cannot refresh: missing connection string or table name");
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

  return (
    <div className="container-fluid mt-3">
      <div className="d-flex justify-content-between align-items-center mb-4">
        <h2>
          Sparvi Data Profiler
        </h2>
        <div>
          <button className="btn btn-outline-secondary" onClick={handleRefresh}>
            <i className="bi bi-arrow-clockwise me-1"></i> Refresh
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
                {/* Other overview cards... */}
              </div>
            </div>
          </div>
          {/* Rest of the component... */}
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