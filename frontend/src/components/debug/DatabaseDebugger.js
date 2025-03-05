// src/components/debug/DatabaseDebugger.js
import React, { useState, useEffect } from 'react';
import { supabase } from '../../lib/supabase';

function DatabaseDebugger() {
  const [userId, setUserId] = useState(null);
  const [userEmail, setUserEmail] = useState(null);
  const [sessionInfo, setSessionInfo] = useState(null);
  const [profileData, setProfileData] = useState(null);
  const [profileError, setProfileError] = useState(null);
  const [rawResult, setRawResult] = useState(null);
  const [loading, setLoading] = useState(true);
  const [actionStatus, setActionStatus] = useState(null);
  const [supabaseConfig, setSupabaseConfig] = useState({
    url: process.env.REACT_APP_SUPABASE_URL || "Not found in env",
    key: process.env.REACT_APP_SUPABASE_ANON_KEY ? "Key exists" : "Key not found in env"
  });

  useEffect(() => {
    async function fetchData() {
      try {
        setLoading(true);

        // Get current session
        const { data: { session } } = await supabase.auth.getSession();
        setSessionInfo({
          exists: !!session,
          expiresAt: session?.expires_at ? new Date(session.expires_at * 1000).toLocaleString() : 'N/A',
          tokenLength: session?.access_token?.length || 0
        });

        if (!session) {
          setLoading(false);
          return;
        }

        // Set user info
        setUserId(session.user.id);
        setUserEmail(session.user.email);

        // Try to get profile with multiple approaches to diagnose the issue

        // Approach 1: Simple select
        const { data: profileSimple, error: errorSimple } = await supabase
          .from('profiles')
          .select('*')
          .eq('id', session.user.id)
          .single();

        // Approach 2: Only select role
        const { data: profileRole, error: errorRole } = await supabase
          .from('profiles')
          .select('role')
          .eq('id', session.user.id)
          .single();

        // Approach 3: RPC call (direct SQL function)
        const { data: rpcResult, error: rpcError } = await supabase.rpc('get_user_role', {
          user_id: session.user.id
        });

        // Save all results for display and debugging
        setRawResult({
          approach1: { data: profileSimple, error: errorSimple },
          approach2: { data: profileRole, error: errorRole },
          approach3: { data: rpcResult, error: rpcError }
        });

        // Use approach 1 results for the main profile data
        setProfileData(profileSimple);
        setProfileError(errorSimple);

      } catch (error) {
        console.error("Debug component error:", error);
        setProfileError(error.message);
      } finally {
        setLoading(false);
      }
    }

    fetchData();
  }, []);

  const handleFixProfile = async () => {
    try {
      setLoading(true);
      setActionStatus(null);

      if (!userId) {
        throw new Error("No user ID available");
      }

      // If profile exists, update it
      if (profileData) {
        const { data, error } = await supabase
          .from('profiles')
          .update({
            role: 'admin',
            updated_at: new Date().toISOString()
          })
          .eq('id', userId)
          .select();

        if (error) throw error;

        setActionStatus({
          success: true,
          message: `Profile updated successfully. Role set to admin.`,
          data
        });

        // Refresh profile data
        const { data: refreshedData } = await supabase
          .from('profiles')
          .select('*')
          .eq('id', userId)
          .single();

        setProfileData(refreshedData);
      }
      // If profile doesn't exist, create it
      else {
        const { data, error } = await supabase
          .from('profiles')
          .insert({
            id: userId,
            email: userEmail,
            role: 'admin',
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
          })
          .select();

        if (error) throw error;

        setActionStatus({
          success: true,
          message: `Profile created successfully with admin role.`,
          data
        });

        // Set the new profile data
        setProfileData(data[0]);
      }
    } catch (error) {
      console.error("Fix profile error:", error);
      setActionStatus({
        success: false,
        message: `Error: ${error.message}`
      });
    } finally {
      setLoading(false);
    }
  };

  const directUpdate = async () => {
    try {
      setLoading(true);
      setActionStatus(null);

      // Run direct SQL query using RPC
      // Note: You need to define this function in Supabase
      const { data, error } = await supabase.rpc('force_update_role', {
        target_user_id: userId,
        new_role: 'admin'
      });

      if (error) throw error;

      setActionStatus({
        success: true,
        message: `Direct database update performed. Check if it worked by refreshing.`,
        data
      });

      // Refresh the page after 2 seconds
      setTimeout(() => {
        window.location.reload();
      }, 2000);

    } catch (error) {
      console.error("Direct update error:", error);
      setActionStatus({
        success: false,
        message: `Error with direct update: ${error.message}`
      });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="container mt-4">
      <div className="card shadow-sm mb-4">
        <div className="card-header bg-primary text-white">
          <h4 className="mb-0">Database Debugger</h4>
        </div>
        <div className="card-body">
          {loading ? (
            <div className="d-flex justify-content-center my-5">
              <div className="spinner-border text-primary" role="status">
                <span className="visually-hidden">Loading...</span>
              </div>
            </div>
          ) : (
            <>
              <div className="alert alert-info">
                <strong>How to use:</strong> This tool shows details about your Supabase connection, user session, and profile data. Use the buttons below to fix profile issues.
              </div>

              <h5 className="mb-3">Supabase Configuration</h5>
              <div className="mb-4">
                <div className="card">
                  <ul className="list-group list-group-flush">
                    <li className="list-group-item d-flex justify-content-between align-items-center">
                      <span>Supabase URL</span>
                      <code>{supabaseConfig.url}</code>
                    </li>
                    <li className="list-group-item d-flex justify-content-between align-items-center">
                      <span>Anon Key Status</span>
                      <span>{supabaseConfig.key}</span>
                    </li>
                  </ul>
                </div>
              </div>

              <h5 className="mb-3">Session Information</h5>
              {!sessionInfo?.exists ? (
                <div className="alert alert-warning">
                  No active session found. Please log in first.
                </div>
              ) : (
                <div className="mb-4">
                  <div className="card">
                    <ul className="list-group list-group-flush">
                      <li className="list-group-item d-flex justify-content-between align-items-center">
                        <span>User ID</span>
                        <code>{userId}</code>
                      </li>
                      <li className="list-group-item d-flex justify-content-between align-items-center">
                        <span>Email</span>
                        <span>{userEmail}</span>
                      </li>
                      <li className="list-group-item d-flex justify-content-between align-items-center">
                        <span>Token Expiry</span>
                        <span>{sessionInfo.expiresAt}</span>
                      </li>
                      <li className="list-group-item d-flex justify-content-between align-items-center">
                        <span>Token Length</span>
                        <span>{sessionInfo.tokenLength} characters</span>
                      </li>
                    </ul>
                  </div>
                </div>
              )}

              <h5 className="mb-3">Profile Data</h5>
              {profileError ? (
                <div className="alert alert-danger">
                  <strong>Error fetching profile:</strong> {profileError.message || String(profileError)}
                </div>
              ) : !profileData ? (
                <div className="alert alert-warning">
                  No profile data found for this user.
                </div>
              ) : (
                <div className="mb-4">
                  <div className="card">
                    <div className="card-header">
                      <strong>Profile Fields</strong>
                    </div>
                    <ul className="list-group list-group-flush">
                      {Object.entries(profileData).map(([key, value]) => (
                        <li key={key} className="list-group-item d-flex justify-content-between align-items-center">
                          <span>{key}</span>
                          <span>
                            {typeof value === 'object' ? JSON.stringify(value) : String(value)}
                          </span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              )}

              <h5 className="mb-3">Actions</h5>
              <div className="d-flex gap-2 mb-4">
                <button
                  className="btn btn-primary"
                  onClick={handleFixProfile}
                  disabled={loading}
                >
                  {loading ? 'Processing...' : 'Fix Profile (Standard)'}
                </button>
                <button
                  className="btn btn-danger"
                  onClick={directUpdate}
                  disabled={loading}
                >
                  {loading ? 'Processing...' : 'Emergency Fix (Direct SQL)'}
                </button>
                <button
                  className="btn btn-secondary"
                  onClick={() => window.location.reload()}
                >
                  Refresh Page
                </button>
              </div>

              {actionStatus && (
                <div className={`alert alert-${actionStatus.success ? 'success' : 'danger'}`}>
                  {actionStatus.message}
                  {actionStatus.data && (
                    <pre className="mt-2 bg-light p-2 rounded">
                      {JSON.stringify(actionStatus.data, null, 2)}
                    </pre>
                  )}
                </div>
              )}

              <h5 className="mb-3">Raw Results</h5>
              <div className="accordion" id="debugAccordion">
                <div className="accordion-item">
                  <h2 className="accordion-header" id="headingOne">
                    <button className="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#collapseOne">
                      View Raw Query Results
                    </button>
                  </h2>
                  <div id="collapseOne" className="accordion-collapse collapse" data-bs-parent="#debugAccordion">
                    <div className="accordion-body">
                      <pre className="bg-light p-3 rounded" style={{maxHeight: '500px', overflow: 'auto'}}>
                        {JSON.stringify(rawResult, null, 2)}
                      </pre>
                    </div>
                  </div>
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

export default DatabaseDebugger;