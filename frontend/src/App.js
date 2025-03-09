import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import AuthHandler from './auth/AuthHandler';
import Login from './components/auth/Login';
import SignupPage from './components/auth/SignupPage';
import ForgotPasswordPage from './components/auth/ForgotPassword';
import ResetPasswordPage from './components/auth/ResetPassword';
import Dashboard from './components/Dashboard';
import Documentation from './components/Documentation';
import Header from './components/Header';
import TestConnection from './components/TestConnection';
import './components/auth/AuthPages.css'
import AdminDashboard from './components/admin/AdminDashboard';
import InvitePage from './components/auth/InvitePage';
import DatabaseDebugger from './components/debug/DatabaseDebugger';
import './colors.css';

console.log("APP.JS LOADED");

function App() {
  const [session, setSession] = useState(null);
  const [loading, setLoading] = useState(true);
  const [checkingSession, setCheckingSession] = useState(false);

  // Function to refresh the session state
  const refreshSessionState = async () => {
    setCheckingSession(true);
    try {
      const currentSession = await AuthHandler.getSession();
      setSession(currentSession);
    } catch (error) {
      console.error("Error getting session:", error);
      setSession(null);
    } finally {
      setCheckingSession(false);
    }
  };

  useEffect(() => {
    // Initial load of session
    const loadInitialSession = async () => {
      await refreshSessionState();
      setLoading(false);

      // Set up session refresh if we have a session
      if (session) {
        AuthHandler.setupSessionRefresh();
      }
    };

    loadInitialSession();

    // Listen for auth changes
    const { data: { subscription } } = AuthHandler.supabase.auth.onAuthStateChange(
      async (event, session) => {
        console.log("Auth state changed:", event);
        setSession(session);

        if (event === 'SIGNED_IN') {
          AuthHandler.setupSessionRefresh();
        } else if (event === 'SIGNED_OUT') {
          AuthHandler.clearSessionRefresh();
        }
      }
    );

    // Clean up on unmount
    return () => {
      subscription?.unsubscribe();
      AuthHandler.clearSessionRefresh();
    };
  }, []); // Empty dependency array means this effect runs once on mount

  // Set up an effect to refresh the session state when it might have changed
  useEffect(() => {
    if (session && !checkingSession) {
      const sessionExpiresAt = session.expires_at * 1000; // Convert to milliseconds
      const timeUntilExpiry = sessionExpiresAt - Date.now();

      // If session is expired or about to expire, refresh it
      if (timeUntilExpiry < 60000) { // Less than a minute
        refreshSessionState();
      }
    }
  }, [session, checkingSession]);

  if (loading) {
    return (
      <div className="d-flex justify-content-center align-items-center" style={{ height: '100vh' }}>
        <div className="spinner-border text-primary" role="status">
          <span className="visually-hidden">Loading...</span>
        </div>
      </div>
    );
  }

  console.log("App: Rendering with session:", session);
  console.log("App: Routes will include admin route:", !!session);

  return (
    <Router>
      <Header session={session} onLogout={() => AuthHandler.signOut()} />
      <div className="container-fluid mt-3">
        <Routes>
          <Route path="/login" element={!session ? <Login /> : <Navigate to="/dashboard" />} />
          <Route path="/signup" element={!session ? <SignupPage /> : <Navigate to="/dashboard" />} />
          <Route path="/forgot-password" element={!session ? <ForgotPasswordPage /> : <Navigate to="/dashboard" />} />
          <Route path="/reset-password" element={!session ? <ResetPasswordPage /> : <Navigate to="/dashboard" />} />
          <Route path="/dashboard" element={session ? <Dashboard /> : <Navigate to="/login" />} />
          <Route path="/test-connection" element={session ? <TestConnection /> : <Navigate to="/login" />} />
          <Route path="/docs/:page" element={<Documentation />} />
          <Route path="/docs" element={<Navigate to="/docs/overview" />} />
          <Route path="/" element={<Navigate to={session ? "/dashboard" : "/login"} />} />
          <Route path="/admin" element={session ? <AdminDashboard /> : <Navigate to="/login" />} />
          <Route path="/invite" element={<InvitePage />} />
          <Route path="/debug-db" element={<DatabaseDebugger />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;