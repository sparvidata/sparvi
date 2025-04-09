import React, { useEffect } from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';

/**
 * ProtectedRoute component that requires authentication to access
 * Redirects to login if user is not authenticated
 */
const ProtectedRoute = ({ children }) => {
  const { user, loading, isAuthenticated, updateActivity, session } = useAuth();
  const location = useLocation();

  // Check session on route access and update activity
  useEffect(() => {
    if (isAuthenticated) {
      updateActivity();
    }
  }, [isAuthenticated, updateActivity, location.pathname]);

  // Check token expiration
  useEffect(() => {
    if (session && session.expires_at) {
      const now = Math.floor(Date.now() / 1000);
      const expiresAt = session.expires_at;

      if (now >= expiresAt) {
        console.log('Session expired in protected route check');
        return <Navigate to="/login" state={{ from: location }} replace />;
      }
    }
  }, [session, location]);

  // Show loading state while authentication is being checked
  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-primary-600"></div>
      </div>
    );
  }

  // Redirect to login if not authenticated
  if (!user) {
    return <Navigate to="/login" state={{ from: location }} replace />;
  }

  // Render children if authenticated
  return children;
};

export default ProtectedRoute;