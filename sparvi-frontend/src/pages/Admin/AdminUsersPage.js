import React from 'react';
import { Navigate } from 'react-router-dom';

// Redirect legacy admin users route to new admin layout
const AdminUsersPage = () => {
  return <Navigate to="/admin/users" replace />;
};

export default AdminUsersPage;