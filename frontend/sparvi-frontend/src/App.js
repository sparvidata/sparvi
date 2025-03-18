import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';

// Contexts
import { AuthProvider } from './contexts/AuthContext';
import { UIProvider } from './contexts/UIContext';
import { ConnectionProvider } from './contexts/ConnectionContext';

// Auth pages
import LoginPage from './pages/Login/LoginPage';
import RegisterPage from './pages/Login/RegisterPage';
import ForgotPasswordPage from './pages/Login/ForgotPasswordPage';
import ResetPasswordPage from './pages/Login/ResetPasswordPage';

// Layout
import MainLayout from './components/layout/MainLayout';
import ProtectedRoute from './components/auth/ProtectedRoute';

// Pages
import DashboardPage from './pages/Dashboard/DashboardPage';
import ConnectionsPage from './pages/Connections/ConnectionsPage';
import ConnectionDetailPage from './pages/Connections/ConnectionDetailPage';
import NewConnectionPage from './pages/Connections/NewConnectionPage';
import DataExplorerPage from './pages/DataExplorer/DataExplorerPage';
import TableDetailPage from './pages/DataExplorer/TableDetailPage';
import ValidationPage from './pages/Validations/ValidationPage';
import MetadataPage from './pages/Metadata/MetadataPage';
import AdminUsersPage from './pages/Admin/AdminUsersPage';
import AdminSettingsPage from './pages/Admin/AdminSettingsPage';
import UserSettingsPage from './pages/Settings/UserSettingsPage';
import NotFoundPage from './pages/NotFoundPage';

function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <UIProvider>
          <ConnectionProvider>
            <Routes>
              {/* Public routes */}
              <Route path="/login" element={<LoginPage />} />
              <Route path="/register" element={<RegisterPage />} />
              <Route path="/forgot-password" element={<ForgotPasswordPage />} />
              <Route path="/reset-password" element={<ResetPasswordPage />} />

              {/* Protected routes */}
              <Route element={
                <ProtectedRoute>
                  <MainLayout />
                </ProtectedRoute>
              }>
                {/* Dashboard */}
                <Route path="/dashboard" element={<DashboardPage />} />

                {/* Connections */}
                <Route path="/connections" element={<ConnectionsPage />} />
                <Route path="/connections/new" element={<NewConnectionPage />} />
                <Route path="/connections/:id" element={<ConnectionDetailPage />} />

                {/* Data Explorer */}
                <Route path="/explorer" element={<DataExplorerPage />} />
                <Route path="/explorer/:connectionId/tables/:tableName" element={<TableDetailPage />} />

                {/* Validations */}
                <Route path="/validations" element={<ValidationPage />} />

                {/* Metadata */}
                <Route path="/metadata" element={<MetadataPage />} />

                {/* Admin */}
                <Route path="/admin/users" element={<AdminUsersPage />} />
                <Route path="/admin/settings" element={<AdminSettingsPage />} />
                <Route path="/admin" element={<Navigate to="/admin/users" replace />} />

                {/* Settings */}
                <Route path="/settings" element={<UserSettingsPage />} />

                {/* Root redirect */}
                <Route path="/" element={<Navigate to="/dashboard" replace />} />

                {/* 404 */}
                <Route path="*" element={<NotFoundPage />} />
              </Route>
            </Routes>
          </ConnectionProvider>
        </UIProvider>
      </AuthProvider>
    </BrowserRouter>
  );
}

export default App;