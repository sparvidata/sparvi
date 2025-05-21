import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClientProvider } from '@tanstack/react-query';
import { queryClient } from './api/queryClient';

// Contexts
import { AuthProvider } from './contexts/AuthContext';
import { UIProvider } from './contexts/UIContext';
import { ConnectionProvider } from './contexts/EnhancedConnectionContext';

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

// Anomaly pages
import AnomalyDashboardPage from './pages/Anomaly/AnomalyDashboardPage';
import AnomalyExplorerPage from './pages/Anomaly/components/AnomalyExplorerPage';
import AnomalyConfigsPage from './pages/Anomaly/components/AnomalyConfigsPage';
import AnomalyConfigForm from './pages/Anomaly/components/AnomalyConfigForm';

// Other pages
import AnalyticsPage from './pages/Analytics/AnalyticsPage';
import TableAnalyticsPage from './pages/Analytics/TableAnalyticsPage';
import SchemaChangesPage from './pages/Analytics/SchemaChangesPage';
import BusinessImpactPage from './pages/Analytics/BusinessImpactPage';
import AdminUsersPage from './pages/Admin/AdminUsersPage';
import AdminSettingsPage from './pages/Admin/AdminSettingsPage';
import UserSettingsPage from './pages/Settings/UserSettingsPage';
import NotFoundPage from './pages/NotFoundPage';

function App() {
  return (
    <QueryClientProvider client={queryClient}>
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

                  {/* Anomalies */}
                  <Route path="/anomalies" element={<AnomalyDashboardPage />} />
                  <Route path="/anomalies/:connectionId" element={<AnomalyDashboardPage />} />
                  <Route path="/anomalies/:connectionId/explorer" element={<AnomalyExplorerPage />} />
                  <Route path="/anomalies/:connectionId/configs" element={<AnomalyConfigsPage />} />
                  <Route path="/anomalies/:connectionId/configs/:configId" element={<AnomalyConfigForm />} />

                  {/* Analytics */}
                  <Route path="/analytics" element={<AnalyticsPage />} />
                  <Route path="/analytics/table/:connectionId/:tableName" element={<TableAnalyticsPage />} />
                  <Route path="/analytics/schema-changes/:connectionId" element={<SchemaChangesPage />} />
                  <Route path="/analytics/business-impact/:connectionId" element={<BusinessImpactPage />} />

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
    </QueryClientProvider>
  );
}

export default App;