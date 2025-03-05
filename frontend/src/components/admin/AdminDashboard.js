import React, { useState } from 'react';
import OrganizationManagement from './OrganizationManagement';
import UserManagement from './UserManagement';

function AdminDashboard() {
  // State to track active tab
  const [activeTab, setActiveTab] = useState('organization');

  return (
    <div className="container-fluid mt-3">
      <div className="d-flex justify-content-between align-items-center mb-4">
        <h2>
          <i className="bi bi-shield-lock me-2"></i>
          Admin Dashboard
        </h2>
      </div>

      {/* Navigation Tabs */}
      <ul className="nav nav-tabs mb-4">
        <li className="nav-item">
          <button
            className={`nav-link ${activeTab === 'organization' ? 'active' : ''}`}
            onClick={() => setActiveTab('organization')}
          >
            <i className="bi bi-building me-1"></i>
            Organization
          </button>
        </li>
        <li className="nav-item">
          <button
            className={`nav-link ${activeTab === 'users' ? 'active' : ''}`}
            onClick={() => setActiveTab('users')}
          >
            <i className="bi bi-people me-1"></i>
            Users
          </button>
        </li>
        <li className="nav-item">
          <button
            className={`nav-link ${activeTab === 'settings' ? 'active' : ''}`}
            onClick={() => setActiveTab('settings')}
          >
            <i className="bi bi-gear me-1"></i>
            Settings
          </button>
        </li>
      </ul>

      {/* Tab Content */}
      <div className="tab-content">
        {/* Organization Tab */}
        <div className={`tab-pane fade ${activeTab === 'organization' ? 'show active' : ''}`}>
          <OrganizationManagement />
        </div>

        {/* Users Tab */}
        <div className={`tab-pane fade ${activeTab === 'users' ? 'show active' : ''}`} id="userManagementSection">
          <UserManagement />
        </div>

        {/* Settings Tab */}
        <div className={`tab-pane fade ${activeTab === 'settings' ? 'show active' : ''}`}>
          <div className="card mb-4 shadow-sm">
            <div className="card-header bg-light">
              <h5 className="mb-0">
                <i className="bi bi-gear me-2"></i>
                Advanced Settings
              </h5>
            </div>
            <div className="card-body">
              <div className="alert alert-info">
                <i className="bi bi-info-circle me-2"></i>
                Advanced organization settings will be available in a future release.
              </div>

              <div className="row">
                <div className="col-md-6">
                  <div className="card border mb-3">
                    <div className="card-body">
                      <h6 className="card-title d-flex align-items-center">
                        <i className="bi bi-bell me-2"></i>
                        Notification Settings
                      </h6>
                      <p className="card-text small">
                        Configure organization-wide notification preferences
                      </p>
                      <button className="btn btn-sm btn-outline-secondary" disabled>
                        Coming Soon
                      </button>
                    </div>
                  </div>
                </div>

                <div className="col-md-6">
                  <div className="card border mb-3">
                    <div className="card-body">
                      <h6 className="card-title d-flex align-items-center">
                        <i className="bi bi-graph-up me-2"></i>
                        Usage & Billing
                      </h6>
                      <p className="card-text small">
                        Manage subscription, view usage statistics, and billing history
                      </p>
                      <button className="btn btn-sm btn-outline-secondary" disabled>
                        Coming Soon
                      </button>
                    </div>
                  </div>
                </div>

                <div className="col-md-6">
                  <div className="card border mb-3">
                    <div className="card-body">
                      <h6 className="card-title d-flex align-items-center">
                        <i className="bi bi-shield-lock me-2"></i>
                        Security Settings
                      </h6>
                      <p className="card-text small">
                        Configure password policies, MFA, and security controls
                      </p>
                      <button className="btn btn-sm btn-outline-secondary" disabled>
                        Coming Soon
                      </button>
                    </div>
                  </div>
                </div>

                <div className="col-md-6">
                  <div className="card border mb-3">
                    <div className="card-body">
                      <h6 className="card-title d-flex align-items-center">
                        <i className="bi bi-globe me-2"></i>
                        API & Integrations
                      </h6>
                      <p className="card-text small">
                        Manage API keys and third-party service integrations
                      </p>
                      <button className="btn btn-sm btn-outline-secondary" disabled>
                        Coming Soon
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default AdminDashboard;