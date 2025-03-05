// frontend/src/components/admin/OrganizationManagement.js
import React, { useState, useEffect } from 'react';
import { fetchOrganization, updateOrganization } from '../../api';

function OrganizationManagement() {
  const [organization, setOrganization] = useState(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [editMode, setEditMode] = useState(false);

  // Form state
  const [formData, setFormData] = useState({
    name: '',
    logo_url: ''
  });

  // Load organization details on component mount
  useEffect(() => {
    loadOrganization();
  }, []);

  // Function to load/refresh organization details
  const loadOrganization = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await fetchOrganization();
      setOrganization(data);

      // Initialize form with organization data
      setFormData({
        name: data.name || '',
        logo_url: data.logo_url || ''
      });
    } catch (err) {
      setError('Failed to load organization details: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // Handle input changes
  const handleInputChange = (e) => {
    const { name, value } = e.target;
    setFormData({
      ...formData,
      [name]: value
    });
  };

  // Start editing
  const handleStartEdit = () => {
    setEditMode(true);
  };

  // Cancel editing
  const handleCancelEdit = () => {
    setEditMode(false);
    // Reset form data to original values
    setFormData({
      name: organization?.name || '',
      logo_url: organization?.logo_url || ''
    });
  };

  // Submit form
  const handleSubmit = async (e) => {
    e.preventDefault();
    setSuccess(null);
    setError(null);

    try {
      setSaving(true);
      await updateOrganization(formData);
      setSuccess('Organization details updated successfully');

      // Refresh organization data
      await loadOrganization();

      // Exit edit mode
      setEditMode(false);
    } catch (err) {
      setError('Failed to update organization: ' + (err.response?.data?.error || err.message));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header bg-light">
        <h5 className="mb-0">
          <i className="bi bi-building me-2"></i>
          Organization Management
        </h5>
      </div>
      <div className="card-body">
        {error && (
          <div className="alert alert-danger alert-dismissible fade show" role="alert">
            <i className="bi bi-exclamation-triangle-fill me-2"></i>
            {error}
            <button type="button" className="btn-close" data-bs-dismiss="alert" aria-label="Close" onClick={() => setError(null)}></button>
          </div>
        )}

        {success && (
          <div className="alert alert-success alert-dismissible fade show" role="alert">
            <i className="bi bi-check-circle-fill me-2"></i>
            {success}
            <button type="button" className="btn-close" data-bs-dismiss="alert" aria-label="Close" onClick={() => setSuccess(null)}></button>
          </div>
        )}

        {loading ? (
          <div className="d-flex justify-content-center my-5">
            <div className="spinner-border text-primary" role="status">
              <span className="visually-hidden">Loading...</span>
            </div>
          </div>
        ) : (
          <>
            {!editMode ? (
              <div className="d-flex flex-column flex-md-row align-items-md-center mb-3">
                <div className="me-md-4 mb-3 mb-md-0">
                  {organization?.logo_url ? (
                    <img
                      src={organization.logo_url}
                      alt={`${organization.name} logo`}
                      className="rounded-circle"
                      style={{ width: '80px', height: '80px', objectFit: 'cover' }}
                    />
                  ) : (
                    <div
                      className="d-flex align-items-center justify-content-center bg-light rounded-circle"
                      style={{ width: '80px', height: '80px' }}
                    >
                      <i className="bi bi-building" style={{ fontSize: '2rem' }}></i>
                    </div>
                  )}
                </div>

                <div className="flex-grow-1">
                  <h2 className="h4 mb-1">{organization?.name || 'Your Organization'}</h2>
                  <p className="text-muted small mb-2">Created: {organization?.created_at ? new Date(organization.created_at).toLocaleDateString() : 'Unknown'}</p>

                  <button
                    className="btn btn-sm btn-outline-primary"
                    onClick={handleStartEdit}
                  >
                    <i className="bi bi-pencil me-1"></i>
                    Edit Details
                  </button>
                </div>
              </div>
            ) : (
              <form onSubmit={handleSubmit}>
                <div className="mb-3">
                  <label htmlFor="name" className="form-label">Organization Name*</label>
                  <input
                    type="text"
                    className="form-control"
                    id="name"
                    name="name"
                    value={formData.name}
                    onChange={handleInputChange}
                    required
                  />
                </div>

                <div className="mb-3">
                  <label htmlFor="logo_url" className="form-label">Logo URL</label>
                  <input
                    type="url"
                    className="form-control"
                    id="logo_url"
                    name="logo_url"
                    value={formData.logo_url}
                    onChange={handleInputChange}
                    placeholder="https://example.com/logo.png"
                  />
                  <div className="form-text">
                    Enter a URL to your organization's logo (optional)
                  </div>
                </div>

                <div className="d-flex">
                  <button type="submit" className="btn btn-primary me-2" disabled={saving}>
                    {saving ? (
                      <>
                        <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                        Saving...
                      </>
                    ) : (
                      <>
                        <i className="bi bi-save me-1"></i>
                        Save Changes
                      </>
                    )}
                  </button>

                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={handleCancelEdit}
                    disabled={saving}
                  >
                    Cancel
                  </button>
                </div>
              </form>
            )}

            <hr className="my-4" />

            <div className="row mb-2">
              <div className="col-md-6">
                <div className="card border">
                  <div className="card-body">
                    <h5 className="card-title">
                      <i className="bi bi-people me-2"></i>
                      Members
                    </h5>
                    <p className="card-text">
                      Manage users and invitations for your organization
                    </p>
                    <button
                      className="btn btn-sm btn-outline-primary"
                      onClick={() => document.getElementById('userManagementSection').scrollIntoView({ behavior: 'smooth' })}
                    >
                      <i className="bi bi-person-gear me-1"></i>
                      Manage Users
                    </button>
                  </div>
                </div>
              </div>

              <div className="col-md-6 mt-3 mt-md-0">
                <div className="card border">
                  <div className="card-body">
                    <h5 className="card-title">
                      <i className="bi bi-gear me-2"></i>
                      Settings
                    </h5>
                    <p className="card-text">
                      Configure organization-wide settings and preferences
                    </p>
                    <button
                      className="btn btn-sm btn-outline-primary disabled"
                      title="Coming soon"
                    >
                      <i className="bi bi-sliders me-1"></i>
                      Manage Settings
                    </button>
                  </div>
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default OrganizationManagement;