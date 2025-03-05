// frontend/src/components/admin/UserManagement.js
import React, { useState, useEffect } from 'react';
import {
  fetchAdminUsers,
  inviteUser,
  updateAdminUser,
  removeUser
} from '../../api';

function UserManagement() {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);

  // State for user invite form
  const [inviteForm, setInviteForm] = useState({
    email: '',
    first_name: '',
    last_name: '',
    role: 'member'
  });

  // State for editing a user
  const [editingUser, setEditingUser] = useState(null);
  const [editForm, setEditForm] = useState({
    first_name: '',
    last_name: '',
    role: 'member'
  });

  // Load users on component mount
  useEffect(() => {
    loadUsers();
  }, []);

  // Function to load/refresh users
  const loadUsers = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await fetchAdminUsers();
      setUsers(data);
    } catch (err) {
      setError('Failed to load users: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // Handle input changes for invite form
  const handleInviteInputChange = (e) => {
    const { name, value } = e.target;
    setInviteForm({
      ...inviteForm,
      [name]: value
    });
  };

  // Handle input changes for edit form
  const handleEditInputChange = (e) => {
    const { name, value } = e.target;
    setEditForm({
      ...editForm,
      [name]: value
    });
  };

  // Start editing a user
  const startEditing = (user) => {
    setEditingUser(user);
    setEditForm({
      first_name: user.first_name || '',
      last_name: user.last_name || '',
      role: user.role || 'member'
    });
  };

  // Cancel editing
  const cancelEditing = () => {
    setEditingUser(null);
    setEditForm({
      first_name: '',
      last_name: '',
      role: 'member'
    });
  };

  // Submit invite form
  const handleInviteSubmit = async (e) => {
    e.preventDefault();
    setSuccess(null);
    setError(null);

    if (!inviteForm.email) {
      setError('Email is required');
      return;
    }

    try {
      setLoading(true);
      const result = await inviteUser(inviteForm);
      setSuccess(`Invitation sent to ${inviteForm.email}`);

      // Reset form
      setInviteForm({
        email: '',
        first_name: '',
        last_name: '',
        role: 'member'
      });

      // Copy invite URL to clipboard
      if (result.invite?.invite_url) {
        navigator.clipboard.writeText(result.invite.invite_url)
          .then(() => {
            setSuccess(`Invitation sent to ${inviteForm.email} and link copied to clipboard`);
          })
          .catch(() => {
            // If clipboard copy fails, just show the link
            setSuccess(`Invitation sent to ${inviteForm.email}. Link: ${result.invite.invite_url}`);
          });
      }

      // Refresh user list
      loadUsers();
    } catch (err) {
      setError('Failed to send invitation: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // Submit edit form
  const handleEditSubmit = async (e) => {
    e.preventDefault();
    setSuccess(null);
    setError(null);

    if (!editingUser) return;

    try {
      setLoading(true);
      await updateAdminUser(editingUser.id, editForm);
      setSuccess(`Updated user ${editingUser.email}`);

      // Refresh user list
      loadUsers();

      // Exit edit mode
      cancelEditing();
    } catch (err) {
      setError('Failed to update user: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // Remove a user
  const handleRemoveUser = async (userId, email) => {
    if (!window.confirm(`Are you sure you want to remove ${email} from your organization?`)) {
      return;
    }

    setSuccess(null);
    setError(null);

    try {
      setLoading(true);
      await removeUser(userId);
      setSuccess(`Removed user ${email} from the organization`);

      // Refresh user list
      loadUsers();
    } catch (err) {
      setError('Failed to remove user: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header bg-light d-flex justify-content-between align-items-center">
        <h5 className="mb-0">
          <i className="bi bi-people me-2"></i>
          User Management
        </h5>
        <button
          className="btn btn-sm btn-primary"
          data-bs-toggle="modal"
          data-bs-target="#inviteUserModal"
        >
          <i className="bi bi-person-plus me-1"></i>
          Invite User
        </button>
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

        {loading && !users.length ? (
          <div className="d-flex justify-content-center my-5">
            <div className="spinner-border text-primary" role="status">
              <span className="visually-hidden">Loading...</span>
            </div>
          </div>
        ) : (
          <div className="table-responsive">
            <table className="table table-hover">
              <thead>
                <tr>
                  <th>Email</th>
                  <th>Name</th>
                  <th>Role</th>
                  <th>Joined</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {users.map(user => (
                  <tr key={user.id}>
                    {editingUser?.id === user.id ? (
                      <td colSpan="4">
                        <form onSubmit={handleEditSubmit} className="row g-2">
                          <div className="col-md-3">
                            <input
                              type="text"
                              className="form-control form-control-sm"
                              name="first_name"
                              value={editForm.first_name}
                              onChange={handleEditInputChange}
                              placeholder="First Name"
                            />
                          </div>
                          <div className="col-md-3">
                            <input
                              type="text"
                              className="form-control form-control-sm"
                              name="last_name"
                              value={editForm.last_name}
                              onChange={handleEditInputChange}
                              placeholder="Last Name"
                            />
                          </div>
                          <div className="col-md-3">
                            <select
                              className="form-select form-select-sm"
                              name="role"
                              value={editForm.role}
                              onChange={handleEditInputChange}
                            >
                              <option value="admin">Admin</option>
                              <option value="member">Member</option>
                            </select>
                          </div>
                          <div className="col-md-3">
                            <div className="btn-group btn-group-sm">
                              <button type="submit" className="btn btn-success" disabled={loading}>
                                <i className="bi bi-check-lg"></i>
                              </button>
                              <button type="button" className="btn btn-secondary" onClick={cancelEditing}>
                                <i className="bi bi-x-lg"></i>
                              </button>
                            </div>
                          </div>
                        </form>
                      </td>
                    ) : (
                      <>
                        <td>{user.email}</td>
                        <td>{user.first_name ? `${user.first_name} ${user.last_name || ''}` : 'Not set'}</td>
                        <td>
                          <span className={`badge bg-${user.role === 'admin' ? 'danger' : 'info'}`}>
                            {user.role || 'member'}
                          </span>
                        </td>
                        <td>{new Date(user.created_at).toLocaleDateString()}</td>
                      </>
                    )}
                    <td>
                      {!editingUser && (
                        <div className="btn-group btn-group-sm">
                          <button
                            className="btn btn-outline-primary"
                            onClick={() => startEditing(user)}
                            title="Edit user"
                          >
                            <i className="bi bi-pencil"></i>
                          </button>
                          <button
                            className="btn btn-outline-danger"
                            onClick={() => handleRemoveUser(user.id, user.email)}
                            title="Remove user"
                            disabled={loading}
                          >
                            <i className="bi bi-person-x"></i>
                          </button>
                        </div>
                      )}
                    </td>
                  </tr>
                ))}
                {users.length === 0 && (
                  <tr>
                    <td colSpan="5" className="text-center py-3">
                      <i className="bi bi-info-circle me-2"></i>
                      No users found in your organization
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Invite User Modal */}
      <div className="modal fade" id="inviteUserModal" tabIndex="-1" aria-labelledby="inviteUserModalLabel" aria-hidden="true">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <h5 className="modal-title" id="inviteUserModalLabel">Invite New User</h5>
              <button type="button" className="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <form onSubmit={handleInviteSubmit}>
              <div className="modal-body">
                <div className="mb-3">
                  <label htmlFor="email" className="form-label">Email Address*</label>
                  <input
                    type="email"
                    className="form-control"
                    id="email"
                    name="email"
                    value={inviteForm.email}
                    onChange={handleInviteInputChange}
                    required
                  />
                </div>

                <div className="mb-3">
                  <label htmlFor="first_name" className="form-label">First Name</label>
                  <input
                    type="text"
                    className="form-control"
                    id="first_name"
                    name="first_name"
                    value={inviteForm.first_name}
                    onChange={handleInviteInputChange}
                  />
                </div>

                <div className="mb-3">
                  <label htmlFor="last_name" className="form-label">Last Name</label>
                  <input
                    type="text"
                    className="form-control"
                    id="last_name"
                    name="last_name"
                    value={inviteForm.last_name}
                    onChange={handleInviteInputChange}
                  />
                </div>

                <div className="mb-3">
                  <label htmlFor="role" className="form-label">Role</label>
                  <select
                    className="form-select"
                    id="role"
                    name="role"
                    value={inviteForm.role}
                    onChange={handleInviteInputChange}
                  >
                    <option value="member">Member</option>
                    <option value="admin">Admin</option>
                  </select>
                  <div className="form-text">
                    Admins can manage users and organization settings.
                  </div>
                </div>
              </div>
              <div className="modal-footer">
                <button type="button" className="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                <button type="submit" className="btn btn-primary" disabled={loading}>
                  {loading ? (
                    <>
                      <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                      Sending...
                    </>
                  ) : (
                    <>
                      <i className="bi bi-envelope me-1"></i>
                      Send Invitation
                    </>
                  )}
                </button>
              </div>
            </form>
          </div>
        </div>
      </div>
    </div>
  );
}

export default UserManagement;