// frontend/src/components/auth/InvitePage.js
import React, { useState, useEffect } from 'react';
import { useSearchParams, Link, useNavigate } from 'react-router-dom';
import { supabase } from '../../lib/supabase';
import './AuthPages.css';

function InvitePage() {
  const [searchParams] = useSearchParams();
  const token = searchParams.get('token');
  const navigate = useNavigate();

  const [inviteData, setInviteData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [formData, setFormData] = useState({
    email: '',
    password: '',
    confirmPassword: '',
    firstName: '',
    lastName: ''
  });
  const [showPassword, setShowPassword] = useState(false);
  const [validationError, setValidationError] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [success, setSuccess] = useState(false);

  // Load invite data on component mount
  useEffect(() => {
    if (!token) {
      setError("Invalid invitation link. The invite token is missing.");
      setLoading(false);
      return;
    }

    fetchInviteData(token);
  }, [token]);

  // Fetch invite data from the token
  const fetchInviteData = async (token) => {
    try {
      // Fetch the invite details from Supabase
      const { data, error } = await supabase
        .from('user_invites')
        .select('*')
        .eq('invite_token', token)
        .is('accepted_at', null)
        .gt('expires_at', new Date().toISOString())
        .single();

      if (error) throw error;

      if (!data) {
        setError("This invitation is invalid or has expired.");
        setLoading(false);
        return;
      }

      // Set the invite data and prefill form
      setInviteData(data);
      setFormData({
        ...formData,
        email: data.email,
        firstName: data.first_name || '',
        lastName: data.last_name || ''
      });
      setLoading(false);
    } catch (err) {
      console.error("Error fetching invite data:", err);
      setError("Failed to load invitation details. The link may be invalid or expired.");
      setLoading(false);
    }
  };

  // Toggle password visibility
  const togglePasswordVisibility = () => {
    setShowPassword(!showPassword);
  };

  // Handle input changes
  const handleInputChange = (e) => {
    const { name, value } = e.target;
    setFormData({
      ...formData,
      [name]: value
    });
  };

  // Validate password
  const validatePassword = (password) => {
    if (password.length < 8) {
      return 'Password must be at least 8 characters long';
    }

    const hasNumber = /\d/.test(password);
    const hasLetter = /[a-zA-Z]/.test(password);

    if (!hasNumber || !hasLetter) {
      return 'Password must include at least one number and one letter';
    }

    return '';
  };

  // Handle form submission
  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setValidationError('');

    // Basic validation
    if (!formData.password) {
      setValidationError('Password is required');
      return;
    }

    // Validate password
    const passwordError = validatePassword(formData.password);
    if (passwordError) {
      setValidationError(passwordError);
      return;
    }

    // Check if passwords match
    if (formData.password !== formData.confirmPassword) {
      setValidationError('Passwords do not match');
      return;
    }

    setIsSubmitting(true);

    try {
      // 1. Sign up the user with Supabase Auth
      const { data: authData, error: authError } = await supabase.auth.signUp({
        email: formData.email,
        password: formData.password,
        options: {
          data: {
            first_name: formData.firstName,
            last_name: formData.lastName
          }
        }
      });

      if (authError) throw authError;

      // 2. Update the profile with organization data
      const { error: profileError } = await supabase
        .from('profiles')
        .upsert({
          id: authData.user.id,
          email: formData.email,
          first_name: formData.firstName,
          last_name: formData.lastName,
          organization_id: inviteData.organization_id,
          role: inviteData.role
        });

      if (profileError) throw profileError;

      // 3. Mark the invite as accepted
      const { error: inviteError } = await supabase
        .from('user_invites')
        .update({
          accepted_at: new Date().toISOString(),
          accepted_by: authData.user.id
        })
        .eq('invite_token', token);

      if (inviteError) throw inviteError;

      // Show success state
      setSuccess(true);

      // Redirect to dashboard after a delay
      setTimeout(() => {
        // Sign in the user automatically
        supabase.auth.signInWithPassword({
          email: formData.email,
          password: formData.password
        }).then(() => {
          navigate('/dashboard');
        });
      }, 3000);

    } catch (err) {
      console.error("Error accepting invite:", err);
      setError(err.message || 'Failed to accept invitation');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="auth-container">
      <div className="auth-card">
        {loading ? (
          <div className="text-center my-5">
            <div className="spinner-border text-primary" role="status">
              <span className="visually-hidden">Loading...</span>
            </div>
            <p className="mt-3">Loading invitation details...</p>
          </div>
        ) : error ? (
          <div className="auth-error">
            <div className="error-icon">
              <svg viewBox="0 0 24 24" width="24" height="24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M6 18L18 6M6 6l12 12" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" stroke="currentColor" />
              </svg>
            </div>
            <h2>Invitation Error</h2>
            <p>{error}</p>
            <Link to="/login" className="primary-button">
              Go to Login
            </Link>
          </div>
        ) : success ? (
          <div className="auth-success">
            <div className="success-icon">
              <svg viewBox="0 0 24 24" width="24" height="24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M5 13l4 4L19 7" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" stroke="currentColor" />
              </svg>
            </div>
            <h2>Invitation Accepted!</h2>
            <p>
              Welcome to {inviteData?.organization_name || "Sparvi"}!
              Your account has been created successfully.
            </p>
            <p>
              You will be redirected to the dashboard shortly.
            </p>
          </div>
        ) : (
          <>
            <div className="auth-header">
              <h1>Accept Invitation</h1>
              <p>
                You've been invited to join an organization in Sparvi.
                Create your account to get started.
              </p>
            </div>

            {/* Invite details */}
            <div className="alert alert-info mb-4">
              <div className="d-flex">
                <div>
                  <i className="bi bi-envelope-check me-2"></i>
                </div>
                <div>
                  <strong>Invitation details:</strong>
                  <p className="mb-0">
                    You've been invited to join as a <strong>{inviteData?.role || 'member'}</strong>.
                  </p>
                </div>
              </div>
            </div>

            {/* Form for accepting invite */}
            <form onSubmit={handleSubmit} className="auth-form">
              <div className="form-group">
                <label htmlFor="email" className="form-label">Email address</label>
                <input
                  id="email"
                  name="email"
                  type="email"
                  autoComplete="email"
                  value={formData.email}
                  onChange={handleInputChange}
                  disabled  // Email is pre-filled and can't be changed
                  className="form-control"
                />
              </div>

              <div className="row">
                <div className="col-md-6">
                  <div className="form-group">
                    <label htmlFor="firstName" className="form-label">First name</label>
                    <input
                      id="firstName"
                      name="firstName"
                      type="text"
                      autoComplete="given-name"
                      value={formData.firstName}
                      onChange={handleInputChange}
                      className="form-control"
                    />
                  </div>
                </div>

                <div className="col-md-6">
                  <div className="form-group">
                    <label htmlFor="lastName" className="form-label">Last name</label>
                    <input
                      id="lastName"
                      name="lastName"
                      type="text"
                      autoComplete="family-name"
                      value={formData.lastName}
                      onChange={handleInputChange}
                      className="form-control"
                    />
                  </div>
                </div>
              </div>

              <div className="form-group">
                <label htmlFor="password" className="form-label">Password*</label>
                <div className="password-input">
                  <input
                    id="password"
                    name="password"
                    type={showPassword ? "text" : "password"}
                    autoComplete="new-password"
                    value={formData.password}
                    onChange={handleInputChange}
                    className={`form-control ${validationError ? 'input-error' : ''}`}
                    required
                  />
                  <button
                    type="button"
                    onClick={togglePasswordVisibility}
                    className="toggle-password"
                  >
                    {showPassword ? (
                      <svg viewBox="0 0 24 24" width="24" height="24" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <path d="M13.875 18.825A10.05 10.05 0 0112 19c-4.478 0-8.268-2.943-9.543-7a9.97 9.97 0 011.563-3.029m5.858.908a3 3 0 114.243 4.243M9.878 9.878l4.242 4.242M9.88 9.88l-3.29-3.29m7.532 7.532l3.29 3.29M3 3l3.59 3.59m0 0A9.953 9.953 0 0112 5c4.478 0 8.268 2.943 9.543 7a10.025 10.025 0 01-4.132 5.411m0 0L21 21"
                          strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" stroke="currentColor" />
                      </svg>
                    ) : (
                      <svg viewBox="0 0 24 24" width="24" height="24" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <path d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"
                          strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" stroke="currentColor" />
                        <path d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"
                          strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" stroke="currentColor" />
                      </svg>
                    )}
                  </button>
                </div>
                <div className="form-text">
                  Password must be at least 8 characters and include at least one number and one letter
                </div>
              </div>

              <div className="form-group">
                <label htmlFor="confirmPassword" className="form-label">Confirm password*</label>
                <input
                  id="confirmPassword"
                  name="confirmPassword"
                  type={showPassword ? "text" : "password"}
                  autoComplete="new-password"
                  value={formData.confirmPassword}
                  onChange={handleInputChange}
                  className={`form-control ${validationError ? 'input-error' : ''}`}
                  required
                />
              </div>

              {/* Validation Error */}
              {validationError && (
                <div className="form-error">
                  {validationError}
                </div>
              )}

              <button
                type="submit"
                disabled={isSubmitting}
                className="submit-button"
              >
                {isSubmitting ? (
                  <>
                    <svg className="spinner" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                      <circle className="spinner-track" cx="12" cy="12" r="10" />
                      <circle className="spinner-path" cx="12" cy="12" r="10" />
                    </svg>
                    Creating account...
                  </>
                ) : (
                  'Accept Invitation'
                )}
              </button>
            </form>

            <div className="auth-footer">
              <p>
                Already have an account?{' '}
                <Link to="/login" className="auth-link">
                  Sign in
                </Link>
              </p>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default InvitePage;