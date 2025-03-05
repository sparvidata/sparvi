import React, { useState, useEffect } from 'react';
import { Link, useNavigate, useLocation } from 'react-router-dom';
import AuthHandler from '../../auth/AuthHandler';
import './AuthPages.css';

function ResetPasswordPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const [token, setToken] = useState('');

  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState('');
  const [tokenValid, setTokenValid] = useState(true);
  const [validationError, setValidationError] = useState('');

  // Extract token from URL when component mounts
  useEffect(() => {
    // In Supabase, the token is usually in the hash part of the URL
    const hashParams = new URLSearchParams(location.hash.replace('#', ''));
    const accessToken = hashParams.get('access_token');

    if (accessToken) {
      setToken(accessToken);
      setTokenValid(true);
    } else {
      setTokenValid(false);
      setError('Invalid or expired password reset link. Please request a new one.');
    }
  }, [location]);

  // Toggle password visibility
  const togglePasswordVisibility = () => {
    setShowPassword(!showPassword);
  };

  // Validate password
  const validatePassword = (password) => {
    if (password.length < 8) {
      return 'Password must be at least 8 characters long';
    }

    // Check for at least one number and one letter
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

    // Validate password
    const passwordError = validatePassword(password);
    if (passwordError) {
      setValidationError(passwordError);
      return;
    }

    // Check if passwords match
    if (password !== confirmPassword) {
      setValidationError('Passwords do not match');
      return;
    }

    setIsSubmitting(true);

    try {
      // Use the AuthHandler to update the password
      const { data, error } = await AuthHandler.updatePassword(password);

      if (error) throw error;

      // Show success message
      setSuccess(true);

      // Redirect to login after a delay
      setTimeout(() => {
        navigate('/login');
      }, 5000);
    } catch (err) {
      setError(err.message || 'Failed to reset password. Please try again.');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="auth-container">
      <div className="auth-card">
        {!tokenValid ? (
          // Invalid token
          <div className="auth-error">
            <div className="error-icon">
              <svg viewBox="0 0 24 24" width="24" height="24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M6 18L18 6M6 6l12 12" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" stroke="currentColor" />
              </svg>
            </div>
            <h2>Invalid Reset Link</h2>
            <p>
              {error || 'The password reset link is invalid or has expired.'}
            </p>
            <Link
              to="/forgot-password"
              className="primary-button"
            >
              Request a new link
            </Link>
          </div>
        ) : success ? (
          // Success state
          <div className="auth-success">
            <div className="success-icon">
              <svg viewBox="0 0 24 24" width="24" height="24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M5 13l4 4L19 7" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" stroke="currentColor" />
              </svg>
            </div>
            <h2>Password Reset Complete</h2>
            <p>
              Your password has been reset successfully. You will be redirected to the login page shortly.
            </p>
            <Link
              to="/login"
              className="primary-button"
            >
              Sign in now
            </Link>
          </div>
        ) : (
          // Reset password form
          <>
            <div className="auth-header">
              <h1>Reset your password</h1>
              <p>
                Please enter a new password for your account.
              </p>
            </div>

            {/* Error Message */}
            {error && (
              <div className="error-message">
                <svg viewBox="0 0 24 24" width="24" height="24" fill="none" xmlns="http://www.w3.org/2000/svg">
                  <path d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                    strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" stroke="currentColor" />
                </svg>
                <span>{error}</span>
              </div>
            )}

            {/* Password Reset Form */}
            <form onSubmit={handleSubmit} className="auth-form">
              <div className="form-group">
                <label htmlFor="password">New Password</label>
                <div className="password-input">
                  <input
                    id="password"
                    name="password"
                    type={showPassword ? "text" : "password"}
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder="••••••••"
                    className={validationError ? 'input-error' : ''}
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
                <p className="form-help">
                  Password must be at least 8 characters and include at least one number and one letter.
                </p>
              </div>

              <div className="form-group">
                <label htmlFor="confirmPassword">Confirm Password</label>
                <input
                  id="confirmPassword"
                  name="confirmPassword"
                  type={showPassword ? "text" : "password"}
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  placeholder="••••••••"
                  className={validationError ? 'input-error' : ''}
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
                    Resetting Password...
                  </>
                ) : (
                  'Reset Password'
                )}
              </button>
            </form>

            <div className="auth-footer">
              <p>
                Remember your password?{' '}
                <Link to="/login" className="auth-link">
                  Back to login
                </Link>
              </p>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default ResetPasswordPage;