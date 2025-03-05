import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { supabase } from '../../lib/supabase';
import './AuthPages.css';

function ForgotPasswordPage() {
  const [email, setEmail] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState('');

  // Handle form submission
  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');

    // Basic validation
    if (!email) {
      setError('Please enter your email address');
      return;
    }

    setIsSubmitting(true);

    try {
      const { error } = await supabase.auth.resetPasswordForEmail(email, {
        redirectTo: `${window.location.origin}/reset-password`,
      });

      if (error) throw error;

      // Show success message
      setSuccess(true);
    } catch (err) {
      setError(err.message || 'Something went wrong. Please try again.');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="auth-container">
      <div className="auth-card">
        {success ? (
          // Success state
          <div className="auth-success">
            <div className="success-icon">
              <svg viewBox="0 0 24 24" width="24" height="24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M5 13l4 4L19 7" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" stroke="currentColor" />
              </svg>
            </div>
            <h2>Check your email</h2>
            <p>
              We've sent a password reset link to <strong>{email}</strong>. The link will expire in 1 hour.
            </p>
            <p>
              Don't see it? Check your spam folder or make sure you entered the correct email address.
            </p>
            <div className="auth-actions">
              <button
                onClick={() => {
                  setSuccess(false);
                  setEmail('');
                }}
                className="secondary-button"
              >
                Try again
              </button>
              <Link
                to="/login"
                className="primary-button"
              >
                Back to login
              </Link>
            </div>
          </div>
        ) : (
          // Reset password form
          <>
            <div className="auth-header">
              <h1>Reset your password</h1>
              <p>
                Enter your email and we'll send you a link to reset your password.
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
                <label htmlFor="email">Email address</label>
                <input
                  id="email"
                  name="email"
                  type="email"
                  autoComplete="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="you@example.com"
                  required
                />
              </div>

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
                    Sending...
                  </>
                ) : (
                  'Send reset link'
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

export default ForgotPasswordPage;