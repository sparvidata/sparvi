import React, { useState, useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { useUI } from '../../contexts/UIContext';
import Logo from '../../components/common/Logo';
import { supabase } from '../../api/supabase';

const ResetPasswordPage = () => {
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isSuccess, setIsSuccess] = useState(false);
  const [error, setError] = useState('');
  const [hashPresent, setHashPresent] = useState(false);

  const { changePassword } = useAuth();
  const { showNotification } = useUI();
  const navigate = useNavigate();

  // Check if the hash is present (needed for password reset)
  useEffect(() => {
    const checkHash = async () => {
      try {
        // Get hash from URL
        const hash = window.location.hash;

        if (hash) {
          // Verify the hash is valid
          const { data, error } = await supabase.auth.getUser();

          if (error) {
            setError('Invalid or expired password reset link');
            setHashPresent(false);
          } else {
            setHashPresent(true);
          }
        } else {
          setError('No password reset link found');
          setHashPresent(false);
        }
      } catch (err) {
        setError('Failed to process password reset link');
        setHashPresent(false);
      }
    };

    checkHash();
  }, []);

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!password || !confirmPassword) {
      setError('Please fill in all fields');
      return;
    }

    if (password !== confirmPassword) {
      setError('Passwords do not match');
      return;
    }

    if (password.length < 6) {
      setError('Password must be at least 6 characters long');
      return;
    }

    try {
      setError('');
      setIsSubmitting(true);

      await changePassword(password);

      setIsSuccess(true);
      showNotification('Password has been reset successfully!', 'success');

      // Redirect to login after 3 seconds
      setTimeout(() => {
        navigate('/login');
      }, 3000);
    } catch (err) {
      setError(err.message || 'Failed to reset password');
      showNotification('Error: ' + err.message, 'error');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen bg-secondary-50 flex flex-col justify-center py-12 sm:px-6 lg:px-8">
      <div className="sm:mx-auto sm:w-full sm:max-w-md">
        <div className="flex justify-center">
          <Logo size="large" />
        </div>
        <h2 className="mt-6 text-center text-3xl font-extrabold text-secondary-900">
          Reset your password
        </h2>
        <p className="mt-2 text-center text-sm text-secondary-600">
          Or{' '}
          <Link to="/login" className="font-medium text-primary-600 hover:text-primary-500">
            sign in to your account
          </Link>
        </p>
      </div>

      <div className="mt-8 sm:mx-auto sm:w-full sm:max-w-md">
        <div className="bg-white py-8 px-4 shadow sm:rounded-lg sm:px-10">
          {isSuccess ? (
            <div className="rounded-md bg-accent-50 p-4">
              <div className="flex">
                <div className="flex-shrink-0">
                  <svg className="h-5 w-5 text-accent-400" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                    <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                  </svg>
                </div>
                <div className="ml-3">
                  <h3 className="text-sm font-medium text-accent-800">Password reset successful</h3>
                  <div className="mt-2 text-sm text-accent-700">
                    <p>
                      Your password has been reset successfully. You will be redirected to the login page in a few seconds.
                    </p>
                  </div>
                  <div className="mt-4">
                    <div className="-mx-2 -my-1.5 flex">
                      <Link
                        to="/login"
                        className="bg-accent-50 px-2 py-1.5 rounded-md text-sm font-medium text-accent-800 hover:bg-accent-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-accent-50 focus:ring-accent-600"
                      >
                        Go to sign in
                      </Link>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          ) : !hashPresent ? (
            <div className="rounded-md bg-danger-50 p-4">
              <div className="flex">
                <div className="flex-shrink-0">
                  <svg className="h-5 w-5 text-danger-400" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                    <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                  </svg>
                </div>
                <div className="ml-3">
                  <h3 className="text-sm font-medium text-danger-800">{error || 'Invalid password reset link'}</h3>
                  <div className="mt-2 text-sm text-danger-700">
                    <p>
                      The password reset link is invalid or has expired. Please request a new password reset link.
                    </p>
                  </div>
                  <div className="mt-4">
                    <div className="-mx-2 -my-1.5 flex">
                      <Link
                        to="/forgot-password"
                        className="bg-danger-50 px-2 py-1.5 rounded-md text-sm font-medium text-danger-800 hover:bg-danger-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-danger-50 focus:ring-danger-600"
                      >
                        Request new link
                      </Link>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          ) : (
            <>
              {error && (
                <div className="mb-4 bg-danger-50 border border-danger-200 text-danger-700 px-4 py-3 rounded relative" role="alert">
                  <span className="block sm:inline">{error}</span>
                </div>
              )}

              <form className="space-y-6" onSubmit={handleSubmit}>
                <div>
                  <label htmlFor="password" className="block text-sm font-medium text-secondary-700">
                    New Password
                  </label>
                  <div className="mt-1">
                    <input
                      id="password"
                      name="password"
                      type="password"
                      autoComplete="new-password"
                      required
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      className="input-field"
                    />
                  </div>
                </div>

                <div>
                  <label htmlFor="confirmPassword" className="block text-sm font-medium text-secondary-700">
                    Confirm New Password
                  </label>
                  <div className="mt-1">
                    <input
                      id="confirmPassword"
                      name="confirmPassword"
                      type="password"
                      autoComplete="new-password"
                      required
                      value={confirmPassword}
                      onChange={(e) => setConfirmPassword(e.target.value)}
                      className="input-field"
                    />
                  </div>
                </div>

                <div>
                  <button
                    type="submit"
                    disabled={isSubmitting}
                    className="w-full flex justify-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    {isSubmitting ? (
                      <>
                        <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                        </svg>
                        Resetting password...
                      </>
                    ) : (
                      'Reset password'
                    )}
                  </button>
                </div>
              </form>
            </>
          )}
        </div>
      </div>
    </div>
  );
};

export default ResetPasswordPage;