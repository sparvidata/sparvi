import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { useUI } from '../../contexts/UIContext';
import Logo from '../../components/common/Logo';

const ForgotPasswordPage = () => {
  const [email, setEmail] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isSuccess, setIsSuccess] = useState(false);
  const [error, setError] = useState('');

  const { sendPasswordResetEmail } = useAuth();
  const { showNotification } = useUI();

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!email) {
      setError('Please enter your email address');
      return;
    }

    try {
      setError('');
      setIsSubmitting(true);

      await sendPasswordResetEmail(email);

      setIsSuccess(true);
      showNotification('Password reset email sent! Check your inbox.', 'success');
    } catch (err) {
      setError(err.message || 'Failed to send password reset email');
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
                  <h3 className="text-sm font-medium text-accent-800">Password reset email sent</h3>
                  <div className="mt-2 text-sm text-accent-700">
                    <p>
                      Check your email for a link to reset your password. If it doesn't appear within a few minutes, check your spam folder.
                    </p>
                  </div>
                  <div className="mt-4">
                    <div className="-mx-2 -my-1.5 flex">
                      <Link
                        to="/login"
                        className="bg-accent-50 px-2 py-1.5 rounded-md text-sm font-medium text-accent-800 hover:bg-accent-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-accent-50 focus:ring-accent-600"
                      >
                        Return to sign in
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

              <div className="mb-4 text-sm text-secondary-600">
                Enter your email address and we'll send you a link to reset your password.
              </div>

              <form className="space-y-6" onSubmit={handleSubmit}>
                <div>
                  <label htmlFor="email" className="block text-sm font-medium text-secondary-700">
                    Email address
                  </label>
                  <div className="mt-1">
                    <input
                      id="email"
                      name="email"
                      type="email"
                      autoComplete="email"
                      required
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
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
                        Sending reset email...
                      </>
                    ) : (
                      'Send password reset email'
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

export default ForgotPasswordPage;