import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import AuthHandler from '../../auth/AuthHandler';
import './AuthPages.css';

function SignupPage() {
  const navigate = useNavigate();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [firstName, setFirstName] = useState('');
  const [lastName, setLastName] = useState('');
  const [organizationName, setOrganizationName] = useState('');
  const [agreeToTerms, setAgreeToTerms] = useState(false);
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState('');
  const [validationError, setValidationError] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [successMessage, setSuccessMessage] = useState('');

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
    setSuccessMessage('');

    // Basic validation
    if (!email || !password || !confirmPassword) {
      setValidationError('Please fill in all required fields');
      return;
    }

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

    // Check terms agreement
    if (!agreeToTerms) {
      setValidationError('You must agree to the Terms of Service');
      return;
    }

    setIsSubmitting(true);

    try {
      // 1. Register the user with Supabase
      console.log("Signing up user with email:", email);
      const { data: authData, error: authError } = await AuthHandler.signUp(
        email,
        password,
        {
          first_name: firstName,
          last_name: lastName
        }
      );

      if (authError) {
        console.error("Auth error during signup:", authError);
        throw authError;
      }

      if (!authData?.user) {
        console.error("No user data returned from signup");
        throw new Error("Failed to create account");
      }

      console.log("User created successfully:", authData.user.id);

      // 2. Call backend to set up profile and organization
      try {
        console.log("Setting up user profile and organization via backend");

        // First try to get a token, with fallback options
        let token = null;
        if (authData.session) {
          token = authData.session.access_token;
          console.log("Using token from auth session");
        } else {
          const session = await AuthHandler.getSession();
          token = session?.access_token;
          console.log("Using token from AuthHandler.getSession");
        }

        if (!token) {
          console.log("No token available, proceeding without authentication");
        }

        // Get the API base URL from environment or default
        const apiBaseUrl = process.env.REACT_APP_API_BASE_URL || '';
        console.log(`API base URL: ${apiBaseUrl}`);

        // Make API call to backend setup endpoint with full URL
        const response = await fetch(`${apiBaseUrl}/api/setup-user`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            ...(token ? {'Authorization': `Bearer ${token}`} : {})
          },
          body: JSON.stringify({
            user_id: authData.user.id,
            email: email,
            first_name: firstName,
            last_name: lastName,
            organization_name: organizationName
          })
        });

        const result = await response.json();

        if (!response.ok) {
          console.error("Backend setup failed:", result);
          // Still show success message but with a caveat
          setSuccessMessage('Account created, but there was an issue setting up your profile. An administrator will fix this for you.');
          throw new Error(result.error || "Failed to set up user profile");
        }

        console.log("User profile and organization setup complete");
        // Set success message
        setSuccessMessage('Account created successfully! Please check your email to confirm your registration.');
      } catch (setupErr) {
        console.error("Error setting up user profile:", setupErr);
        // Show modified success message
        setSuccessMessage('Account created, but there was an issue setting up your profile. Please contact support.');
      }

      // Clear form after successful submission
      setEmail('');
      setPassword('');
      setConfirmPassword('');
      setFirstName('');
      setLastName('');
      setOrganizationName('');
      setAgreeToTerms(false);

      // Redirect to login after delay
      setTimeout(() => {
        navigate('/login');
      }, 5000);
    } catch (err) {
      console.error('Signup error:', err);
      setError(err.message || 'Failed to create account. Please try again.');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="auth-container">
      <div className="auth-card">
        <div className="auth-header">
          <h1>Create your account</h1>
          <p>Sign up for Sparvi to start monitoring your data quality</p>
        </div>

        {/* Success Message */}
        {successMessage && (
          <div className="alert alert-success">
            <svg viewBox="0 0 24 24" width="24" height="24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M5 13l4 4L19 7" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" stroke="currentColor" />
            </svg>
            <span>{successMessage}</span>
          </div>
        )}

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

        {/* Validation Error */}
        {validationError && (
          <div className="error-message">
            <svg viewBox="0 0 24 24" width="24" height="24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" stroke="currentColor" />
            </svg>
            <span>{validationError}</span>
          </div>
        )}

        {/* Signup Form */}
        <form onSubmit={handleSubmit} className="auth-form">
          <div className="form-group">
            <label htmlFor="email">Email address *</label>
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

          <div className="row">
            <div className="col-md-6">
              <div className="form-group">
                <label htmlFor="firstName">First name</label>
                <input
                  id="firstName"
                  name="firstName"
                  type="text"
                  autoComplete="given-name"
                  value={firstName}
                  onChange={(e) => setFirstName(e.target.value)}
                  placeholder="John"
                />
              </div>
            </div>
            <div className="col-md-6">
              <div className="form-group">
                <label htmlFor="lastName">Last name</label>
                <input
                  id="lastName"
                  name="lastName"
                  type="text"
                  autoComplete="family-name"
                  value={lastName}
                  onChange={(e) => setLastName(e.target.value)}
                  placeholder="Doe"
                />
              </div>
            </div>
          </div>

          <div className="form-group">
            <label htmlFor="organizationName">Organization name</label>
            <input
              id="organizationName"
              name="organizationName"
              type="text"
              value={organizationName}
              onChange={(e) => setOrganizationName(e.target.value)}
              placeholder="Your company (optional)"
            />
            <div className="form-text">
              We'll create a default organization for you if not specified
            </div>
          </div>

          <div className="form-group">
            <label htmlFor="password">Password *</label>
            <div className="password-input">
              <input
                id="password"
                name="password"
                type={showPassword ? "text" : "password"}
                autoComplete="new-password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="••••••••"
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
            <label htmlFor="confirmPassword">Confirm password *</label>
            <input
              id="confirmPassword"
              name="confirmPassword"
              type={showPassword ? "text" : "password"}
              autoComplete="new-password"
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
              placeholder="••••••••"
              required
            />
          </div>

          <div className="tos-agreement">
            <input
              id="agreeToTerms"
              name="agreeToTerms"
              type="checkbox"
              checked={agreeToTerms}
              onChange={(e) => setAgreeToTerms(e.target.checked)}
              required
            />
            <label htmlFor="agreeToTerms">
              I agree to the <a href="/terms" className="auth-link">Terms of Service</a> and <a href="/privacy" className="auth-link">Privacy Policy</a>
            </label>
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
                Creating account...
              </>
            ) : (
              'Create account'
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
      </div>
    </div>
  );
}

export default SignupPage;