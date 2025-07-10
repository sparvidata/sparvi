import React from 'react';
import { resetAllCircuitBreakers } from '../utils/requestUtils';

/**
 * Error boundary to catch and handle React errors gracefully
 */
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      hasError: false,
      error: null,
      errorInfo: null,
      errorId: null
    };
  }

  static getDerivedStateFromError(error) {
    // Update state so the next render will show the fallback UI
    return {
      hasError: true,
      errorId: Date.now().toString(36) + Math.random().toString(36).substr(2)
    };
  }

  componentDidCatch(error, errorInfo) {
    // Log error details
    console.error('Error Boundary caught an error:', error, errorInfo);

    this.setState({
      error,
      errorInfo
    });

    // Optional: Send error to logging service
    if (this.props.onError) {
      this.props.onError(error, errorInfo);
    }
  }

  handleRetry = () => {
    // Reset circuit breakers
    resetAllCircuitBreakers();

    // Reset error state
    this.setState({
      hasError: false,
      error: null,
      errorInfo: null,
      errorId: null
    });
  };

  render() {
    if (this.state.hasError) {
      // Custom fallback UI
      if (this.props.fallback) {
        return this.props.fallback;
      }

      // Default fallback UI
      return (
        <div className="error-boundary">
          <div className="error-boundary-content">
            <h2>Something went wrong</h2>
            <p>An unexpected error occurred. Please try refreshing the page.</p>

            {this.props.showDetails && this.state.error && (
              <details style={{ marginTop: '1rem' }}>
                <summary>Error Details</summary>
                <pre style={{
                  marginTop: '0.5rem',
                  padding: '1rem',
                  background: '#f5f5f5',
                  overflow: 'auto',
                  fontSize: '0.875rem'
                }}>
                  {this.state.error.toString()}
                  {this.state.errorInfo.componentStack}
                </pre>
              </details>
            )}

            <div style={{ marginTop: '1rem' }}>
              <button
                onClick={this.handleRetry}
                style={{
                  padding: '0.5rem 1rem',
                  marginRight: '0.5rem',
                  backgroundColor: '#007bff',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer'
                }}
              >
                Try Again
              </button>

              <button
                onClick={() => window.location.reload()}
                style={{
                  padding: '0.5rem 1rem',
                  backgroundColor: '#6c757d',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer'
                }}
              >
                Refresh Page
              </button>
            </div>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

/**
 * Hook-based error boundary for functional components
 */
export const useErrorHandler = () => {
  const [error, setError] = React.useState(null);

  const resetError = React.useCallback(() => {
    setError(null);
    resetAllCircuitBreakers();
  }, []);

  const handleError = React.useCallback((error) => {
    console.error('Error handler caught:', error);
    setError(error);
  }, []);

  // Throw error to be caught by nearest error boundary
  if (error) {
    throw error;
  }

  return { handleError, resetError };
};

export default ErrorBoundary;