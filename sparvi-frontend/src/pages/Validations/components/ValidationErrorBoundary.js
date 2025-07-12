import React from 'react';
import { ExclamationTriangleIcon, ArrowPathIcon } from '@heroicons/react/24/outline';

class ValidationErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      hasError: false,
      error: null,
      errorInfo: null,
      retryCount: 0
    };
  }

  static getDerivedStateFromError(error) {
    // Update state so the next render will show the fallback UI
    return { hasError: true };
  }

  componentDidCatch(error, errorInfo) {
    // Log the error for debugging
    console.error('ValidationErrorBoundary caught an error:', error, errorInfo);

    this.setState({
      error,
      errorInfo
    });

    // Report error to monitoring service if available
    if (window.reportError) {
      window.reportError(error, {
        context: 'ValidationErrorBoundary',
        componentStack: errorInfo.componentStack,
        tableName: this.props.tableName,
        connectionId: this.props.connectionId
      });
    }
  }

  handleRetry = () => {
    this.setState(prevState => ({
      hasError: false,
      error: null,
      errorInfo: null,
      retryCount: prevState.retryCount + 1
    }));
  }

  handleReload = () => {
    window.location.reload();
  }

  render() {
    if (this.state.hasError) {
      // Custom fallback UI
      return (
        <div className="bg-white p-6 rounded-lg shadow border border-danger-200">
          <div className="flex flex-col items-center justify-center h-64">
            <ExclamationTriangleIcon className="h-12 w-12 text-danger-400 mb-4" />

            <h3 className="text-lg font-medium text-secondary-900 mb-2">
              Something went wrong with the validation dashboard
            </h3>

            <p className="text-secondary-600 text-center mb-4 max-w-md">
              {this.props.tableName
                ? `There was an error loading validation data for "${this.props.tableName}"`
                : "There was an error loading the validation dashboard"
              }
            </p>

            <div className="flex flex-col sm:flex-row gap-3 mb-4">
              <button
                onClick={this.handleRetry}
                disabled={this.state.retryCount >= 3}
                className="flex items-center px-4 py-2 bg-primary-600 text-white rounded-md hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <ArrowPathIcon className="h-4 w-4 mr-2" />
                {this.state.retryCount >= 3 ? 'Max Retries Reached' : 'Try Again'}
              </button>

              <button
                onClick={this.handleReload}
                className="px-4 py-2 bg-secondary-100 text-secondary-700 rounded-md hover:bg-secondary-200"
              >
                Reload Page
              </button>
            </div>

            {this.state.retryCount > 0 && (
              <p className="text-xs text-secondary-500 mb-2">
                Retry attempts: {this.state.retryCount}/3
              </p>
            )}

            {/* Error details for development */}
            {process.env.NODE_ENV === 'development' && this.state.error && (
              <details className="mt-4 w-full max-w-2xl">
                <summary className="cursor-pointer text-sm font-medium text-secondary-700 mb-2">
                  Technical Details (Development)
                </summary>
                <div className="bg-secondary-50 p-3 rounded border text-xs font-mono overflow-auto max-h-40">
                  <div className="mb-2">
                    <strong>Error:</strong> {this.state.error.toString()}
                  </div>
                  {this.state.errorInfo?.componentStack && (
                    <div>
                      <strong>Component Stack:</strong>
                      <pre className="whitespace-pre-wrap">{this.state.errorInfo.componentStack}</pre>
                    </div>
                  )}
                </div>
              </details>
            )}
          </div>
        </div>
      );
    }

    // If no error, render children normally
    return this.props.children;
  }
}

export default ValidationErrorBoundary;