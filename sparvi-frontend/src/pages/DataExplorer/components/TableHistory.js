import React, { useState, useEffect } from 'react';
import {
  ClockIcon,
  CheckCircleIcon,
  XCircleIcon,
  ArrowTrendingUpIcon,
  ArrowTrendingDownIcon,
  MinusIcon,
  ChartBarIcon,
  DocumentMagnifyingGlassIcon
} from '@heroicons/react/24/outline';
import { profilingAPI, validationsAPI } from '../../../api/enhancedApiService';
import { useUI } from '../../../contexts/UIContext';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import { formatDate } from '../../../utils/formatting';

const TableHistory = ({ tableName, connectionId }) => {
  const { showNotification } = useUI();

  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedProfile, setSelectedProfile] = useState(null);
  const [validationResults, setValidationResults] = useState([]);
  const [loadingValidations, setLoadingValidations] = useState(false);

  // Load history data
  useEffect(() => {
    const loadHistory = async () => {
      if (!tableName) return;

      try {
        setLoading(true);

        const response = await profilingAPI.getHistory(tableName);

        // Ensure we get an array, even if empty
        setHistory(response.data.history || []);
      } catch (error) {
        console.error(`Error loading history for ${tableName}:`, error);
        showNotification(`Failed to load history for ${tableName}`, 'error');
      } finally {
        setLoading(false);
      }
    };

    loadHistory();
  }, [tableName]);

  // Handle profile selection
  const handleProfileSelect = async (profile) => {
    if (selectedProfile && selectedProfile.id === profile.id) {
      // Deselect if already selected
      setSelectedProfile(null);
      setValidationResults([]);
      return;
    }

    setSelectedProfile(profile);

    // Load validation results for this profile
    try {
      setLoadingValidations(true);

      const response = await validationsAPI.getValidationHistory(profile.id);
      setValidationResults(response.data.results || []);
    } catch (error) {
      console.error(`Error loading validation results for profile ${profile.id}:`, error);
      showNotification('Failed to load validation results', 'error');
    } finally {
      setLoadingValidations(false);
    }
  };

  // Calculate row count trend between profiles
  const getRowCountTrend = (profile, index) => {
    if (index === history.length - 1) return 'neutral'; // First profile has no trend

    const prevProfile = history[index + 1];
    if (!prevProfile || !profile.row_count || !prevProfile.row_count) return 'neutral';

    if (profile.row_count > prevProfile.row_count) return 'up';
    if (profile.row_count < prevProfile.row_count) return 'down';
    return 'neutral';
  };

  // Get row count trend icon
  const getRowCountTrendIcon = (trend) => {
    switch (trend) {
      case 'up':
        return <ArrowTrendingUpIcon className="h-5 w-5 text-accent-500" />;
      case 'down':
        return <ArrowTrendingDownIcon className="h-5 w-5 text-warning-500" />;
      default:
        return <MinusIcon className="h-5 w-5 text-secondary-400" />;
    }
  };

  // If loading with no history, show loading state
  if (loading && !history.length) {
    return (
      <div className="flex justify-center py-10">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  // If no history, show empty state
  if (!history.length) {
    return (
      <div className="text-center py-10">
        <ClockIcon className="mx-auto h-12 w-12 text-secondary-400" />
        <h3 className="mt-2 text-sm font-medium text-secondary-900">No profile history</h3>
        <p className="mt-1 text-sm text-secondary-500">
          No profiles have been run for this table yet.
        </p>
      </div>
    );
  }

  return (
    <div>
      <h3 className="text-lg font-medium text-secondary-900 mb-4">Profile History</h3>

      <div className="bg-white shadow overflow-hidden sm:rounded-md">
        <ul className="divide-y divide-secondary-200">
          {history.map((profile, index) => {
            const isSelected = selectedProfile && selectedProfile.id === profile.id;
            const rowCountTrend = getRowCountTrend(profile, index);

            return (
              <li key={profile.id}>
                <div
                  className={`px-4 py-4 sm:px-6 hover:bg-secondary-50 cursor-pointer ${
                    isSelected ? 'bg-primary-50' : ''
                  }`}
                  onClick={() => handleProfileSelect(profile)}
                >
                  {/* Header row with date and actions */}
                  <div className="flex items-center justify-between">
                    <div className="flex items-center min-w-0">
                      <ClockIcon className="h-5 w-5 text-secondary-400 mr-2" aria-hidden="true" />
                      <p className="text-sm font-medium text-secondary-900 truncate">
                        {formatDate(profile.created_at, true)}
                      </p>
                    </div>

                    <div className="flex items-center space-x-2">
                      <button
                        type="button"
                        className="p-1 text-secondary-400 hover:text-secondary-500 focus:outline-none"
                        title="View profile details"
                      >
                        <DocumentMagnifyingGlassIcon className="h-5 w-5" aria-hidden="true" />
                      </button>

                      <button
                        type="button"
                        className="p-1 text-secondary-400 hover:text-secondary-500 focus:outline-none"
                        title="View profile chart"
                      >
                        <ChartBarIcon className="h-5 w-5" aria-hidden="true" />
                      </button>
                    </div>
                  </div>

                  {/* Profile summary */}
                  <div className="mt-2 sm:flex sm:justify-between">
                    <div className="sm:flex items-center">
                      <div className="flex items-center">
                        <span className="text-sm text-secondary-500 mr-1">Rows:</span>
                        <span className="text-sm font-medium text-secondary-900 mr-2">
                          {profile.row_count?.toLocaleString() || 'N/A'}
                        </span>
                        <span className="mr-2">{getRowCountTrendIcon(rowCountTrend)}</span>
                      </div>

                      <div className="mt-2 flex items-center sm:mt-0 sm:ml-6">
                        <span className="text-sm text-secondary-500 mr-1">Columns:</span>
                        <span className="text-sm font-medium text-secondary-900">
                          {profile.column_count || 'N/A'}
                        </span>
                      </div>
                    </div>

                    <div className="mt-2 flex items-center text-sm text-secondary-500 sm:mt-0">
                      <p>
                        Run by: <span className="font-medium">{profile.created_by_email || 'System'}</span>
                      </p>
                    </div>
                  </div>

                  {/* Validation summary if available */}
                  {profile.validation_summary && (
                    <div className="mt-2 flex items-center">
                      <div className="text-sm text-secondary-500 mr-2">Validations:</div>
                      <div className="flex items-center">
                        <CheckCircleIcon className="h-4 w-4 text-accent-500 mr-1" />
                        <span className="text-sm font-medium text-accent-700 mr-2">
                          {profile.validation_summary.passed || 0}
                        </span>
                      </div>
                      <div className="flex items-center">
                        <XCircleIcon className="h-4 w-4 text-danger-500 mr-1" />
                        <span className="text-sm font-medium text-danger-700">
                          {profile.validation_summary.failed || 0}
                        </span>
                      </div>
                    </div>
                  )}
                </div>

                {/* Expanded validation results */}
                {isSelected && validationResults.length > 0 && (
                  <div className="px-4 py-4 sm:px-6 bg-secondary-50 border-t border-secondary-200">
                    <h4 className="text-sm font-medium text-secondary-900 mb-2">Validation Results</h4>

                    {loadingValidations ? (
                      <div className="flex justify-center py-4">
                        <LoadingSpinner size="md" />
                      </div>
                    ) : (
                      <div className="space-y-3">
                        {validationResults.map((result) => (
                          <div
                            key={result.id}
                            className="bg-white p-3 rounded-md shadow-sm flex items-center justify-between"
                          >
                            <div className="flex items-center">
                              {result.is_valid ? (
                                <CheckCircleIcon className="h-5 w-5 text-accent-500 mr-2" />
                              ) : (
                                <XCircleIcon className="h-5 w-5 text-danger-500 mr-2" />
                              )}
                              <div>
                                <p className="text-sm font-medium text-secondary-900">
                                  {result.validation_rules?.rule_name || 'Unknown rule'}
                                </p>
                                <p className="text-xs text-secondary-500">
                                  {result.validation_rules?.operator || ''} {result.validation_rules?.expected_value || ''}
                                </p>
                              </div>
                            </div>

                            {result.actual_value !== undefined && (
                              <div className="text-sm">
                                <span className="text-secondary-500">Actual: </span>
                                <span className={`font-medium ${
                                  result.is_valid ? 'text-accent-600' : 'text-danger-600'
                                }`}>
                                  {result.actual_value}
                                </span>
                              </div>
                            )}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}

                {/* Show message if no validation results */}
                {isSelected && validationResults.length === 0 && !loadingValidations && (
                  <div className="px-4 py-4 sm:px-6 bg-secondary-50 border-t border-secondary-200">
                    <p className="text-sm text-secondary-500 text-center">
                      No validation results available for this profile
                    </p>
                  </div>
                )}
              </li>
            );
          })}
        </ul>
      </div>
    </div>
  );
};

export default TableHistory;