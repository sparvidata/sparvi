import React from 'react';
import { LightBulbIcon, CheckCircleIcon } from '@heroicons/react/24/outline';

/**
 * A card component for displaying actionable insights
 */
const InsightCard = ({
  title,
  description,
  type = 'suggestion', // 'suggestion', 'issue', 'optimization'
  impact = 'medium', // 'low', 'medium', 'high'
  actionText = 'Apply',
  onAction,
  actionInProgress = false,
  actionComplete = false,
  tableName,
  className = ''
}) => {
  // Type colors
  const typeColors = {
    suggestion: {
      bg: 'bg-primary-50',
      border: 'border-primary-200',
      icon: 'text-primary-600'
    },
    issue: {
      bg: 'bg-danger-50',
      border: 'border-danger-200',
      icon: 'text-danger-600'
    },
    optimization: {
      bg: 'bg-accent-50',
      border: 'border-accent-200',
      icon: 'text-accent-600'
    }
  };

  // Impact badges
  const impactBadges = {
    low: 'bg-secondary-100 text-secondary-800',
    medium: 'bg-primary-100 text-primary-800',
    high: 'bg-accent-100 text-accent-800'
  };

  const colors = typeColors[type] || typeColors.suggestion;
  const impactBadge = impactBadges[impact] || impactBadges.medium;

  return (
    <div className={`rounded-lg ${colors.bg} ${colors.border} border p-4 ${className}`}>
      <div className="flex items-start">
        <div className={`${colors.icon}`}>
          <LightBulbIcon className="h-5 w-5" />
        </div>

        <div className="ml-3 flex-1">
          <div className="flex justify-between">
            <h3 className="text-sm font-medium text-secondary-900">{title}</h3>

            <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${impactBadge}`}>
              {impact.charAt(0).toUpperCase() + impact.slice(1)} Impact
            </span>
          </div>

          {description && (
            <p className="mt-1 text-sm text-secondary-700">{description}</p>
          )}

          {tableName && (
            <div className="mt-1 text-xs text-secondary-500">
              Table: {tableName}
            </div>
          )}

          {onAction && (
            <div className="mt-3">
              {actionComplete ? (
                <div className="flex items-center text-accent-600 text-sm">
                  <CheckCircleIcon className="h-4 w-4 mr-1" />
                  Applied
                </div>
              ) : (
                <button
                  onClick={onAction}
                  disabled={actionInProgress}
                  className={`inline-flex items-center px-3 py-1.5 border border-transparent text-xs font-medium rounded text-white ${
                    actionInProgress ? 'bg-primary-400 cursor-not-allowed' : 'bg-primary-600 hover:bg-primary-700'
                  }`}
                >
                  {actionInProgress ? (
                    <>
                      <svg className="animate-spin -ml-1 mr-2 h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                      </svg>
                      Applying...
                    </>
                  ) : (
                    actionText
                  )}
                </button>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default InsightCard;