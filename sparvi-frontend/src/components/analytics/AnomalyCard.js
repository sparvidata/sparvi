import React from 'react';
import { Link } from 'react-router-dom';
import { ExclamationTriangleIcon, ArrowRightIcon } from '@heroicons/react/24/solid';
import { formatDate } from '../../utils/formatting';

/**
 * A card component for displaying detected anomalies
 */
const AnomalyCard = ({
  title,
  description,
  severity = 'medium', // 'low', 'medium', 'high'
  detectedAt,
  tableName,
  columnName,
  actionLink,
  actionText = 'View Details',
  onAction,
  className = ''
}) => {
  // Severity colors
  const severityColors = {
    low: {
      bg: 'bg-warning-50',
      border: 'border-warning-200',
      icon: 'text-warning-500'
    },
    medium: {
      bg: 'bg-danger-50',
      border: 'border-danger-200',
      icon: 'text-danger-500'
    },
    high: {
      bg: 'bg-danger-100',
      border: 'border-danger-300',
      icon: 'text-danger-600'
    }
  };

  const colors = severityColors[severity] || severityColors.medium;

  return (
    <div className={`rounded-lg ${colors.bg} ${colors.border} border p-4 ${className}`}>
      <div className="flex items-start">
        <div className={`mt-0.5 ${colors.icon}`}>
          <ExclamationTriangleIcon className="h-5 w-5" />
        </div>

        <div className="ml-3 flex-1">
          <h3 className="text-sm font-medium text-secondary-900">{title}</h3>

          {description && (
            <p className="mt-1 text-sm text-secondary-700">{description}</p>
          )}

          <div className="mt-2 text-xs text-secondary-500 space-y-1">
            {detectedAt && (
              <div>Detected: {formatDate(detectedAt, true)}</div>
            )}

            {tableName && (
              <div>Table: {tableName}</div>
            )}

            {columnName && (
              <div>Column: {columnName}</div>
            )}
          </div>

          {(actionLink || onAction) && (
            <div className="mt-3">
              {actionLink ? (
                <Link
                  to={actionLink}
                  className="text-sm font-medium text-primary-600 hover:text-primary-500 flex items-center"
                >
                  {actionText}
                  <ArrowRightIcon className="ml-1 h-3 w-3" />
                </Link>
              ) : (
                <button
                  onClick={onAction}
                  className="text-sm font-medium text-primary-600 hover:text-primary-500 flex items-center"
                >
                  {actionText}
                  <ArrowRightIcon className="ml-1 h-3 w-3" />
                </button>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default AnomalyCard;