import React from 'react';
import { ArrowUpIcon, ArrowDownIcon } from '@heroicons/react/24/solid';
import { formatNumber, formatPercentage } from '../../utils/formatting';

/**
 * A card component for displaying a metric with trend indicator
 */
const MetricCard = ({
  title,
  value,
  previousValue,
  percentChange,
  format = 'number',
  isLoading = false,
  inverse = false, // If true, down is good (e.g., for error rates)
  precision = 1,
  size = 'default', // 'small', 'default', or 'large'
  icon: Icon = null,
  trendLabel,
  className = ''
}) => {
  // Format the value based on the specified format
  const formatValue = (val) => {
    if (val === null || val === undefined) return '-';

    switch (format) {
      case 'percentage':
        return formatPercentage(val, precision);
      case 'number':
      default:
        return formatNumber(val, precision);
    }
  };

  // Determine if the trend is positive or negative
  const isTrendPositive = percentChange > 0
    ? !inverse  // If not inverse, up is good
    : inverse;  // If inverse, down is good

  // Determine trend text and color
  let trendColor = 'text-secondary-500'; // neutral
  if (percentChange !== 0 && percentChange !== null && percentChange !== undefined) {
    trendColor = isTrendPositive ? 'text-accent-500' : 'text-danger-500';
  }

  // Size classes
  const sizeClasses = {
    small: {
      card: 'p-3',
      title: 'text-sm',
      value: 'text-xl',
      icon: 'h-5 w-5',
    },
    default: {
      card: 'p-4',
      title: 'text-md',
      value: 'text-2xl',
      icon: 'h-6 w-6',
    },
    large: {
      card: 'p-5',
      title: 'text-lg',
      value: 'text-3xl font-bold',
      icon: 'h-8 w-8',
    }
  };

  const classes = sizeClasses[size] || sizeClasses.default;

  return (
    <div className={`bg-white rounded-lg shadow ${classes.card} ${className}`}>
      <div className="flex items-center justify-between">
        <div className="flex items-center">
          {Icon && (
            <div className={`mr-2 text-primary-500 ${classes.icon}`}>
              <Icon />
            </div>
          )}
          <h3 className={`font-medium text-secondary-900 ${classes.title}`}>
            {title}
          </h3>
        </div>
      </div>

      <div className="mt-2">
        {isLoading ? (
          <div className="animate-pulse h-8 bg-secondary-200 rounded"></div>
        ) : (
          <div className={`font-semibold text-secondary-900 ${classes.value}`}>
            {formatValue(value)}
          </div>
        )}

        {percentChange !== null && percentChange !== undefined && !isLoading && (
          <div className={`flex items-center mt-1 ${trendColor}`}>
            {percentChange > 0 ? (
              <ArrowUpIcon className="h-4 w-4 mr-1" />
            ) : percentChange < 0 ? (
              <ArrowDownIcon className="h-4 w-4 mr-1" />
            ) : null}
            <span className="text-sm">
              {formatPercentage(Math.abs(percentChange), 1)}
              {trendLabel ? ` ${trendLabel}` : ''}
            </span>
          </div>
        )}
      </div>
    </div>
  );
};

export default MetricCard;