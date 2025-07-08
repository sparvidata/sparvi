import React from 'react';
import {
  SparklesIcon,
  ClipboardDocumentCheckIcon,
  ExclamationTriangleIcon,
  CalendarDaysIcon,
  ArrowTrendingUpIcon,
  ArrowTrendingDownIcon
} from '@heroicons/react/24/outline';

const ValidationMetricCards = ({ metrics }) => {
  if (!metrics) return null;

  // Get trend indicator
  const getTrendIndicator = (trend) => {
    if (trend > 0) {
      return {
        icon: ArrowTrendingUpIcon,
        color: 'text-accent-600',
        bgColor: 'bg-accent-50',
        text: `+${trend.toFixed(1)}%`
      };
    } else if (trend < 0) {
      return {
        icon: ArrowTrendingDownIcon,
        color: 'text-danger-600',
        bgColor: 'bg-danger-50',
        text: `${trend.toFixed(1)}%`
      };
    }
    return null;
  };

  const healthTrend = getTrendIndicator(metrics.healthScoreTrend);

  const cards = [
    {
      title: 'Current Health Score',
      value: `${metrics.currentHealthScore}%`,
      subtitle: healthTrend ? (
        <div className={`flex items-center ${healthTrend.color}`}>
          <healthTrend.icon className="h-4 w-4 mr-1" />
          <span className="text-xs">{healthTrend.text} from yesterday</span>
        </div>
      ) : (
        <span className="text-xs text-secondary-500">No change from yesterday</span>
      ),
      icon: SparklesIcon,
      color: metrics.currentHealthScore >= 80
        ? 'accent'
        : metrics.currentHealthScore >= 60
        ? 'warning'
        : 'danger'
    },
    {
      title: 'Validations Run',
      value: metrics.totalValidationsRun.toLocaleString(),
      subtitle: `${metrics.latestTotalValidations} in latest run`,
      icon: ClipboardDocumentCheckIcon,
      color: 'primary'
    },
    {
      title: 'Avg Daily Failures',
      value: metrics.avgDailyFailures.toString(),
      subtitle: `${metrics.latestFailed} in latest run`,
      icon: ExclamationTriangleIcon,
      color: metrics.avgDailyFailures > 0 ? 'warning' : 'accent'
    },
    {
      title: 'Days Since Last Failure',
      value: metrics.daysSinceLastFailure.toString(),
      subtitle: metrics.daysSinceLastFailure === 0
        ? 'Failed in latest run'
        : metrics.daysSinceLastFailure === 1
        ? '1 day streak'
        : `${metrics.daysSinceLastFailure} day streak`,
      icon: CalendarDaysIcon,
      color: metrics.daysSinceLastFailure >= 7
        ? 'accent'
        : metrics.daysSinceLastFailure >= 3
        ? 'primary'
        : metrics.daysSinceLastFailure > 0
        ? 'warning'
        : 'danger'
    }
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      {cards.map((card, index) => {
        const IconComponent = card.icon;

        return (
          <div key={index} className="bg-white p-4 rounded-lg shadow border border-secondary-200">
            <div className="flex items-center">
              <div className={`p-2 rounded-lg bg-${card.color}-100 mr-3`}>
                <IconComponent className={`h-5 w-5 text-${card.color}-600`} />
              </div>
              <div className="flex-1 min-w-0">
                <h4 className="text-sm font-medium text-secondary-900 truncate">
                  {card.title}
                </h4>
              </div>
            </div>

            <div className="mt-3">
              <div className={`text-2xl font-bold text-${card.color}-600`}>
                {card.value}
              </div>
              <div className="mt-1">
                {card.subtitle}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
};

export default ValidationMetricCards;