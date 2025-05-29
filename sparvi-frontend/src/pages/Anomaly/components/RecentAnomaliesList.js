import React from 'react';
import { Link } from 'react-router-dom';
import { formatDate } from '../../../utils/formatting';
import {
  ExclamationTriangleIcon,
  ExclamationCircleIcon,
  ArrowRightIcon
} from '@heroicons/react/24/outline';

const RecentAnomaliesList = ({ anomalies, connectionId }) => {
  // Helper to get severity icon
  const getSeverityIcon = (severity) => {
    switch (severity) {
      case 'high':
        return <ExclamationTriangleIcon className="h-5 w-5 text-red-600" />;
      case 'medium':
        return <ExclamationCircleIcon className="h-5 w-5 text-yellow-500" />;
      case 'low':
        return <ExclamationCircleIcon className="h-5 w-5 text-green-500" />;
      default:
        return <ExclamationCircleIcon className="h-5 w-5 text-gray-400" />;
    }
  };

  // Helper to format metric display value
  const formatMetricValue = (value) => {
    if (value === null || value === undefined) return 'N/A';

    // Check if it's a number
    if (!isNaN(parseFloat(value))) {
      const num = parseFloat(value);
      if (num === Math.floor(num)) {
        return num.toFixed(0);
      } else {
        return num.toFixed(2);
      }
    }

    return String(value);
  };

  if (!anomalies || anomalies.length === 0) {
    return (
      <div className="text-center py-6 text-gray-500">
        No recent anomalies found
      </div>
    );
  }

  return (
    <div className="overflow-hidden">
      <ul className="divide-y divide-gray-200">
        {anomalies.map((anomaly) => (
          <li key={anomaly.id} className="py-4">
            <Link
              to={`/anomalies/${connectionId}/detail/${anomaly.id}`}
              className="block hover:bg-gray-50 -mx-4 px-4 py-2 rounded-md transition-colors"
            >
              <div className="flex items-center">
                <div className="mr-3 flex-shrink-0">
                  {getSeverityIcon(anomaly.severity)}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex flex-col sm:flex-row sm:justify-between">
                    <p className="text-sm font-medium text-primary-600 truncate">
                      {anomaly.table_name}{anomaly.column_name ? `.${anomaly.column_name}` : ''}
                    </p>
                    <div className="mt-1 sm:mt-0 flex-shrink-0">
                      <p className="text-xs text-gray-500">
                        {formatDate(anomaly.detected_at, true)}
                      </p>
                    </div>
                  </div>
                  <div className="mt-1 flex flex-wrap">
                    <p className="text-sm text-gray-800 mr-3">
                      <span className="font-medium">{anomaly.metric_name}</span>: {formatMetricValue(anomaly.metric_value)}
                    </p>
                    <p className="text-xs bg-gray-100 text-gray-700 px-2 py-1 rounded-full">
                      {anomaly.status}
                    </p>
                  </div>
                </div>
                <div className="ml-3">
                  <ArrowRightIcon className="h-4 w-4 text-gray-400" />
                </div>
              </div>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
};

export default RecentAnomaliesList;