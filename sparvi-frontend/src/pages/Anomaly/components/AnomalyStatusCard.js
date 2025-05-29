import React from 'react';
import { formatNumber } from '../../../utils/formatting';

const AnomalyStatusCard = ({ title, value, subtitle, icon: Icon, iconColor, bgColor }) => {
  return (
    <div className={`${bgColor} overflow-hidden shadow rounded-lg`}>
      <div className="p-5">
        <div className="flex items-center">
          <div className={`flex-shrink-0 ${iconColor}`}>
            <Icon className="h-6 w-6" aria-hidden="true" />
          </div>
          <div className="ml-5 w-0 flex-1">
            <dl>
              <dt className="text-sm font-medium text-gray-500 truncate">{title}</dt>
              <dd>
                <div className="text-3xl font-semibold text-gray-900">{formatNumber(value)}</div>
              </dd>
              {subtitle && (
                <dd className="mt-1 text-sm text-gray-500">{subtitle}</dd>
              )}
            </dl>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AnomalyStatusCard;