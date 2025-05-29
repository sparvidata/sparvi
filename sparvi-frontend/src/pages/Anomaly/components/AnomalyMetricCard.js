import React from 'react';
import {
  TableCellsIcon,
  ViewColumnsIcon,     // Changed from ColumnIcon to ViewColumnsIcon
  ListBulletIcon,
  ChartBarIcon
} from '@heroicons/react/24/outline';

const AnomalyMetricCard = ({ name, count, type, onClick }) => {
  // Helper to get icon based on type
  const getIcon = () => {
    switch (type) {
      case 'table':
        return <TableCellsIcon className="h-5 w-5 text-primary-500" />;
      case 'column':
        return <ViewColumnsIcon className="h-5 w-5 text-primary-500" />; // Updated here too
      case 'metric':
        return <ChartBarIcon className="h-5 w-5 text-primary-500" />;
      default:
        return <ListBulletIcon className="h-5 w-5 text-primary-500" />;
    }
  };

  return (
    <button
      onClick={onClick}
      className="w-full flex items-center p-3 border border-gray-200 rounded-md hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 transition-colors"
    >
      <div className="flex-shrink-0">
        {getIcon()}
      </div>
      <div className="ml-3 flex-1">
        <p className="text-sm font-medium text-gray-900 truncate">{name}</p>
      </div>
      <div className="ml-2 flex-shrink-0">
        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-primary-100 text-primary-800">
          {count}
        </span>
      </div>
    </button>
  );
};

export default AnomalyMetricCard;