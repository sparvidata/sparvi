import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  AreaChart,
  Area
} from 'recharts';
import { formatDate, formatNumber, formatPercentage } from '../../utils/formatting';

/**
 * A reusable chart component for displaying trend data
 */
const TrendChart = ({
  data = [],
  xKey = 'timestamp',
  yKey = 'value',
  title,
  type = 'line', // 'line', 'area'
  color = '#6366f1', // primary color default
  fillOpacity = 0.2,
  height = 200,
  width = '100%',
  showXAxis = true,
  showYAxis = true,
  showGrid = true,
  showTooltip = true,
  dateFormat = true, // Format x-axis as date
  valueFormat = 'number', // 'number', 'percentage', 'currency'
  loading = false,
  emptyMessage = 'No data available',
  className = ''
}) => {
  // Format the tooltip value based on the specified format
  const formatValue = (value) => {
    if (value === null || value === undefined) return '-';

    switch (valueFormat) {
      case 'percentage':
        return formatPercentage(value, 1);
      case 'currency':
        return `$${formatNumber(value, 2)}`;
      case 'number':
      default:
        return formatNumber(value, 1);
    }
  };

  // Format x-axis labels
  const formatXAxis = (value) => {
    if (dateFormat) {
      return formatDate(value, false);
    }
    return value;
  };

  // Custom tooltip component
  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-2 border border-secondary-200 shadow-md rounded text-sm">
          <p className="font-medium text-secondary-900">
            {dateFormat ? formatDate(label, true) : label}
          </p>
          <p className="text-primary-600">
            {formatValue(payload[0].value)}
          </p>
        </div>
      );
    }
    return null;
  };

  // If data is loading
  if (loading) {
    return (
      <div className={`bg-white rounded-lg shadow p-4 ${className}`}>
        {title && <h3 className="text-md font-medium text-secondary-900 mb-2">{title}</h3>}
        <div className="h-[200px] w-full flex items-center justify-center bg-secondary-50 rounded animate-pulse">
          <div className="text-secondary-400">Loading...</div>
        </div>
      </div>
    );
  }

  // If no data is available
  if (!data || data.length === 0) {
    return (
      <div className={`bg-white rounded-lg shadow p-4 ${className}`}>
        {title && <h3 className="text-md font-medium text-secondary-900 mb-2">{title}</h3>}
        <div className="h-[200px] w-full flex items-center justify-center bg-secondary-50 rounded">
          <div className="text-secondary-500">{emptyMessage}</div>
        </div>
      </div>
    );
  }

  return (
    <div className={`bg-white rounded-lg shadow p-4 ${className}`}>
      {title && <h3 className="text-md font-medium text-secondary-900 mb-2">{title}</h3>}
      <div style={{ height: height, width: width }}>
        <ResponsiveContainer width="100%" height="100%">
          {type === 'area' ? (
            <AreaChart data={data} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
              {showGrid && <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />}
              {showXAxis && (
                <XAxis
                  dataKey={xKey}
                  tickFormatter={formatXAxis}
                  tick={{ fill: '#64748b', fontSize: 12 }}
                  stroke="#e2e8f0"
                  tickMargin={10}
                />
              )}
              {showYAxis && (
                <YAxis
                  tickFormatter={(value) => formatValue(value)}
                  tick={{ fill: '#64748b', fontSize: 12 }}
                  stroke="#e2e8f0"
                  tickMargin={10}
                />
              )}
              {showTooltip && <Tooltip content={<CustomTooltip />} />}
              <Area
                type="monotone"
                dataKey={yKey}
                stroke={color}
                fill={color}
                fillOpacity={fillOpacity}
                strokeWidth={2}
              />
            </AreaChart>
          ) : (
            <LineChart data={data} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
              {showGrid && <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />}
              {showXAxis && (
                <XAxis
                  dataKey={xKey}
                  tickFormatter={formatXAxis}
                  tick={{ fill: '#64748b', fontSize: 12 }}
                  stroke="#e2e8f0"
                  tickMargin={10}
                />
              )}
              {showYAxis && (
                <YAxis
                  tickFormatter={(value) => formatValue(value)}
                  tick={{ fill: '#64748b', fontSize: 12 }}
                  stroke="#e2e8f0"
                  tickMargin={10}
                />
              )}
              {showTooltip && <Tooltip content={<CustomTooltip />} />}
              <Line
                type="monotone"
                dataKey={yKey}
                stroke={color}
                strokeWidth={2}
                dot={{ fill: color, strokeWidth: 2 }}
                activeDot={{ r: 6, fill: color, stroke: '#ffffff', strokeWidth: 2 }}
              />
            </LineChart>
          )}
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default TrendChart;