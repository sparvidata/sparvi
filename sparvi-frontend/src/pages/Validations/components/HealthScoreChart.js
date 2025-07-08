import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';
import { ArrowUpIcon, ArrowDownIcon, MinusIcon } from '@heroicons/react/24/solid';

const HealthScoreChart = ({
  data = [],
  title = "Health Score Over Time",
  currentScore = 0,
  trend = 0
}) => {
  // Filter out days with zero validations
  const filteredData = data.filter(day => day.total_validations > 0);

  // Format date for display
  const formatXAxis = (dateStr) => {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
  };

  // Get trend indicator
  const getTrendIndicator = () => {
    if (trend > 0) {
      return {
        icon: ArrowUpIcon,
        color: 'text-accent-600',
        text: `+${trend.toFixed(1)}%`
      };
    } else if (trend < 0) {
      return {
        icon: ArrowDownIcon,
        color: 'text-danger-600',
        text: `${trend.toFixed(1)}%`
      };
    } else {
      return {
        icon: MinusIcon,
        color: 'text-secondary-500',
        text: 'No change'
      };
    }
  };

  // Get health score color
  const getHealthScoreColor = (score) => {
    if (score >= 80) return '#10b981'; // green
    if (score >= 60) return '#f59e0b'; // yellow
    return '#ef4444'; // red
  };

  // Custom tooltip
  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white p-3 border border-secondary-200 shadow-lg rounded-md">
          <p className="font-medium text-secondary-900">{formatXAxis(label)}</p>
          <p className="text-lg font-bold" style={{ color: getHealthScoreColor(data.health_score) }}>
            {data.health_score}% Health Score
          </p>
          <div className="mt-2 text-xs text-secondary-600">
            <p>Passed: {data.passed || 0}</p>
            <p>Failed: {data.failed || 0}</p>
            <p>Total: {data.total_validations}</p>
          </div>
        </div>
      );
    }
    return null;
  };

  const trendIndicator = getTrendIndicator();
  const TrendIcon = trendIndicator.icon;

  return (
    <div className="bg-white p-6 rounded-lg shadow">
      {/* Header with current score and trend */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h3 className="text-lg font-medium text-secondary-900">{title}</h3>
          <div className="mt-1 flex items-center space-x-4">
            <div className="flex items-center">
              <span
                className="text-3xl font-bold"
                style={{ color: getHealthScoreColor(currentScore) }}
              >
                {currentScore}%
              </span>
              <span className="ml-2 text-sm text-secondary-500">current</span>
            </div>
            <div className={`flex items-center ${trendIndicator.color}`}>
              <TrendIcon className="h-4 w-4 mr-1" />
              <span className="text-sm font-medium">{trendIndicator.text}</span>
            </div>
          </div>
        </div>

        {/* Health Status Badge */}
        <div className={`px-3 py-1 rounded-full text-sm font-medium ${
          currentScore >= 80 
            ? 'bg-accent-100 text-accent-800' 
            : currentScore >= 60 
            ? 'bg-warning-100 text-warning-800'
            : 'bg-danger-100 text-danger-800'
        }`}>
          {currentScore >= 80 ? 'Excellent' : currentScore >= 60 ? 'Good' : 'Needs Attention'}
        </div>
      </div>

      {/* Chart */}
      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={filteredData}
            margin={{ top: 5, right: 20, left: 10, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
            <XAxis
              dataKey="day"
              tickFormatter={formatXAxis}
              tick={{ fontSize: 12, fill: '#64748b' }}
              axisLine={{ stroke: '#e2e8f0' }}
            />
            <YAxis
              domain={[0, 100]}
              tickFormatter={(value) => `${value}%`}
              tick={{ fontSize: 12, fill: '#64748b' }}
              axisLine={{ stroke: '#e2e8f0' }}
            />
            <Tooltip content={<CustomTooltip />} />

            {/* Reference lines for health zones */}
            <ReferenceLine y={80} stroke="#10b981" strokeDasharray="2 2" strokeOpacity={0.5} />
            <ReferenceLine y={60} stroke="#f59e0b" strokeDasharray="2 2" strokeOpacity={0.5} />

            <Line
              type="monotone"
              dataKey="health_score"
              stroke="#6366f1"
              strokeWidth={3}
              dot={{ fill: '#6366f1', strokeWidth: 2, r: 4 }}
              activeDot={{
                r: 6,
                fill: '#6366f1',
                stroke: '#ffffff',
                strokeWidth: 2,
                filter: 'drop-shadow(0 2px 4px rgba(0,0,0,0.1))'
              }}
              connectNulls={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Health zones legend */}
      <div className="mt-4 flex items-center justify-center space-x-6 text-xs text-secondary-500">
        <div className="flex items-center">
          <div className="w-3 h-0.5 bg-accent-500 mr-2"></div>
          <span>Excellent (80-100%)</span>
        </div>
        <div className="flex items-center">
          <div className="w-3 h-0.5 bg-warning-500 mr-2"></div>
          <span>Good (60-79%)</span>
        </div>
        <div className="flex items-center">
          <div className="w-3 h-0.5 bg-danger-500 mr-2"></div>
          <span>Needs Attention (&lt;60%)</span>
        </div>
      </div>

      {/* Data summary */}
      <div className="mt-4 text-center text-xs text-secondary-500">
        Showing {filteredData.length} days with validation activity
      </div>
    </div>
  );
};

export default HealthScoreChart;