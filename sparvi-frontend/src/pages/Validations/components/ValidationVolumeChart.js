import React from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';

const ValidationVolumeChart = ({
  data = [],
  title = "Validation Results Volume"
}) => {
  // Filter out days with zero validations
  const filteredData = data.filter(day => day.total_validations > 0);

  // Format date for display
  const formatXAxis = (dateStr) => {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
  };

  // Custom tooltip
  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      const total = data.total_validations;
      const passed = data.passed || 0;
      const failed = data.failed || 0;
      const notRun = data.not_run || 0;

      return (
        <div className="bg-white p-3 border border-secondary-200 shadow-lg rounded-md">
          <p className="font-medium text-secondary-900 mb-2">{formatXAxis(label)}</p>
          <div className="space-y-1">
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <div className="w-3 h-3 bg-accent-500 rounded-full mr-2"></div>
                <span className="text-sm">Passed:</span>
              </div>
              <span className="text-sm font-medium">{passed}</span>
            </div>
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <div className="w-3 h-3 bg-danger-500 rounded-full mr-2"></div>
                <span className="text-sm">Failed:</span>
              </div>
              <span className="text-sm font-medium">{failed}</span>
            </div>
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <div className="w-3 h-3 bg-secondary-400 rounded-full mr-2"></div>
                <span className="text-sm">Not Run:</span>
              </div>
              <span className="text-sm font-medium">{notRun}</span>
            </div>
            <div className="border-t border-secondary-200 pt-1 mt-2">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium">Total:</span>
                <span className="text-sm font-bold">{total}</span>
              </div>
            </div>
          </div>
        </div>
      );
    }
    return null;
  };

  // Calculate summary stats
  const totalValidations = filteredData.reduce((sum, day) => sum + day.total_validations, 0);
  const avgDailyValidations = filteredData.length > 0
    ? Math.round(totalValidations / filteredData.length)
    : 0;

  return (
    <div className="bg-white p-6 rounded-lg shadow">
      {/* Header with summary stats */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h3 className="text-lg font-medium text-secondary-900">{title}</h3>
          <p className="text-sm text-secondary-500 mt-1">
            {totalValidations.toLocaleString()} total validations • {avgDailyValidations} average per day
          </p>
        </div>
      </div>

      {/* Chart */}
      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart
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
              tick={{ fontSize: 12, fill: '#64748b' }}
              axisLine={{ stroke: '#e2e8f0' }}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend
              wrapperStyle={{ fontSize: '12px' }}
              iconType="circle"
            />

            {/* Stacked areas */}
            <Area
              type="monotone"
              dataKey="not_run"
              stackId="1"
              stroke="#94a3b8"
              fill="#94a3b8"
              fillOpacity={0.8}
              name="Not Run"
            />
            <Area
              type="monotone"
              dataKey="failed"
              stackId="1"
              stroke="#ef4444"
              fill="#ef4444"
              fillOpacity={0.8}
              name="Failed"
            />
            <Area
              type="monotone"
              dataKey="passed"
              stackId="1"
              stroke="#10b981"
              fill="#10b981"
              fillOpacity={0.8}
              name="Passed"
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Volume insights */}
      <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-4 text-center">
        <div className="bg-accent-50 p-3 rounded-lg">
          <div className="text-lg font-bold text-accent-600">
            {filteredData.reduce((sum, day) => sum + (day.passed || 0), 0).toLocaleString()}
          </div>
          <div className="text-xs text-accent-700">Total Passed</div>
        </div>
        <div className="bg-danger-50 p-3 rounded-lg">
          <div className="text-lg font-bold text-danger-600">
            {filteredData.reduce((sum, day) => sum + (day.failed || 0), 0).toLocaleString()}
          </div>
          <div className="text-xs text-danger-700">Total Failed</div>
        </div>
        <div className="bg-secondary-50 p-3 rounded-lg">
          <div className="text-lg font-bold text-secondary-600">
            {filteredData.reduce((sum, day) => sum + (day.not_run || 0), 0).toLocaleString()}
          </div>
          <div className="text-xs text-secondary-700">Total Not Run</div>
        </div>
      </div>

      {/* Data summary */}
      <div className="mt-4 text-center text-xs text-secondary-500">
        Showing validation volume across {filteredData.length} active days
      </div>
    </div>
  );
};

export default ValidationVolumeChart;