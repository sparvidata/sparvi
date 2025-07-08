import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const ValidationDetailCharts = ({ data = [] }) => {
  // Filter out days with zero validations
  const filteredData = data.filter(day => day.total_validations > 0);

  // Format date for display
  const formatXAxis = (dateStr) => {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
  };

  // Simple tooltip component
  const SimpleTooltip = ({ active, payload, label, metricName, formatValue }) => {
    if (active && payload && payload.length) {
      const value = payload[0].value;
      return (
        <div className="bg-white p-3 border border-secondary-200 shadow-lg rounded-md">
          <p className="font-medium text-secondary-900">{formatXAxis(label)}</p>
          <p className="text-sm text-secondary-600">
            {metricName}: {formatValue ? formatValue(value) : value}
          </p>
        </div>
      );
    }
    return null;
  };

  const sparklineHeight = 120;

  return (
    <div className="space-y-6">
      <h4 className="text-sm font-medium text-secondary-900 mb-4">Individual Metric Trends</h4>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Passed Validations Sparkline */}
        <div className="bg-accent-50 p-4 rounded-lg">
          <h5 className="text-sm font-medium text-accent-800 mb-2">Passed Validations</h5>
          <div style={{ height: sparklineHeight }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={filteredData} margin={{ top: 5, right: 5, left: 5, bottom: 5 }}>
                <XAxis dataKey="day" hide />
                <YAxis hide />
                <Tooltip
                  content={(props) =>
                    <SimpleTooltip
                      {...props}
                      metricName="Passed"
                    />
                  }
                />
                <Line
                  type="monotone"
                  dataKey="passed"
                  stroke="#10b981"
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4, fill: '#10b981' }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-2 text-xs text-accent-700">
            Latest: {filteredData[filteredData.length - 1]?.passed || 0}
          </div>
        </div>

        {/* Failed Validations Sparkline */}
        <div className="bg-danger-50 p-4 rounded-lg">
          <h5 className="text-sm font-medium text-danger-800 mb-2">Failed Validations</h5>
          <div style={{ height: sparklineHeight }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={filteredData} margin={{ top: 5, right: 5, left: 5, bottom: 5 }}>
                <XAxis dataKey="day" hide />
                <YAxis hide />
                <Tooltip
                  content={(props) =>
                    <SimpleTooltip
                      {...props}
                      metricName="Failed"
                    />
                  }
                />
                <Line
                  type="monotone"
                  dataKey="failed"
                  stroke="#ef4444"
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4, fill: '#ef4444' }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-2 text-xs text-danger-700">
            Latest: {filteredData[filteredData.length - 1]?.failed || 0}
          </div>
        </div>

        {/* Not Run Validations Sparkline */}
        <div className="bg-secondary-50 p-4 rounded-lg">
          <h5 className="text-sm font-medium text-secondary-800 mb-2">Not Run</h5>
          <div style={{ height: sparklineHeight }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={filteredData} margin={{ top: 5, right: 5, left: 5, bottom: 5 }}>
                <XAxis dataKey="day" hide />
                <YAxis hide />
                <Tooltip
                  content={(props) =>
                    <SimpleTooltip
                      {...props}
                      metricName="Not Run"
                    />
                  }
                />
                <Line
                  type="monotone"
                  dataKey="not_run"
                  stroke="#64748b"
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4, fill: '#64748b' }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-2 text-xs text-secondary-700">
            Latest: {filteredData[filteredData.length - 1]?.not_run || 0}
          </div>
        </div>

        {/* Total Validations Sparkline */}
        <div className="bg-primary-50 p-4 rounded-lg">
          <h5 className="text-sm font-medium text-primary-800 mb-2">Total Validations</h5>
          <div style={{ height: sparklineHeight }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={filteredData} margin={{ top: 5, right: 5, left: 5, bottom: 5 }}>
                <XAxis dataKey="day" hide />
                <YAxis hide />
                <Tooltip
                  content={(props) =>
                    <SimpleTooltip
                      {...props}
                      metricName="Total"
                    />
                  }
                />
                <Line
                  type="monotone"
                  dataKey="total_validations"
                  stroke="#6366f1"
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4, fill: '#6366f1' }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-2 text-xs text-primary-700">
            Latest: {filteredData[filteredData.length - 1]?.total_validations || 0}
          </div>
        </div>
      </div>

      {/* Data insights */}
      <div className="bg-secondary-50 p-4 rounded-lg">
        <h5 className="text-sm font-medium text-secondary-800 mb-2">Insights</h5>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs">
          <div>
            <span className="font-medium">Peak Validations:</span>
            <br />
            {Math.max(...filteredData.map(d => d.total_validations))} validations
          </div>
          <div>
            <span className="font-medium">Best Health Score:</span>
            <br />
            {Math.max(...filteredData.map(d => d.health_score))}%
          </div>
          <div>
            <span className="font-medium">Worst Health Score:</span>
            <br />
            {Math.min(...filteredData.map(d => d.health_score))}%
          </div>
        </div>
      </div>
    </div>
  );
};

export default ValidationDetailCharts;