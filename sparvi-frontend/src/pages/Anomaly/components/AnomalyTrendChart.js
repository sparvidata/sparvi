// src/components/anomaly/AnomalyTrendChart.js

import React, { useState, useEffect } from 'react';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend
} from 'recharts';
import { formatDate } from '../../../utils/formatting';

const AnomalyTrendChart = ({ data, timeRange }) => {
  const [chartData, setChartData] = useState([]);

  useEffect(() => {
    if (data && data.length > 0) {
      // Process data for chart, ensuring dates are formatted
      const processed = data.map(item => ({
        ...item,
        date: new Date(item.date).toLocaleDateString(),
      }));
      setChartData(processed);
    }
  }, [data]);

  // Handle empty data
  if (!chartData || chartData.length === 0) {
    return (
      <div className="flex items-center justify-center h-72 border border-gray-200 rounded-md bg-gray-50">
        <p className="text-gray-500">No trend data available</p>
      </div>
    );
  }

  return (
    <div className="h-72">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart
          data={chartData}
          margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="date"
            tickFormatter={date => formatDate(date, false)}
            tick={{ fontSize: 12 }}
          />
          <YAxis
            tick={{ fontSize: 12 }}
            allowDecimals={false}
          />
          <Tooltip
            formatter={(value) => [`${value} anomalies`, '']}
            labelFormatter={(label) => formatDate(label, true)}
          />
          <Legend />
          <Area
            type="monotone"
            dataKey="high"
            stackId="1"
            name="High Severity"
            stroke="#f87171"
            fill="#fca5a5"
          />
          <Area
            type="monotone"
            dataKey="medium"
            stackId="1"
            name="Medium Severity"
            stroke="#f59e0b"
            fill="#fcd34d"
          />
          <Area
            type="monotone"
            dataKey="low"
            stackId="1"
            name="Low Severity"
            stroke="#10b981"
            fill="#6ee7b7"
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
};

export default AnomalyTrendChart;