import React, { useState, useEffect } from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';

const AnomalySeverityChart = ({ highCount, mediumCount, lowCount }) => {
  const [chartData, setChartData] = useState([]);

  useEffect(() => {
    // Create chart data
    const data = [
      { name: 'High', value: highCount, color: '#ef4444' },
      { name: 'Medium', value: mediumCount, color: '#f59e0b' },
      { name: 'Low', value: lowCount, color: '#10b981' }
    ].filter(item => item.value > 0);

    setChartData(data);
  }, [highCount, mediumCount, lowCount]);

  // Handle empty data
  if (chartData.length === 0 || (highCount === 0 && mediumCount === 0 && lowCount === 0)) {
    return (
      <div className="flex items-center justify-center h-72 border border-gray-200 rounded-md bg-gray-50">
        <p className="text-gray-500">No severity data available</p>
      </div>
    );
  }

  return (
    <div className="h-72">
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={chartData}
            cx="50%"
            cy="50%"
            labelLine={false}
            innerRadius={60}
            outerRadius={80}
            paddingAngle={5}
            dataKey="value"
            label={({ name, value, percent }) => `${name}: ${value} (${(percent * 100).toFixed(0)}%)`}
          >
            {chartData.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={entry.color} />
            ))}
          </Pie>
          <Tooltip formatter={(value) => [`${value} anomalies`, '']} />
          <Legend />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
};

export default AnomalySeverityChart;