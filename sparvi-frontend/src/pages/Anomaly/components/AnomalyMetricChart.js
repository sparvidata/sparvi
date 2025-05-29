import React, { useState, useEffect } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceDot,
  ResponsiveContainer
} from 'recharts';
import { formatDate } from '../../../utils/formatting';

const AnomalyMetricChart = ({ metrics, anomalyValue, anomalyTimestamp }) => {
  const [chartData, setChartData] = useState([]);
  const [anomalyPoint, setAnomalyPoint] = useState(null);

  useEffect(() => {
    if (!metrics || metrics.length === 0) return;

    // Process data for chart
    const formattedData = metrics.map(metric => ({
      timestamp: new Date(metric.timestamp).getTime(),
      value: typeof metric.metric_value === 'number'
        ? metric.metric_value
        : parseFloat(metric.metric_value) || parseFloat(metric.metric_text) || 0
    }))
    .sort((a, b) => a.timestamp - b.timestamp);

    setChartData(formattedData);

    // Find where anomaly fits in timeline
    if (anomalyTimestamp && anomalyValue) {
      const anomalyTime = new Date(anomalyTimestamp).getTime();

      setAnomalyPoint({
        timestamp: anomalyTime,
        value: typeof anomalyValue === 'number'
          ? anomalyValue
          : parseFloat(anomalyValue) || 0
      });
    }
  }, [metrics, anomalyValue, anomalyTimestamp]);

  // Format tooltip display
  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-2 shadow-md border border-gray-200 rounded-md">
          <p className="text-sm font-medium">{formatDate(new Date(label), true)}</p>
          <p className="text-sm text-primary-600">
            Value: <span className="font-medium">{payload[0].value}</span>
          </p>
        </div>
      );
    }
    return null;
  };

  // Check if we have valid data to display
  if (!chartData || chartData.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-gray-500">
        No metric history available
      </div>
    );
  }

  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={chartData} margin={{ top: 10, right: 30, left: 10, bottom: 10 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
        <XAxis
          dataKey="timestamp"
          tickFormatter={(timestamp) => formatDate(new Date(timestamp), false)}
          type="number"
          domain={['dataMin', 'dataMax']}
          tick={{ fontSize: 12 }}
        />
        <YAxis tick={{ fontSize: 12 }} />
        <Tooltip content={<CustomTooltip />} />
        <Line
          type="monotone"
          dataKey="value"
          stroke="#6366f1"
          strokeWidth={2}
          dot={{ r: 4, fill: '#6366f1' }}
          activeDot={{ r: 6, fill: '#6366f1', stroke: '#ffffff', strokeWidth: 2 }}
        />

        {/* Highlight the anomaly point if available */}
        {anomalyPoint && (
          <ReferenceDot
            x={anomalyPoint.timestamp}
            y={anomalyPoint.value}
            r={6}
            fill="#ef4444"
            stroke="#ffffff"
            strokeWidth={2}
          />
        )}
      </LineChart>
    </ResponsiveContainer>
  );
};

export default AnomalyMetricChart;