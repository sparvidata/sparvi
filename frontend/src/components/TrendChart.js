import React from 'react';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';

// Register all the necessary components
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

function TrendChart({ data }) {
  // Placeholder data if no trends provided
  const chartData = data || {
    labels: ['Jan', 'Feb', 'Mar', 'Apr'],
    datasets: [{
      label: 'Row Count',
      data: [100, 120, 90, 150],
      borderColor: 'rgba(75,192,192,1)',
      fill: false,
    }],
  };

  return (
    <div className="card mb-3">
      <div className="card-body">
        <h5 className="card-title">Trends</h5>
        <Line data={chartData} />
      </div>
    </div>
  );
}

export default TrendChart;
