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

function TrendChart({ title, labels, datasets }) {
  // Format data for Chart.js
  const chartData = {
    labels: labels || ['No Data Available'],
    datasets: datasets || [
      {
        label: 'No Data',
        data: [0],
        borderColor: 'rgba(200,200,200,1)',
        backgroundColor: 'rgba(200,200,200,0.2)',
      },
    ],
  };

  // Chart options
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top',
      },
      tooltip: {
        mode: 'index',
        intersect: false,
      },
      title: {
        display: false,
      },
    },
    scales: {
      y: {
        beginAtZero: datasets && datasets[0]?.data?.every(val => val >= 0),
      },
    },
    elements: {
      line: {
        tension: 0.3, // Smooth curves
      },
      point: {
        radius: 3,
        hitRadius: 10,
        hoverRadius: 5,
      },
    },
    interaction: {
      mode: 'nearest',
      axis: 'x',
      intersect: false
    }
  };

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header">
        <h5 className="mb-0">{title || 'Trend Analysis'}</h5>
      </div>
      <div className="card-body">
        {labels && labels.length > 1 ? (
          <div style={{ height: '300px' }}>
            <Line data={chartData} options={options} />
          </div>
        ) : (
          <div className="alert alert-info">
            <i className="bi bi-info-circle-fill me-2"></i>
            Not enough historical data available for trend visualization.
          </div>
        )}
      </div>
    </div>
  );
}

export default TrendChart;