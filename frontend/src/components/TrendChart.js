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
  Filler,
} from 'chart.js';

// Register all the necessary components
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

function TrendChart({ title, labels, datasets, height = 300, subtitle }) {
  console.log(`Rendering TrendChart: ${title}`);
  console.log("Labels:", labels);
  console.log("Datasets:", datasets);

  // Check if we have valid data to display
  const hasData = labels && labels.length > 1 && datasets && datasets.length > 0;
  console.log(`hasData: ${hasData}`);

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
    spanGaps: true, // Connects lines across missing (null/undefined) data points
    plugins: {
      legend: {
        position: 'top',
        labels: {
          boxWidth: 12,
          padding: 10,
          // Limit the number of legends displayed if we have a lot of columns
          filter: (item, chart) => {
            if (datasets && datasets.length > 10) {
              // Only show legend for datasets with some non-zero values
              const dataset = datasets.find(d => d.label === item.text);
              return dataset && dataset.data.some(v => v > 0);
            }
            return true;
          }
        }
      },
      tooltip: {
        mode: 'index',
        intersect: false,
        callbacks: {
          title: function(tooltipItems) {
            return tooltipItems[0].label;
          },
          label: function(context) {
            let label = context.dataset.label || '';
            if (label) {
                label += ': ';
            }
            if (context.parsed.y !== null) {
                if (title.toLowerCase().includes('percentage') || title.toLowerCase().includes('rate')) {
                    label += context.parsed.y.toFixed(1) + '%';
                } else if (context.parsed.y >= 1000) {
                    label += context.parsed.y.toLocaleString();
                } else {
                    label += context.parsed.y;
                }
            }
            return label;
          }
        }
      },
      title: {
        display: !!subtitle,
        text: subtitle || '',
        font: {
          size: 14
        }
      },
    },
    scales: {
      y: {
        beginAtZero: title.toLowerCase().includes('rate') ||
                     title.toLowerCase().includes('percentage') ||
                     (datasets && datasets[0]?.data?.every(val => val >= 0)),
        ticks: {
          callback: function(value) {
            // Format y-axis labels based on dataset type
            if (title.toLowerCase().includes('percentage') ||
                title.toLowerCase().includes('rate')) {
              return value + '%';
            }
            // For large numbers, format with k/M/B suffixes
            if (value >= 1000000) {
              return (value / 1000000).toFixed(1) + 'M';
            } else if (value >= 1000) {
              return (value / 1000).toFixed(1) + 'k';
            }
            return value;
          }
        }
      },
      x: {
        grid: {
          display: false
        }
      }
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
        {hasData ? (
          <div style={{ height: `${height}px` }}>
            <Line data={chartData} options={options} />
          </div>
        ) : (
          <div className="alert alert-info">
            <i className="bi bi-info-circle-fill me-2"></i>
            Not enough historical data available for trend visualization. Run the profiler multiple times to generate trend data.
          </div>
        )}
      </div>
    </div>
  );
}

export default TrendChart;