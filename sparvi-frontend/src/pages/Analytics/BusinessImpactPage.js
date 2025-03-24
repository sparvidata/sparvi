import React, { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useConnection } from '../../contexts/EnhancedConnectionContext';
import { useUI } from '../../contexts/UIContext';
import { useHistoricalMetrics } from '../../hooks/useAnalytics';
import {
  ArrowLeftIcon,
  BuildingOfficeIcon,
  ClockIcon,
  CurrencyDollarIcon,
  ShieldCheckIcon
} from '@heroicons/react/24/outline';
import TrendChart from '../../components/analytics/TrendChart';
import MetricCard from '../../components/analytics/MetricCard';
import Heatmap from '../../components/analytics/Heatmap';
import LoadingSpinner from '../../components/common/LoadingSpinner';
import { formatNumber, formatPercentage } from '../../utils/formatting';

const BusinessImpactPage = () => {
  const { connectionId } = useParams();
  const { activeConnection, getConnection } = useConnection();
  const { updateBreadcrumbs } = useUI();
  const [timeframe, setTimeframe] = useState(30); // Default to 30 days
  const [businessData, setBusinessData] = useState(null);
  const [isLoading, setIsLoading] = useState(false);

  // Fetch quality score metrics
  const {
    data: qualityScoreData,
    isLoading: isQualityScoreLoading
  } = useHistoricalMetrics(
    connectionId,
    {
      metricName: 'quality_score',
      days: timeframe,
      enabled: !!connectionId
    }
  );

  // Fetch trust score metrics
  const {
    data: trustScoreData,
    isLoading: isTrustScoreLoading
  } = useHistoricalMetrics(
    connectionId,
    {
      metricName: 'trust_score',
      days: timeframe,
      enabled: !!connectionId
    }
  );

  // Load connection and business impact data
  useEffect(() => {
    // Ensure we have an active connection matching the URL parameter
    const fetchConnectionIfNeeded = async () => {
      if (!activeConnection || activeConnection.id !== connectionId) {
        try {
          await getConnection(connectionId);
        } catch (error) {
          console.error('Error fetching connection:', error);
        }
      }
    };

    // Simulate fetching business impact data
    // In a real implementation, this would be an API call
    const fetchBusinessData = async () => {
      setIsLoading(true);
      try {
        // This is placeholder data - in a real app, this would come from your API
        setTimeout(() => {
          setBusinessData({
            // Cost metrics
            estimated_cost_savings: 28500,
            cost_prevention: 42000,
            team_efficiency_gain: 15.3,

            // Time metrics
            avg_resolution_time: 8.2, // in hours
            avg_detection_time: 2.3, // in hours

            // Trust metrics
            overall_trust_score: 87.2,
            data_usage: 95.3,

            // Department impacts - for heatmap
            department_impact: [
              { row: 'Finance', col: 'Data Accuracy', value: 0.95 },
              { row: 'Finance', col: 'Data Completeness', value: 0.88 },
              { row: 'Finance', col: 'Data Timeliness', value: 0.91 },

              { row: 'Marketing', col: 'Data Accuracy', value: 0.82 },
              { row: 'Marketing', col: 'Data Completeness', value: 0.79 },
              { row: 'Marketing', col: 'Data Timeliness', value: 0.86 },

              { row: 'Sales', col: 'Data Accuracy', value: 0.89 },
              { row: 'Sales', col: 'Data Completeness', value: 0.84 },
              { row: 'Sales', col: 'Data Timeliness', value: 0.94 },

              { row: 'Product', col: 'Data Accuracy', value: 0.93 },
              { row: 'Product', col: 'Data Completeness', value: 0.87 },
              { row: 'Product', col: 'Data Timeliness', value: 0.81 },
            ]
          });
          setIsLoading(false);
        }, 1000);
      } catch (error) {
        console.error('Error fetching business impact data:', error);
        setIsLoading(false);
      }
    };

    fetchConnectionIfNeeded();
    if (connectionId) {
      fetchBusinessData();
    }
  }, [connectionId, activeConnection, getConnection]);

  // Update breadcrumbs
  useEffect(() => {
    updateBreadcrumbs([
      { name: 'Analytics', href: '/analytics' },
      { name: 'Business Impact', href: `/analytics/business-impact/${connectionId}` },
    ]);
  }, [updateBreadcrumbs, connectionId]);

  // Handle timeframe change
  const handleTimeframeChange = (days) => {
    setTimeframe(days);
  };

  if (isLoading && !businessData) {
    return (
      <div className="flex justify-center items-center h-64">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <div className="flex items-center mb-4">
          <Link to="/analytics" className="mr-4 text-secondary-500 hover:text-secondary-700">
            <ArrowLeftIcon className="h-5 w-5" />
          </Link>
          <h1 className="text-2xl font-bold text-secondary-900">
            Business Impact
          </h1>
        </div>

        <div className="flex justify-between items-center">
          <p className="text-secondary-500">
            Connecting data quality to business outcomes for {activeConnection?.name || 'this connection'}
          </p>

          {/* Time range selector */}
          <div className="flex items-center space-x-2 bg-white rounded-md shadow-sm p-1">
            <button
              onClick={() => handleTimeframeChange(7)}
              className={`px-3 py-1.5 text-sm font-medium rounded-md ${
                timeframe === 7
                  ? 'bg-primary-100 text-primary-700'
                  : 'text-secondary-500 hover:text-secondary-700'
              }`}
            >
              7 Days
            </button>
            <button
              onClick={() => handleTimeframeChange(30)}
              className={`px-3 py-1.5 text-sm font-medium rounded-md ${
                timeframe === 30
                  ? 'bg-primary-100 text-primary-700'
                  : 'text-secondary-500 hover:text-secondary-700'
              }`}
            >
              30 Days
            </button>
            <button
              onClick={() => handleTimeframeChange(90)}
              className={`px-3 py-1.5 text-sm font-medium rounded-md ${
                timeframe === 90
                  ? 'bg-primary-100 text-primary-700'
                  : 'text-secondary-500 hover:text-secondary-700'
              }`}
            >
              90 Days
            </button>
          </div>
        </div>
      </div>

      {/* Business Value Metrics */}
      <div>
        <h2 className="text-lg font-medium text-secondary-900 mb-4">Business Value</h2>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <MetricCard
            title="Estimated Cost Savings"
            value={businessData?.estimated_cost_savings}
            format="currency"
            icon={CurrencyDollarIcon}
            isLoading={isLoading}
            size="large"
          />

          <MetricCard
            title="Issue Prevention Value"
            value={businessData?.cost_prevention}
            format="currency"
            icon={ShieldCheckIcon}
            isLoading={isLoading}
            size="large"
          />

          <MetricCard
            title="Team Efficiency Gain"
            value={businessData?.team_efficiency_gain}
            format="percentage"
            icon={BuildingOfficeIcon}
            isLoading={isLoading}
            size="large"
          />
        </div>
      </div>

      {/* Time Metrics */}
      <div>
        <h2 className="text-lg font-medium text-secondary-900 mb-4">Time Metrics</h2>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <MetricCard
            title="Average Issue Resolution Time"
            value={businessData?.avg_resolution_time}
            format="number"
            precision={1}
            trendLabel="hours"
            icon={ClockIcon}
            isLoading={isLoading}
            inverse={true} // Lower is better
            size="large"
          />

          <MetricCard
            title="Average Issue Detection Time"
            value={businessData?.avg_detection_time}
            format="number"
            precision={1}
            trendLabel="hours"
            icon={ClockIcon}
            isLoading={isLoading}
            inverse={true} // Lower is better
            size="large"
          />
        </div>
      </div>

      {/* Trust Score Trend */}
      <div>
        <h2 className="text-lg font-medium text-secondary-900 mb-4">Trust Score Trend</h2>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <TrendChart
            title="Data Quality Score"
            data={qualityScoreData?.metrics || []}
            yKey="metric_value"
            type="area"
            color="#6366f1"
            valueFormat="percentage"
            loading={isQualityScoreLoading}
            height={250}
          />

          <TrendChart
            title="Trust Score"
            data={trustScoreData?.metrics || []}
            yKey="metric_value"
            type="area"
            color="#10b981"
            valueFormat="percentage"
            loading={isTrustScoreLoading}
            height={250}
          />
        </div>
      </div>

      {/* Department Impact Heatmap */}
      <div>
        <h2 className="text-lg font-medium text-secondary-900 mb-4">Department Impact</h2>

        <Heatmap
          data={businessData?.department_impact || []}
          rowKey="row"
          colKey="col"
          valueKey="value"
          colorScale={(value) => {
            // Green color scale
            const intensity = Math.min(0.9, Math.max(0.1, value));
            return `rgba(16, 185, 129, ${intensity})`; // Accent color
          }}
          formatValue={(value) => formatPercentage(value * 100, 0)}
          loading={isLoading}
          height={300}
          cellSize={{ width: 120, height: 60 }}
        />
      </div>

      {/* Business Recommendations */}
      <div>
        <h2 className="text-lg font-medium text-secondary-900 mb-4">Business Recommendations</h2>

        <div className="bg-white rounded-lg shadow p-6">
          <ul className="space-y-4">
            <li className="flex">
              <div className="flex-shrink-0">
                <div className="flex items-center justify-center h-8 w-8 rounded-full bg-primary-100 text-primary-600">
                  1
                </div>
              </div>
              <div className="ml-4">
                <h3 className="text-sm font-medium text-secondary-900">Improve Finance Data Completeness</h3>
                <p className="mt-1 text-sm text-secondary-500">
                  Focus on improving data completeness in Finance tables, which has the lowest score (88%).
                  This could yield an estimated $12,000 in additional annual savings.
                </p>
              </div>
            </li>

            <li className="flex">
              <div className="flex-shrink-0">
                <div className="flex items-center justify-center h-8 w-8 rounded-full bg-primary-100 text-primary-600">
                  2
                </div>
              </div>
              <div className="ml-4">
                <h3 className="text-sm font-medium text-secondary-900">Prioritize Marketing Data Quality</h3>
                <p className="mt-1 text-sm text-secondary-500">
                  Marketing data has the lowest overall quality scores. Implement additional validation
                  rules and data quality checks to improve accuracy and completeness.
                </p>
              </div>
            </li>

            <li className="flex">
              <div className="flex-shrink-0">
                <div className="flex items-center justify-center h-8 w-8 rounded-full bg-primary-100 text-primary-600">
                  3
                </div>
              </div>
              <div className="ml-4">
                <h3 className="text-sm font-medium text-secondary-900">Reduce Issue Resolution Time</h3>
                <p className="mt-1 text-sm text-secondary-500">
                  Current average resolution time of 8.2 hours can be improved to under 6 hours by
                  implementing automated remediation workflows for common data quality issues.
                </p>
              </div>
            </li>
          </ul>
        </div>
      </div>
    </div>
  );
};

export default BusinessImpactPage;