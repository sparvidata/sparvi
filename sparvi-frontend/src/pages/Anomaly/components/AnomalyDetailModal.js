import React, { useState, useEffect } from 'react';
import { Dialog, Transition } from '@headlessui/react';
import {
  XMarkIcon,
  ExclamationCircleIcon,
  ExclamationTriangleIcon,
  CheckIcon,
  TableCellsIcon,
  ChartBarIcon,
  ClockIcon
} from '@heroicons/react/24/outline';
import { formatDate } from '../../../utils/formatting';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import AnomalyMetricChart from './AnomalyMetricChart';
import anomalyService from '../../../services/anomalyService';

const AnomalyDetailModal = ({ connectionId, anomalyId, onClose, onStatusUpdate }) => {
  const [anomaly, setAnomaly] = useState(null);
  const [loading, setLoading] = useState(true);
  const [loadingMetrics, setLoadingMetrics] = useState(true);
  const [updatingStatus, setUpdatingStatus] = useState(false);
  const [metrics, setMetrics] = useState([]);
  const [resolutionNote, setResolutionNote] = useState('');
  const [error, setError] = useState(null);

  // Check if we have valid IDs before making API calls
  const hasValidIds = connectionId && connectionId !== 'undefined' && anomalyId;

  // Fetch anomaly data using anomalyService
  useEffect(() => {
    if (!hasValidIds) {
      setError('Invalid connection or anomaly ID');
      setLoading(false);
      return;
    }

    const fetchAnomalyData = async () => {
      try {
        setLoading(true);
        setError(null);

        console.log(`Loading anomaly ${anomalyId} for connection ${connectionId}`);

        const anomalyData = await anomalyService.getAnomaly(connectionId, anomalyId);
        setAnomaly(anomalyData);

        console.log('Anomaly data loaded:', anomalyData);
      } catch (err) {
        console.error('Error fetching anomaly:', err);
        setError(`Failed to load anomaly details: ${err.message}`);
      } finally {
        setLoading(false);
      }
    };

    fetchAnomalyData();
  }, [connectionId, anomalyId, hasValidIds]);

  // Fetch historical metrics using anomalyService
  useEffect(() => {
    if (!anomaly || !hasValidIds) {
      setLoadingMetrics(false);
      return;
    }

    const fetchMetricData = async () => {
      try {
        setLoadingMetrics(true);

        console.log(`Loading metrics for anomaly: ${anomaly.metric_name}, table: ${anomaly.table_name}`);

        // Get metric history for this anomaly
        const metricData = await anomalyService.getHistoricalMetrics(connectionId, {
          metric_name: anomaly.metric_name,
          table_name: anomaly.table_name,
          column_name: anomaly.column_name || undefined,
          days: 30
        });

        setMetrics(metricData);
        console.log(`Loaded ${metricData.length} metric data points`);
      } catch (err) {
        console.error('Error fetching metrics:', err);
        // Don't show error for metrics, just leave chart empty
      } finally {
        setLoadingMetrics(false);
      }
    };

    fetchMetricData();
  }, [connectionId, anomaly, hasValidIds]);

  // Get severity icon
  const getSeverityIcon = (severity) => {
    switch (severity) {
      case 'high':
        return <ExclamationTriangleIcon className="h-8 w-8 text-red-600" aria-hidden="true" />;
      case 'medium':
        return <ExclamationCircleIcon className="h-8 w-8 text-yellow-500" aria-hidden="true" />;
      case 'low':
        return <ExclamationCircleIcon className="h-8 w-8 text-green-500" aria-hidden="true" />;
      default:
        return <ExclamationCircleIcon className="h-8 w-8 text-gray-400" aria-hidden="true" />;
    }
  };

  // Format metric value using anomalyService helper
  const formatMetricValue = (value) => {
    return anomalyService.formatMetricValue(value);
  };

  // Handle status update using anomalyService
  const handleStatusUpdate = async (newStatus) => {
    if (!hasValidIds) {
      setError('Cannot update status: Invalid connection ID');
      return;
    }

    try {
      setUpdatingStatus(true);
      setError(null);

      console.log(`Updating anomaly ${anomalyId} status to ${newStatus}`);

      await anomalyService.updateAnomalyStatus(connectionId, anomalyId, newStatus, resolutionNote);

      // Call the parent callback to refresh data
      if (onStatusUpdate) {
        onStatusUpdate(anomalyId, newStatus, resolutionNote);
      }

      console.log(`Status updated successfully to ${newStatus}`);
    } catch (err) {
      console.error('Error updating status:', err);
      setError(`Failed to update status: ${err.message}`);
    } finally {
      setUpdatingStatus(false);
    }
  };

  return (
    <Transition.Root show={true} as={React.Fragment}>
      <Dialog as="div" className="fixed z-10 inset-0 overflow-y-auto" onClose={onClose}>
        <div className="flex items-center justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
          <Transition.Child
            as={React.Fragment}
            enter="ease-out duration-300"
            enterFrom="opacity-0"
            enterTo="opacity-100"
            leave="ease-in duration-200"
            leaveFrom="opacity-100"
            leaveTo="opacity-0"
          >
            <Dialog.Overlay className="fixed inset-0 bg-gray-500 bg-opacity-75 transition-opacity" />
          </Transition.Child>

          {/* This centers the modal contents. */}
          <span className="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">
            &#8203;
          </span>

          <Transition.Child
            as={React.Fragment}
            enter="ease-out duration-300"
            enterFrom="opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"
            enterTo="opacity-100 translate-y-0 sm:scale-100"
            leave="ease-in duration-200"
            leaveFrom="opacity-100 translate-y-0 sm:scale-100"
            leaveTo="opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"
          >
            <div className="inline-block align-bottom bg-white rounded-lg px-4 pt-5 pb-4 text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-3xl sm:w-full sm:p-6">
              <div className="absolute top-0 right-0 pt-4 pr-4">
                <button
                  type="button"
                  className="bg-white rounded-md text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
                  onClick={onClose}
                  disabled={updatingStatus}
                >
                  <span className="sr-only">Close</span>
                  <XMarkIcon className="h-6 w-6" aria-hidden="true" />
                </button>
              </div>

              {loading ? (
                <div className="flex justify-center py-12">
                  <LoadingSpinner size="lg" />
                </div>
              ) : error ? (
                <div className="text-center py-12">
                  <ExclamationCircleIcon className="h-12 w-12 text-red-500 mx-auto" />
                  <h3 className="mt-2 text-lg font-medium text-gray-900">Error</h3>
                  <p className="mt-1 text-sm text-gray-500">{error}</p>
                </div>
              ) : anomaly ? (
                <div className="sm:flex sm:items-start">
                  <div className="mt-3 text-center sm:mt-0 sm:text-left w-full">
                    <Dialog.Title as="h3" className="text-lg leading-6 font-medium text-gray-900 flex items-center">
                      {getSeverityIcon(anomaly.severity)}
                      <span className="ml-2">Anomaly Details</span>
                      <span className={`ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                        anomaly.severity === 'high' ? 'bg-red-100 text-red-800' :
                        anomaly.severity === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                        'bg-green-100 text-green-800'
                      }`}>
                        {anomaly.severity} severity
                      </span>
                    </Dialog.Title>

                    <div className="mt-4 border-t border-gray-200 pt-4">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        {/* Location info */}
                        <div className="border rounded-lg p-4 bg-gray-50">
                          <h4 className="text-sm font-medium text-gray-500 mb-2 flex items-center">
                            <TableCellsIcon className="h-5 w-5 mr-1 text-gray-400" />
                            Location
                          </h4>
                          <p className="text-base font-medium text-gray-900">{anomaly.table_name}</p>
                          {anomaly.column_name && (
                            <p className="text-sm text-gray-500">Column: {anomaly.column_name}</p>
                          )}
                        </div>

                        {/* Metric info */}
                        <div className="border rounded-lg p-4 bg-gray-50">
                          <h4 className="text-sm font-medium text-gray-500 mb-2 flex items-center">
                            <ChartBarIcon className="h-5 w-5 mr-1 text-gray-400" />
                            Metric
                          </h4>
                          <p className="text-base font-medium text-gray-900">{anomaly.metric_name}</p>
                          <div className="flex items-center mt-1">
                            <p className="text-sm text-gray-500">Value: </p>
                            <p className="text-sm font-medium text-gray-900 ml-1">
                              {formatMetricValue(anomaly.metric_value)}
                            </p>
                          </div>
                        </div>

                        {/* Detection info */}
                        <div className="border rounded-lg p-4 bg-gray-50">
                          <h4 className="text-sm font-medium text-gray-500 mb-2 flex items-center">
                            <ClockIcon className="h-5 w-5 mr-1 text-gray-400" />
                            Detection
                          </h4>
                          <p className="text-sm text-gray-500">
                            Detected: {formatDate(anomaly.detected_at, true)}
                          </p>
                          <p className="text-sm text-gray-500">
                            Method: {anomaly.anomaly_detection_configs?.detection_method || 'Unknown'}
                          </p>
                          <p className="text-sm text-gray-500 flex items-center">
                            Score:
                            <span className="font-medium ml-1">{parseFloat(anomaly.score || 0).toFixed(2)}</span>
                            <span className="ml-1">(threshold: {parseFloat(anomaly.threshold || 0).toFixed(2)})</span>
                          </p>
                        </div>

                        {/* Status info */}
                        <div className="border rounded-lg p-4 bg-gray-50">
                          <h4 className="text-sm font-medium text-gray-500 mb-2">Status</h4>
                          <p className="text-base font-medium text-gray-900 capitalize">{anomaly.status}</p>
                          {anomaly.resolved_at && (
                            <p className="text-sm text-gray-500">
                              Resolved: {formatDate(anomaly.resolved_at, true)}
                            </p>
                          )}
                          {anomaly.resolution_note && (
                            <p className="text-sm text-gray-500 mt-1">
                              Note: {anomaly.resolution_note}
                            </p>
                          )}
                        </div>
                      </div>

                      {/* Chart */}
                      <div className="mt-6 border rounded-lg p-4">
                        <h4 className="text-sm font-medium text-gray-500 mb-2">Metric History</h4>
                        <div className="h-64">
                          {loadingMetrics ? (
                            <div className="flex justify-center items-center h-full">
                              <LoadingSpinner size="md" />
                            </div>
                          ) : metrics.length > 0 ? (
                            <AnomalyMetricChart
                              metrics={metrics}
                              anomalyValue={anomaly.metric_value}
                              anomalyTimestamp={anomaly.detected_at}
                            />
                          ) : (
                            <div className="flex justify-center items-center h-full text-gray-500">
                              No historical data available
                            </div>
                          )}
                        </div>
                      </div>

                      {/* Actions */}
                      {anomaly.status !== 'resolved' && (
                        <div className="mt-6 border-t border-gray-200 pt-4">
                          <h4 className="text-sm font-medium text-gray-500 mb-2">Actions</h4>

                          {/* Resolution note */}
                          <div className="mb-4">
                            <label htmlFor="resolution-note" className="block text-sm font-medium text-gray-700">
                              Resolution Note
                            </label>
                            <div className="mt-1">
                              <textarea
                                id="resolution-note"
                                name="resolution-note"
                                rows={3}
                                className="shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-gray-300 rounded-md"
                                placeholder="Add a note about this anomaly..."
                                value={resolutionNote}
                                onChange={(e) => setResolutionNote(e.target.value)}
                                disabled={updatingStatus}
                              />
                            </div>
                          </div>

                          <div className="flex flex-col sm:flex-row sm:justify-end gap-3">
                            {anomaly.status === 'open' && (
                              <button
                                type="button"
                                className="w-full inline-flex justify-center rounded-md border border-yellow-300 shadow-sm px-4 py-2 bg-yellow-50 text-base font-medium text-yellow-700 hover:bg-yellow-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-yellow-500 sm:w-auto sm:text-sm disabled:opacity-50 disabled:cursor-not-allowed"
                                onClick={() => handleStatusUpdate('acknowledged')}
                                disabled={updatingStatus}
                              >
                                {updatingStatus ? (
                                  <>
                                    <LoadingSpinner size="xs" className="mr-2" />
                                    Updating...
                                  </>
                                ) : (
                                  'Acknowledge'
                                )}
                              </button>
                            )}

                            <button
                              type="button"
                              className="w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-green-600 text-base font-medium text-white hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 sm:w-auto sm:text-sm disabled:opacity-50 disabled:cursor-not-allowed"
                              onClick={() => handleStatusUpdate('resolved')}
                              disabled={updatingStatus}
                            >
                              {updatingStatus ? (
                                <>
                                  <LoadingSpinner size="xs" className="mr-2" />
                                  Updating...
                                </>
                              ) : (
                                'Mark as Resolved'
                              )}
                            </button>
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="text-center py-12">
                  <ExclamationCircleIcon className="h-12 w-12 text-gray-400 mx-auto" />
                  <h3 className="mt-2 text-lg font-medium text-gray-900">No Data</h3>
                  <p className="mt-1 text-sm text-gray-500">The anomaly could not be found.</p>
                </div>
              )}
            </div>
          </Transition.Child>
        </div>
      </Dialog>
    </Transition.Root>
  );
};

export default AnomalyDetailModal;