import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import {
  ClockIcon,
  Cog6ToothIcon,
  PlayIcon,
  PauseIcon,
  ExclamationCircleIcon
} from '@heroicons/react/24/outline';

const MetadataAutomationControls = ({ connectionId }) => {
  const [automationConfig, setAutomationConfig] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadAutomationConfig();
  }, [connectionId]);

  const loadAutomationConfig = async () => {
    if (!connectionId) return;

    try {
      const response = await fetch(`/api/automation/connection-configs/${connectionId}`);
      if (response.ok) {
        const config = await response.json();
        setAutomationConfig(config);
      }
    } catch (error) {
      console.error('Error loading automation config:', error);
    } finally {
      setLoading(false);
    }
  };

  const toggleMetadataAutomation = async () => {
    const newEnabled = !automationConfig.metadata_refresh.enabled;

    try {
      const updatedConfig = {
        ...automationConfig,
        metadata_refresh: {
          ...automationConfig.metadata_refresh,
          enabled: newEnabled
        }
      };

      const response = await fetch(`/api/automation/connection-configs/${connectionId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updatedConfig)
      });

      if (response.ok) {
        setAutomationConfig(updatedConfig);
      }
    } catch (error) {
      console.error('Error toggling automation:', error);
    }
  };

  if (loading || !automationConfig) {
    return <div className="animate-pulse h-16 bg-gray-100 rounded"></div>;
  }

  const { metadata_refresh, schema_change_detection } = automationConfig;

  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4">
      <div className="flex items-center justify-between mb-3">
        <h4 className="text-sm font-medium text-gray-900 flex items-center">
          <ClockIcon className="h-4 w-4 mr-2" />
          Automation Status
        </h4>
        <Link
          to="/settings/automation"
          className="text-xs text-primary-600 hover:text-primary-700 flex items-center"
        >
          <Cog6ToothIcon className="h-3 w-3 mr-1" />
          Configure
        </Link>
      </div>

      <div className="space-y-2">
        {/* Metadata Refresh Status */}
        <div className="flex items-center justify-between">
          <div className="flex items-center">
            <span className="text-xs text-gray-700">Metadata Refresh</span>
            {metadata_refresh.enabled && (
              <span className="ml-2 text-xs text-gray-500">
                (Every {metadata_refresh.interval_hours}h)
              </span>
            )}
          </div>
          <button
            onClick={toggleMetadataAutomation}
            className={`flex items-center px-2 py-1 rounded text-xs font-medium ${
              metadata_refresh.enabled
                ? 'bg-green-100 text-green-800 hover:bg-green-200'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
            }`}
          >
            {metadata_refresh.enabled ? (
              <>
                <PlayIcon className="h-3 w-3 mr-1" />
                On
              </>
            ) : (
              <>
                <PauseIcon className="h-3 w-3 mr-1" />
                Off
              </>
            )}
          </button>
        </div>

        {/* Schema Change Detection Status */}
        <div className="flex items-center justify-between">
          <div className="flex items-center">
            <span className="text-xs text-gray-700">Schema Detection</span>
            {schema_change_detection.enabled && (
              <span className="ml-2 text-xs text-gray-500">
                (Every {schema_change_detection.interval_hours}h)
              </span>
            )}
          </div>
          <span className={`px-2 py-1 rounded text-xs font-medium ${
            schema_change_detection.enabled
              ? 'bg-blue-100 text-blue-800'
              : 'bg-gray-100 text-gray-600'
          }`}>
            {schema_change_detection.enabled ? 'On' : 'Off'}
          </span>
        </div>

        {/* Warning if no automation enabled */}
        {!metadata_refresh.enabled && !schema_change_detection.enabled && (
          <div className="mt-2 p-2 bg-yellow-50 border border-yellow-200 rounded flex items-start">
            <ExclamationCircleIcon className="h-4 w-4 text-yellow-400 mr-2 mt-0.5" />
            <div>
              <p className="text-xs text-yellow-800">
                No automation enabled for this connection.
              </p>
              <Link
                to="/settings/automation"
                className="text-xs text-yellow-600 hover:text-yellow-700 underline"
              >
                Enable automation →
              </Link>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};