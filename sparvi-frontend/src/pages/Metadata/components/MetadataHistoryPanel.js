import React, { useState, useEffect } from 'react';
import {
  CalendarIcon,
  ClockIcon,
  ArrowsRightLeftIcon,
  MagnifyingGlassIcon,
  ArrowDownTrayIcon
} from '@heroicons/react/24/outline';
import LoadingSpinner from '../../../components/common/LoadingSpinner';
import HistoricalMetadataViewer from './HistoricalMetadataViewer';
import MetadataDiffViewer from './MetadataDiffViewer';
import MetadataTimelineSlider from './MetadataTimelineSlider';
import { mockHistoricalMetadataService } from '../../../services/mockHistoricalMetadataService';

const MetadataHistoryPanel = ({ connectionId }) => {
  const [selectedDate, setSelectedDate] = useState(new Date().toISOString().split('T')[0]);
  const [selectedTable, setSelectedTable] = useState('all');
  const [viewMode, setViewMode] = useState('snapshot'); // 'snapshot' or 'compare'
  const [compareDate, setCompareDate] = useState('');
  const [metadataType, setMetadataType] = useState('tables'); // 'tables' or 'columns'
  const [loading, setLoading] = useState(false);
  const [historicalData, setHistoricalData] = useState(null);
  const [compareData, setCompareData] = useState(null);
  const [availableDates, setAvailableDates] = useState([]);
  const [availableTables, setAvailableTables] = useState([]);

  // Load available dates and tables on component mount
  useEffect(() => {
    const loadAvailableData = async () => {
      if (!connectionId) return;

      try {
        setLoading(true);
        const dates = await mockHistoricalMetadataService.getAvailableDates(connectionId);
        const tables = await mockHistoricalMetadataService.getAvailableTables(connectionId);

        setAvailableDates(dates);
        setAvailableTables(tables);

        // Set default selected date to most recent if available
        if (dates.length > 0) {
          setSelectedDate(dates[0]);
        }
      } catch (error) {
        console.error('Error loading available data:', error);
      } finally {
        setLoading(false);
      }
    };

    loadAvailableData();
  }, [connectionId]);

  // Load historical data when date/table/type changes
  useEffect(() => {
    const loadHistoricalData = async () => {
      if (!connectionId || !selectedDate) return;

      try {
        setLoading(true);
        const data = await mockHistoricalMetadataService.getHistoricalMetadata(
          connectionId,
          selectedDate,
          metadataType,
          selectedTable === 'all' ? null : selectedTable
        );
        setHistoricalData(data);
      } catch (error) {
        console.error('Error loading historical data:', error);
      } finally {
        setLoading(false);
      }
    };

    loadHistoricalData();
  }, [connectionId, selectedDate, selectedTable, metadataType]);

  // Load compare data when in compare mode
  useEffect(() => {
    const loadCompareData = async () => {
      if (!connectionId || !compareDate || viewMode !== 'compare') return;

      try {
        const data = await mockHistoricalMetadataService.getHistoricalMetadata(
          connectionId,
          compareDate,
          metadataType,
          selectedTable === 'all' ? null : selectedTable
        );
        setCompareData(data);
      } catch (error) {
        console.error('Error loading compare data:', error);
      }
    };

    loadCompareData();
  }, [connectionId, compareDate, selectedTable, metadataType, viewMode]);

  const handleDateChange = (event) => {
    setSelectedDate(event.target.value);
  };

  const handleCompareDateChange = (event) => {
    setCompareDate(event.target.value);
  };

  const handleTableChange = (event) => {
    setSelectedTable(event.target.value);
  };

  const handleViewModeChange = (mode) => {
    setViewMode(mode);
    if (mode === 'snapshot') {
      setCompareData(null);
      setCompareDate('');
    }
  };

  const handleMetadataTypeChange = (type) => {
    setMetadataType(type);
  };

  const handleExport = async () => {
    try {
      const exportData = viewMode === 'compare'
        ? { primary: historicalData, compare: compareData }
        : historicalData;

      const dataStr = JSON.stringify(exportData, null, 2);
      const dataBlob = new Blob([dataStr], { type: 'application/json' });

      const link = document.createElement('a');
      link.href = URL.createObjectURL(dataBlob);
      link.download = `metadata-history-${selectedDate}${compareDate ? `-vs-${compareDate}` : ''}.json`;
      link.click();
    } catch (error) {
      console.error('Error exporting data:', error);
    }
  };

  if (!connectionId) {
    return (
      <div className="text-center py-8">
        <ClockIcon className="mx-auto h-12 w-12 text-secondary-400" />
        <p className="mt-2 text-secondary-500">Select a connection to view metadata history</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Timeline Slider */}
      <MetadataTimelineSlider
        availableDates={availableDates}
        selectedDate={selectedDate}
        onDateChange={setSelectedDate}
        compareDate={compareDate}
        onCompareDateChange={setCompareDate}
        viewMode={viewMode}
      />

      {/* Controls */}
      <div className="bg-secondary-50 p-4 rounded-lg">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-4">
          {/* Table Selector */}
          <div>
            <label htmlFor="selected-table" className="block text-sm font-medium text-secondary-700 mb-1">
              Table Filter
            </label>
            <select
              id="selected-table"
              value={selectedTable}
              onChange={handleTableChange}
              className="block w-full pl-3 pr-10 py-2 text-base border-secondary-300 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm rounded-md"
            >
              <option value="all">All Tables</option>
              {availableTables.map(table => (
                <option key={table} value={table}>{table}</option>
              ))}
            </select>
          </div>

          {/* Metadata Type */}
          <div>
            <label htmlFor="metadata-type" className="block text-sm font-medium text-secondary-700 mb-1">
              Data View
            </label>
            <select
              id="metadata-type"
              value={metadataType}
              onChange={(e) => handleMetadataTypeChange(e.target.value)}
              className="block w-full pl-3 pr-10 py-2 text-base border-secondary-300 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm rounded-md"
            >
              <option value="tables">Tables</option>
              <option value="columns">Columns</option>
            </select>
          </div>

          {/* View Mode Toggle */}
          <div>
            <label className="block text-sm font-medium text-secondary-700 mb-1">
              View Mode
            </label>
            <div className="flex rounded-md shadow-sm">
              <button
                onClick={() => handleViewModeChange('snapshot')}
                className={`relative inline-flex items-center px-3 py-2 rounded-l-md border text-sm font-medium focus:z-10 focus:outline-none focus:ring-1 focus:ring-primary-500 focus:border-primary-500 ${
                  viewMode === 'snapshot'
                    ? 'z-10 bg-primary-50 border-primary-500 text-primary-700'
                    : 'bg-white border-secondary-300 text-secondary-500 hover:bg-secondary-50'
                }`}
              >
                <ClockIcon className="-ml-1 mr-1 h-4 w-4" />
                Snapshot
              </button>
              <button
                onClick={() => handleViewModeChange('compare')}
                className={`relative inline-flex items-center px-3 py-2 rounded-r-md border text-sm font-medium focus:z-10 focus:outline-none focus:ring-1 focus:ring-primary-500 focus:border-primary-500 ${
                  viewMode === 'compare'
                    ? 'z-10 bg-primary-50 border-primary-500 text-primary-700'
                    : 'bg-white border-secondary-300 text-secondary-500 hover:bg-secondary-50'
                }`}
              >
                <ArrowsRightLeftIcon className="-ml-1 mr-1 h-4 w-4" />
                Compare
              </button>
            </div>
          </div>

          {/* Export Button */}
          <div className="flex items-end">
            <button
              onClick={handleExport}
              disabled={!historicalData}
              className="w-full inline-flex items-center justify-center px-3 py-2 border border-secondary-300 shadow-sm text-sm font-medium rounded-md text-secondary-700 bg-white hover:bg-secondary-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
            >
              <ArrowDownTrayIcon className="-ml-1 mr-2 h-4 w-4" />
              Export
            </button>
          </div>
        </div>
      </div>

      {/* Content */}
      {loading ? (
        <div className="flex justify-center items-center py-12">
          <LoadingSpinner size="lg" />
          <span className="ml-3 text-secondary-600">Loading historical metadata...</span>
        </div>
      ) : viewMode === 'compare' && compareDate ? (
        <MetadataDiffViewer
          primaryData={historicalData}
          compareData={compareData}
          primaryDate={selectedDate}
          compareDate={compareDate}
          metadataType={metadataType}
          tableName={selectedTable === 'all' ? null : selectedTable}
        />
      ) : (
        <HistoricalMetadataViewer
          data={historicalData}
          date={selectedDate}
          metadataType={metadataType}
          tableName={selectedTable === 'all' ? null : selectedTable}
        />
      )}
    </div>
  );
};

export default MetadataHistoryPanel;