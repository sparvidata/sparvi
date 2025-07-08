import React, { useState, useEffect, useRef } from 'react';
import {
  CalendarIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  ClockIcon
} from '@heroicons/react/24/outline';

const MetadataTimelineSlider = ({
  availableDates = [],
  selectedDate,
  onDateChange,
  compareDate = null,
  onCompareDateChange = null,
  viewMode = 'snapshot'
}) => {
  const [timelineRange, setTimelineRange] = useState({ start: null, end: null });
  const [hoveredDate, setHoveredDate] = useState(null);
  const sliderRef = useRef(null);

  // Calculate timeline range based on available dates
  useEffect(() => {
    if (availableDates.length > 0) {
      const sortedDates = [...availableDates].sort();
      const startDate = new Date(sortedDates[0]);
      const endDate = new Date(sortedDates[sortedDates.length - 1]);

      // Extend range slightly for better UX
      startDate.setDate(startDate.getDate() - 2);
      endDate.setDate(endDate.getDate() + 2);

      setTimelineRange({ start: startDate, end: endDate });
    }
  }, [availableDates]);

  // Convert date to position percentage
  const dateToPosition = (date) => {
    if (!timelineRange.start || !timelineRange.end) return 0;

    const targetDate = new Date(date);
    const totalRange = timelineRange.end.getTime() - timelineRange.start.getTime();
    const datePosition = targetDate.getTime() - timelineRange.start.getTime();

    return Math.max(0, Math.min(100, (datePosition / totalRange) * 100));
  };

  // Convert position percentage to date
  const positionToDate = (position) => {
    if (!timelineRange.start || !timelineRange.end) return null;

    const totalRange = timelineRange.end.getTime() - timelineRange.start.getTime();
    const targetTime = timelineRange.start.getTime() + (totalRange * (position / 100));

    return new Date(targetTime);
  };

  // Find closest available date to a given date
  const findClosestAvailableDate = (targetDate) => {
    if (availableDates.length === 0) return null;

    const target = new Date(targetDate).getTime();
    let closest = availableDates[0];
    let minDiff = Math.abs(new Date(availableDates[0]).getTime() - target);

    for (const date of availableDates) {
      const diff = Math.abs(new Date(date).getTime() - target);
      if (diff < minDiff) {
        minDiff = diff;
        closest = date;
      }
    }

    return closest;
  };

  // Handle slider interaction
  const handleSliderClick = (event) => {
    if (!sliderRef.current || !timelineRange.start) return;

    const rect = sliderRef.current.getBoundingClientRect();
    const clickX = event.clientX - rect.left;
    const position = (clickX / rect.width) * 100;

    const targetDate = positionToDate(position);
    const closestDate = findClosestAvailableDate(targetDate);

    if (closestDate) {
      if (viewMode === 'compare' && event.shiftKey && onCompareDateChange) {
        onCompareDateChange(closestDate);
      } else {
        onDateChange(closestDate);
      }
    }
  };

  // Handle mouse move for hover effect
  const handleMouseMove = (event) => {
    if (!sliderRef.current || !timelineRange.start) return;

    const rect = sliderRef.current.getBoundingClientRect();
    const moveX = event.clientX - rect.left;
    const position = (moveX / rect.width) * 100;

    const targetDate = positionToDate(position);
    const closestDate = findClosestAvailableDate(targetDate);
    setHoveredDate(closestDate);
  };

  // Navigation functions
  const navigateDate = (direction) => {
    const currentIndex = availableDates.indexOf(selectedDate);
    if (currentIndex === -1) return;

    const newIndex = direction === 'prev' ? currentIndex + 1 : currentIndex - 1;
    if (newIndex >= 0 && newIndex < availableDates.length) {
      onDateChange(availableDates[newIndex]);
    }
  };

  // Format date for display
  const formatDate = (date) => {
    return new Date(date).toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric'
    });
  };

  // Get relative time description
  const getRelativeTime = (date) => {
    const now = new Date();
    const target = new Date(date);
    const diffDays = Math.floor((now - target) / (1000 * 60 * 60 * 24));

    if (diffDays === 0) return 'Today';
    if (diffDays === 1) return 'Yesterday';
    if (diffDays < 7) return `${diffDays} days ago`;
    if (diffDays < 30) return `${Math.floor(diffDays / 7)} weeks ago`;
    return `${Math.floor(diffDays / 30)} months ago`;
  };

  if (availableDates.length === 0) {
    return (
      <div className="text-center py-8">
        <ClockIcon className="mx-auto h-8 w-8 text-secondary-400" />
        <p className="mt-2 text-sm text-secondary-500">No historical data available</p>
      </div>
    );
  }

  return (
    <div className="bg-white border border-secondary-200 rounded-lg p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-sm font-medium text-secondary-900 flex items-center">
            <CalendarIcon className="mr-2 h-4 w-4" />
            Timeline Navigation
          </h3>
          <p className="text-xs text-secondary-500 mt-1">
            {availableDates.length} snapshots available • Click to select
            {viewMode === 'compare' && ' • Shift+click for compare date'}
          </p>
        </div>

        {/* Navigation buttons */}
        <div className="flex items-center space-x-2">
          <button
            onClick={() => navigateDate('prev')}
            disabled={availableDates.indexOf(selectedDate) >= availableDates.length - 1}
            className="p-1 rounded text-secondary-400 hover:text-secondary-600 disabled:opacity-30 disabled:cursor-not-allowed"
            title="Previous snapshot"
          >
            <ChevronLeftIcon className="h-4 w-4" />
          </button>
          <button
            onClick={() => navigateDate('next')}
            disabled={availableDates.indexOf(selectedDate) <= 0}
            className="p-1 rounded text-secondary-400 hover:text-secondary-600 disabled:opacity-30 disabled:cursor-not-allowed"
            title="Next snapshot"
          >
            <ChevronRightIcon className="h-4 w-4" />
          </button>
        </div>
      </div>

      {/* Selected Date Info */}
      <div className="mb-4 p-3 bg-primary-50 rounded-md">
        <div className="flex items-center justify-between">
          <div>
            <div className="font-medium text-primary-900">{formatDate(selectedDate)}</div>
            <div className="text-sm text-primary-600">{getRelativeTime(selectedDate)}</div>
          </div>
          {viewMode === 'compare' && compareDate && (
            <div className="text-right">
              <div className="text-sm text-secondary-600">Comparing with:</div>
              <div className="font-medium text-secondary-900">{formatDate(compareDate)}</div>
            </div>
          )}
        </div>
      </div>

      {/* Timeline Slider */}
      <div className="relative">
        {/* Background track */}
        <div
          ref={sliderRef}
          className="relative h-12 bg-secondary-100 rounded-lg cursor-pointer overflow-hidden"
          onClick={handleSliderClick}
          onMouseMove={handleMouseMove}
          onMouseLeave={() => setHoveredDate(null)}
        >
          {/* Date markers for available snapshots */}
          {availableDates.map((date) => {
            const position = dateToPosition(date);
            const isSelected = date === selectedDate;
            const isCompareDate = date === compareDate;
            const isHovered = date === hoveredDate;

            return (
              <div
                key={date}
                className="absolute top-0 bottom-0 flex items-center justify-center transition-all duration-200"
                style={{ left: `${position}%`, transform: 'translateX(-50%)' }}
              >
                {/* Snapshot indicator */}
                <div
                  className={`w-3 h-3 rounded-full border-2 transition-all duration-200 ${
                    isSelected 
                      ? 'bg-primary-600 border-primary-600 scale-150' 
                      : isCompareDate
                      ? 'bg-accent-500 border-accent-500 scale-125'
                      : isHovered
                      ? 'bg-secondary-400 border-secondary-400 scale-125'
                      : 'bg-white border-secondary-400 hover:border-secondary-600'
                  }`}
                  title={`${formatDate(date)} (${getRelativeTime(date)})`}
                />

                {/* Vertical line for snapshot */}
                <div
                  className={`absolute top-0 w-0.5 h-full transition-all duration-200 ${
                    isSelected 
                      ? 'bg-primary-600 opacity-60' 
                      : isCompareDate
                      ? 'bg-accent-500 opacity-60'
                      : 'bg-secondary-300 opacity-30'
                  }`}
                />
              </div>
            );
          })}

          {/* Range indicator between compare dates */}
          {viewMode === 'compare' && compareDate && selectedDate !== compareDate && (
            <div
              className="absolute top-0 bottom-0 bg-accent-200 opacity-30"
              style={{
                left: `${Math.min(dateToPosition(selectedDate), dateToPosition(compareDate))}%`,
                width: `${Math.abs(dateToPosition(selectedDate) - dateToPosition(compareDate))}%`
              }}
            />
          )}

          {/* Hover tooltip */}
          {hoveredDate && (
            <div
              className="absolute -top-8 bg-secondary-900 text-white text-xs px-2 py-1 rounded whitespace-nowrap z-10"
              style={{
                left: `${dateToPosition(hoveredDate)}%`,
                transform: 'translateX(-50%)'
              }}
            >
              {formatDate(hoveredDate)}
            </div>
          )}
        </div>

        {/* Timeline labels */}
        <div className="flex justify-between mt-2 text-xs text-secondary-500">
          <span>{timelineRange.start && formatDate(timelineRange.start)}</span>
          <span>{timelineRange.end && formatDate(timelineRange.end)}</span>
        </div>
      </div>

      {/* Quick date selection */}
      <div className="mt-4 flex flex-wrap gap-2">
        {availableDates.slice(0, 7).map((date) => (
          <button
            key={date}
            onClick={() => onDateChange(date)}
            className={`px-3 py-1 text-xs rounded-full transition-colors ${
              date === selectedDate
                ? 'bg-primary-100 text-primary-700 border border-primary-300'
                : 'bg-secondary-100 text-secondary-600 hover:bg-secondary-200'
            }`}
          >
            {getRelativeTime(date)}
          </button>
        ))}
        {availableDates.length > 7 && (
          <span className="text-xs text-secondary-400 self-center">
            +{availableDates.length - 7} more
          </span>
        )}
      </div>
    </div>
  );
};

export default MetadataTimelineSlider;