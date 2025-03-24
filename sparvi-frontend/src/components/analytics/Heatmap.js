import React from 'react';

/**
 * A reusable heatmap component for visualizing data intensity
 */
const Heatmap = ({
  data = [],
  title,
  rowKey = 'row',
  colKey = 'col',
  valueKey = 'value',
  colorScale = (value) => {
    // Default blue color scale
    const intensity = Math.min(0.9, Math.max(0.1, value));
    return `rgba(99, 102, 241, ${intensity})`;
  },
  height = 'auto',
  width = '100%',
  cellSize = { width: 40, height: 40 },
  loading = false,
  emptyMessage = 'No data available',
  formatValue = (value) => value.toFixed(1),
  formatRowLabel = (row) => row,
  formatColLabel = (col) => col,
  className = ''
}) => {
  // Extract unique rows and columns
  const rows = [...new Set(data.map(item => item[rowKey]))];
  const cols = [...new Set(data.map(item => item[colKey]))];

  // Get value for a cell
  const getValue = (row, col) => {
    const cell = data.find(item => item[rowKey] === row && item[colKey] === col);
    return cell ? cell[valueKey] : 0;
  };

  // If data is loading
  if (loading) {
    return (
      <div className={`bg-white rounded-lg shadow p-4 ${className}`}>
        {title && <h3 className="text-md font-medium text-secondary-900 mb-2">{title}</h3>}
        <div className="h-[200px] w-full flex items-center justify-center bg-secondary-50 rounded animate-pulse">
          <div className="text-secondary-400">Loading...</div>
        </div>
      </div>
    );
  }

  // If no data is available
  if (!data || data.length === 0 || rows.length === 0 || cols.length === 0) {
    return (
      <div className={`bg-white rounded-lg shadow p-4 ${className}`}>
        {title && <h3 className="text-md font-medium text-secondary-900 mb-2">{title}</h3>}
        <div className="h-[200px] w-full flex items-center justify-center bg-secondary-50 rounded">
          <div className="text-secondary-500">{emptyMessage}</div>
        </div>
      </div>
    );
  }

  return (
    <div className={`bg-white rounded-lg shadow p-4 ${className}`}>
      {title && <h3 className="text-md font-medium text-secondary-900 mb-2">{title}</h3>}

      <div className="overflow-x-auto" style={{ height, width }}>
        <div className="flex">
          {/* Empty top-left cell */}
          <div style={{ width: cellSize.width, height: cellSize.height }} className="flex items-center justify-center"></div>

          {/* Column headers */}
          {cols.map((col, index) => (
            <div
              key={`col-${index}`}
              style={{ width: cellSize.width, height: cellSize.height }}
              className="flex items-center justify-center font-medium text-sm text-secondary-700"
            >
              {formatColLabel(col)}
            </div>
          ))}
        </div>

        {/* Rows */}
        {rows.map((row, rowIndex) => (
          <div key={`row-${rowIndex}`} className="flex">
            {/* Row label */}
            <div
              style={{ width: cellSize.width, height: cellSize.height }}
              className="flex items-center justify-center font-medium text-sm text-secondary-700"
            >
              {formatRowLabel(row)}
            </div>

            {/* Cells */}
            {cols.map((col, colIndex) => {
              const value = getValue(row, col);
              return (
                <div
                  key={`cell-${rowIndex}-${colIndex}`}
                  style={{
                    width: cellSize.width,
                    height: cellSize.height,
                    backgroundColor: colorScale(value),
                  }}
                  className="flex items-center justify-center text-xs font-medium border border-white"
                  title={`${formatRowLabel(row)} - ${formatColLabel(col)}: ${formatValue(value)}`}
                >
                  <span className="text-white drop-shadow-sm">
                    {formatValue(value)}
                  </span>
                </div>
              );
            })}
          </div>
        ))}
      </div>
    </div>
  );
};

export default Heatmap;