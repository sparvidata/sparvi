import React from 'react';

function SchemaShift({ shifts = [] }) {
  if (!shifts || shifts.length === 0) {
    return (
      <div className="card mb-4 shadow-sm">
        <div className="card-header">
          <h5 className="mb-0">Schema Shifts</h5>
        </div>
        <div className="card-body">
          <div className="alert alert-success">
            <i className="bi bi-check-circle-fill me-2"></i>
            No schema shifts detected between profile runs.
          </div>
        </div>
      </div>
    );
  }

  // Map shift types to icons and styles
  const shiftTypeIcons = {
    'column_added': { icon: 'bi-plus-circle-fill', color: 'success', title: 'Column Added' },
    'column_removed': { icon: 'bi-dash-circle-fill', color: 'danger', title: 'Column Removed' },
    'type_changed': { icon: 'bi-arrow-left-right', color: 'warning', title: 'Type Changed' },
    'length_increased': { icon: 'bi-arrows-angle-expand', color: 'info', title: 'Length Increased' },
    'default': { icon: 'bi-info-circle-fill', color: 'info', title: 'Schema Change' }
  };

  // Group shifts by type for better organization
  const groupedShifts = shifts.reduce((acc, shift) => {
    const type = shift.type || 'default';
    if (!acc[type]) {
      acc[type] = [];
    }
    acc[type].push(shift);
    return acc;
  }, {});

  // Order types for display
  const typeOrder = ['column_removed', 'type_changed', 'length_increased', 'column_added'];
  const orderedTypes = [...typeOrder, ...Object.keys(groupedShifts).filter(type => !typeOrder.includes(type))];

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header bg-warning-subtle">
        <h5 className="mb-0">
          <i className="bi bi-exclamation-triangle-fill me-2"></i>
          Schema Shifts Detected
        </h5>
      </div>
      <div className="card-body">
        <div className="alert alert-warning">
          <i className="bi bi-exclamation-triangle-fill me-2"></i>
          <strong>{shifts.length} schema changes</strong> detected since the previous profile run.
        </div>

        {orderedTypes.map(type => {
          if (!groupedShifts[type] || groupedShifts[type].length === 0) return null;

          const typeInfo = shiftTypeIcons[type] || shiftTypeIcons.default;

          return (
            <div key={type} className="mb-4">
              <h6 className={`text-${typeInfo.color} mb-2`}>
                <i className={`bi ${typeInfo.icon} me-2`}></i>
                {typeInfo.title} ({groupedShifts[type].length})
              </h6>

              <div className="list-group">
                {groupedShifts[type].map((shift, idx) => (
                  <div key={idx} className={`list-group-item list-group-item-${typeInfo.color} d-flex align-items-center`}>
                    <div className="flex-grow-1">
                      <div className="d-flex justify-content-between align-items-center">
                        <h6 className="mb-1">
                          <span className="badge bg-secondary me-2">{shift.column}</span>
                          {shift.description}
                        </h6>
                        {shift.timestamp && (
                          <small className="text-muted">
                            {new Date(shift.timestamp).toLocaleString()}
                          </small>
                        )}
                      </div>

                      {/* Display additional details based on shift type */}
                      {type === 'type_changed' && (
                        <div className="small mt-1">
                          <span className="me-2">From: <code>{shift.from_type}</code></span>
                          <i className="bi bi-arrow-right"></i>
                          <span className="ms-2">To: <code>{shift.to_type}</code></span>
                        </div>
                      )}

                      {type === 'length_increased' && (
                        <div className="small mt-1">
                          <span className="me-2">From: <code>{shift.from_length}</code></span>
                          <i className="bi bi-arrow-right"></i>
                          <span className="ms-2">To: <code>{shift.to_length}</code></span>
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          );
        })}

        <div className="text-muted small mt-3">
          <i className="bi bi-info-circle me-1"></i>
          Schema shifts may indicate data structure changes that could affect applications or reports.
        </div>
      </div>
    </div>
  );
}

export default SchemaShift;