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
    'column_added': { icon: 'bi-plus-circle-fill', color: 'success' },
    'column_removed': { icon: 'bi-dash-circle-fill', color: 'danger' },
    'type_changed': { icon: 'bi-arrow-left-right', color: 'warning' },
    'default': { icon: 'bi-info-circle-fill', color: 'info' }
  };

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header">
        <h5 className="mb-0">Schema Shifts</h5>
      </div>
      <div className="card-body">
        <div className="alert alert-warning">
          <i className="bi bi-exclamation-triangle-fill me-2"></i>
          <strong>{shifts.length} schema changes</strong> detected since the previous profile run.
        </div>

        <div className="list-group mt-3">
          {shifts.map((shift, idx) => {
            const typeInfo = shiftTypeIcons[shift.type] || shiftTypeIcons.default;
            return (
              <div key={idx} className={`list-group-item list-group-item-${typeInfo.color} d-flex align-items-center`}>
                <i className={`bi ${typeInfo.icon} me-3 fs-5`}></i>
                <div>
                  <h6 className="mb-1">{shift.description}</h6>
                  <div className="d-flex text-muted small">
                    <div className="me-3">Type: <strong>{shift.type}</strong></div>
                    {shift.column && <div className="me-3">Column: <strong>{shift.column}</strong></div>}
                    {shift.severity && <div>Severity: <strong>{shift.severity}</strong></div>}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

export default SchemaShift;