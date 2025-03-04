import React from 'react';

function AlertsPanel({ alerts = [] }) {
  // Group alerts by severity
  const alertsBySeverity = alerts.reduce((acc, alert) => {
    const severity = alert.severity || 'info';
    if (!acc[severity]) {
      acc[severity] = [];
    }
    acc[severity].push(alert);
    return acc;
  }, {});

  // Order of severity levels
  const severityOrder = ['critical', 'error', 'warning', 'info'];

  // Map severity to Bootstrap classes
  const severityClasses = {
    critical: 'danger',
    error: 'danger',
    warning: 'warning',
    info: 'info'
  };

  // Map severity to icons
  const severityIcons = {
    critical: 'bi-exclamation-octagon-fill',
    error: 'bi-exclamation-triangle-fill',
    warning: 'bi-exclamation-circle-fill',
    info: 'bi-info-circle-fill'
  };

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header">
        <h5 className="mb-0">Alerts</h5>
      </div>
      <div className="card-body">
        {alerts.length === 0 ? (
          <div className="alert alert-success">
            <i className="bi bi-check-circle-fill me-2"></i>
            No alerts detected for this profile run.
          </div>
        ) : (
          <>
            <p>Total alerts: <strong>{alerts.length}</strong></p>

            {severityOrder.map(severity => {
              if (!alertsBySeverity[severity] || alertsBySeverity[severity].length === 0) {
                return null;
              }

              return (
                <div key={severity} className={`alert alert-${severityClasses[severity]} mb-3`}>
                  <h6 className="alert-heading">
                    <i className={`bi ${severityIcons[severity]} me-2`}></i>
                    {severity.toUpperCase()} ({alertsBySeverity[severity].length})
                  </h6>
                  <ul className="list-group list-group-flush mt-2">
                    {alertsBySeverity[severity].map((alert, idx) => (
                      <li key={idx} className="list-group-item bg-transparent border-0 py-1">
                        <div className="d-flex">
                          <div className="flex-grow-1">
                            <p className="mb-1">{alert.description}</p>
                            <small className="text-muted">
                              {alert.table} - {alert.type} - {new Date(alert.timestamp).toLocaleString()}
                            </small>
                          </div>
                        </div>
                      </li>
                    ))}
                  </ul>
                </div>
              );
            })}
          </>
        )}
      </div>
    </div>
  );
}

export default AlertsPanel;