import React from 'react';

function AnomalyList({ anomalies = [] }) {
  if (!anomalies || anomalies.length === 0) {
    return (
      <div className="card mb-4 shadow-sm">
        <div className="card-header">
          <h5 className="mb-0">Anomalies</h5>
        </div>
        <div className="card-body">
          <div className="alert alert-success">
            <i className="bi bi-check-circle-fill me-2"></i>
            No anomalies detected in this profiling run.
          </div>
        </div>
      </div>
    );
  }

  // Group anomalies by type
  const anomalyTypes = {};
  anomalies.forEach(anomaly => {
    const type = anomaly.type || 'other';
    if (!anomalyTypes[type]) {
      anomalyTypes[type] = [];
    }
    anomalyTypes[type].push(anomaly);
  });

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header">
        <h5 className="mb-0">Anomalies</h5>
      </div>
      <div className="card-body">
        <p>
          <strong>{anomalies.length} anomalies</strong> detected in this profiling run:
        </p>

        <div className="accordion mt-3" id="anomalyAccordion">
          {Object.entries(anomalyTypes).map(([type, items], index) => (
            <div className="accordion-item" key={type}>
              <h2 className="accordion-header" id={`heading-${type}`}>
                <button
                  className="accordion-button"
                  type="button"
                  data-bs-toggle="collapse"
                  data-bs-target={`#collapse-${type}`}
                  aria-expanded={index === 0 ? "true" : "false"}
                  aria-controls={`collapse-${type}`}
                >
                  <strong>{type.replace(/_/g, ' ').toUpperCase()}</strong> ({items.length})
                </button>
              </h2>
              <div
                id={`collapse-${type}`}
                className={`accordion-collapse collapse ${index === 0 ? 'show' : ''}`}
                aria-labelledby={`heading-${type}`}
                data-bs-parent="#anomalyAccordion"
              >
                <div className="accordion-body">
                  <div className="table-responsive">
                    <table className="table table-hover">
                      <thead>
                        <tr>
                          <th>Description</th>
                          <th>Severity</th>
                          {items[0].column && <th>Column</th>}
                          <th>Details</th>
                        </tr>
                      </thead>
                      <tbody>
                        {items.map((anomaly, i) => (
                          <tr key={i}>
                            <td>{anomaly.description}</td>
                            <td>
                              <span className={`badge bg-${
                                anomaly.severity === 'high' || anomaly.severity === 'critical' ? 'danger' :
                                anomaly.severity === 'medium' ? 'warning' : 'info'
                              }`}>
                                {anomaly.severity}
                              </span>
                            </td>
                            {anomaly.column && <td>{anomaly.column}</td>}
                            <td>
                              {anomaly.details && (
                                <button
                                  type="button"
                                  className="btn btn-sm btn-outline-secondary"
                                  data-bs-toggle="tooltip"
                                  data-bs-placement="top"
                                  title={JSON.stringify(anomaly.details, null, 2)}
                                >
                                  <i className="bi bi-info-circle"></i>
                                </button>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export default AnomalyList;