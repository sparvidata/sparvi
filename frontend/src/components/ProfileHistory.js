import React from 'react';

function ProfileHistory({ history, activeProfileIndex, onSelectProfile }) {
  if (!history || !history.length) {
    return (
      <div className="card mb-4 shadow-sm">
        <div className="card-header">
          <h5 className="mb-0">Profile History</h5>
        </div>
        <div className="card-body">
          <div className="alert alert-info">
            <i className="bi bi-info-circle-fill me-2"></i>
            No historical profile data available yet. Run the profiler multiple times to see history.
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="card mb-4 shadow-sm">
      <div className="card-header">
        <h5 className="mb-0">Profile History</h5>
      </div>
      <div className="card-body">
        <div className="table-responsive">
          <table className="table table-hover table-striped">
            <thead>
              <tr>
                <th>Date/Time</th>
                <th>Row Count</th>
                <th>Duplicate Count</th>
                <th>Anomalies</th>
                <th>Schema Shifts</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {history.map((profile, index) => (
                <tr key={index} className={activeProfileIndex === index ? "table-primary" : ""}>
                  <td>
                    {new Date(profile.timestamp).toLocaleString()}
                    {index === 0 && (
                      <span className="badge bg-info ms-2">Latest</span>
                    )}
                  </td>
                  <td>{profile.row_count.toLocaleString()}</td>
                  <td>{(profile.duplicate_count || 0).toLocaleString()}</td>
                  <td>
                    {profile.anomalies && profile.anomalies.length > 0 ? (
                      <span className="badge bg-warning text-dark">
                        {profile.anomalies.length}
                      </span>
                    ) : (
                      <span className="badge bg-success">None</span>
                    )}
                  </td>
                  <td>
                    {profile.schema_shifts && profile.schema_shifts.length > 0 ? (
                      <span className="badge bg-danger">
                        {profile.schema_shifts.length}
                      </span>
                    ) : (
                      <span className="badge bg-success">None</span>
                    )}
                  </td>
                  <td>
                    <button
                      className="btn btn-sm btn-primary"
                      onClick={() => onSelectProfile(index)}
                      disabled={activeProfileIndex === index}
                    >
                      {activeProfileIndex === index ? (
                        <>
                          <i className="bi bi-check-circle me-1"></i>
                          Active
                        </>
                      ) : (
                        "View"
                      )}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="text-muted small mt-2">
          <i className="bi bi-info-circle me-1"></i>
          Click "View" to see detailed information for a specific profile run.
        </div>
      </div>
    </div>
  );
}

export default ProfileHistory;