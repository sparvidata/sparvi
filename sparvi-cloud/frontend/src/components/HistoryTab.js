import React from 'react';
import ProfileHistory from './ProfileHistory';

function HistoryTab({ profileHistory, historyLoading, activeProfileIndex, onSelectProfile }) {
  return (
    <div>
      <div className="alert alert-info mb-4">
        <i className="bi bi-info-circle-fill me-2"></i>
        <strong>History Tab:</strong> Select a profile run to view its data across all dashboard tabs.
      </div>

      {historyLoading ? (
        <div className="d-flex justify-content-center my-4">
          <div className="spinner-border text-primary" role="status">
            <span className="visually-hidden">Loading history...</span>
          </div>
        </div>
      ) : (
        <ProfileHistory
          history={profileHistory}
          activeProfileIndex={activeProfileIndex}
          onSelectProfile={onSelectProfile}
        />
      )}
    </div>
  );
}

export default HistoryTab;