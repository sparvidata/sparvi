import React from 'react';

function AnomalyList({ anomalies }) {
  return (
    <div className="card mb-3">
      <div className="card-body">
        <h5 className="card-title">Anomalies</h5>
        {anomalies && anomalies.length > 0 ? (
          <ul>
            {anomalies.map((a, index) => <li key={index}>{a.description}</li>)}
          </ul>
        ) : (
          <p>No anomalies detected</p>
        )}
      </div>
    </div>
  );
}

export default AnomalyList;
