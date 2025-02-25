import React from 'react';

function SchemaShift({ shifts }) {
  return (
    <div className="card mb-3">
      <div className="card-body">
        <h5 className="card-title">Schema Shifts</h5>
        {shifts && shifts.length > 0 ? (
          <ul>
            {shifts.map((shift, index) => <li key={index}>{shift}</li>)}
          </ul>
        ) : (
          <p>No schema shifts detected</p>
        )}
      </div>
    </div>
  );
}

export default SchemaShift;
