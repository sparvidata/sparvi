(function cleanupLocalStorage() {
  // Check for and remove any direct connection strings
  const storedConn = localStorage.getItem('connectionString');
  if (storedConn && (storedConn.includes('://') || storedConn.includes('password'))) {
    console.warn('Removing unsafe connection string from localStorage');
    localStorage.removeItem('connectionString');
    // If we have connection ID, great, otherwise save a flag that we cleared something
    if (!localStorage.getItem('connectionId')) {
      localStorage.setItem('connectionReference', 'cleared-credentials');
    }
  }
})();

import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import 'bootstrap/dist/css/bootstrap.min.css';
import 'bootstrap-icons/font/bootstrap-icons.css';
import './colors.css';

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
