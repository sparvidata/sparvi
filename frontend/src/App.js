import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Login from './components/Login';
import Dashboard from './components/Dashboard';
import Documentation from './components/Documentation';
import Header from './components/Header';

function App() {
  const isAuthenticated = localStorage.getItem("token");

  return (
    <Router>
      <Header />
      <div className="container-fluid mt-3">
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/dashboard" element={
            isAuthenticated ? <Dashboard /> : <Navigate to="/login" />
          } />
          <Route path="/docs/:page" element={<Documentation />} />
          <Route path="/docs" element={<Navigate to="/docs/overview" />} />
          <Route path="/" element={<Navigate to={isAuthenticated ? "/dashboard" : "/login"} />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;