import React from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import AuthHandler from '../auth/AuthHandler';

function Header({ session }) {
  const location = useLocation();
  const navigate = useNavigate();
  const isDocumentation = location.pathname.startsWith('/docs');
  const isDashboard = location.pathname.startsWith('/dashboard');

  const handleLogout = async () => {
    await AuthHandler.signOut();
    navigate("/login");
  };

  return (
    <nav className="navbar navbar-expand-lg navbar-dark bg-primary">
      <div className="container-fluid">
        <Link className="navbar-brand" to="/">
          Sparvi
        </Link>

        <button className="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
          <span className="navbar-toggler-icon"></span>
        </button>

        <div className="collapse navbar-collapse" id="navbarNav">
          <ul className="navbar-nav me-auto">
            {session && (
              <>
                <li className="nav-item">
                  <Link className={`nav-link ${isDashboard ? 'active' : ''}`} to="/dashboard">
                    <i className="bi bi-speedometer2 me-1"></i>
                    Dashboard
                  </Link>
                </li>
                <li className="nav-item">
                  <Link className={`nav-link ${isDocumentation ? 'active' : ''}`} to="/docs/validations">
                    <i className="bi bi-book me-1"></i>
                    Documentation
                  </Link>
                </li>
              </>
            )}
          </ul>

          {session && (
            <div className="d-flex">
              <span className="navbar-text me-3">
                <i className="bi bi-person-circle me-1"></i>
                {session.user.email}
              </span>
              <button className="btn btn-outline-light" onClick={handleLogout}>
                <i className="bi bi-box-arrow-right me-1"></i>
                Logout
              </button>
            </div>
          )}
        </div>
      </div>
    </nav>
  );
}

export default Header;