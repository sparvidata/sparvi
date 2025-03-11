import React, { useState, useEffect } from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import AuthHandler from '../auth/AuthHandler';
import { supabase } from '../lib/supabase';

function Header({ session }) {
  const location = useLocation();
  const navigate = useNavigate();
  const [userRole, setUserRole] = useState(null);
  const [organization, setOrganization] = useState(null);

  const isDocumentation = location.pathname.startsWith('/docs');
  const isDashboard = location.pathname.startsWith('/dashboard');

  // Fetch user role and organization when session changes
  useEffect(() => {
    const fetchUserInfo = async () => {
      console.log("Header: Session changed, fetching user info", session);
      if (session) {
        try {
          console.log("Header: User ID for query:", session.user.id);

          // Using Supabase directly to get the user's profile with organization
          const { data, error } = await supabase
            .from('profiles')
            .select(`
              role,
              organization_id,
              organizations (
                name
              )
            `)
            .eq('id', session.user.id)
            .single();

          console.log("Header: Profile data received:", data);
          console.log("Header: Profile error (if any):", error);

          if (data && !error) {
            console.log("Header: Setting user role to:", data.role);
            setUserRole(data.role);
            if (data.organizations) {
              console.log("Header: Setting organization:", data.organizations);
              setOrganization({
                id: data.organization_id,
                name: data.organizations.name
              });
            } else {
              console.log("Header: No organizations data found in profile");
            }
          }
        } catch (err) {
          console.error("Header: Error fetching user info:", err);
        }
      } else {
        console.log("Header: No session, clearing user role and organization");
        setUserRole(null);
        setOrganization(null);
      }
    };

    fetchUserInfo();
  }, [session]);

  // Add effect to log when userRole changes
  useEffect(() => {
    console.log("Header: Current userRole:", userRole);
    console.log("Header: Admin link should be visible:", userRole === 'admin');
  }, [userRole]);

  const handleLogout = async () => {
    await AuthHandler.signOut();
    navigate("/login");
  };

  // Force visibility for testing
  const forceShowAdmin = false; // Set to true to force display the Admin link

  return (
    <nav className="navbar navbar-expand-lg navbar-dark bg-primary">
      <div className="container-fluid">
        <Link to="/" className="d-flex align-items-center text-white text-decoration-none">
          <svg className="h-8 w-8 text-white me-2" width="32" height="32" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" fill="none">
            <path d="M16 8V16L12 12L8 16V8L12 4L16 8Z" fill="currentColor"/>
            <path d="M12 12L3 21M12 12L21 21" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
          </svg>
          <span className="fs-4 me-3">Sparvi</span>
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
                  <Link className={`nav-link ${location.pathname === '/connections' ? 'active' : ''}`} to="/connections">
                    <i className="bi bi-database me-1"></i>
                    Connections
                  </Link>
                </li>
                <li className="nav-item">
                  <Link className={`nav-link ${isDocumentation ? 'active' : ''}`} to="/docs/validations">
                    <i className="bi bi-book me-1"></i>
                    Documentation
                  </Link>
                </li>
                {/* Show Admin link for admins or when forcing for debugging */}
                {(userRole === 'admin' || forceShowAdmin) && (
                  <li className="nav-item">
                    <Link className={`nav-link ${location.pathname === '/admin' ? 'active' : ''}`} to="/admin">
                      <i className="bi bi-shield-lock me-1"></i>
                      Admin {userRole !== 'admin' && '(Forced)'}
                    </Link>
                  </li>
                )}
              </>
            )}
          </ul>

          {session && (
            <div className="d-flex align-items-center">


              {/* Display organization name if available */}
              {organization && (
                <span className="navbar-text me-3 d-none d-md-inline">
                  <i className="bi bi-building me-1"></i>
                  {organization.name}
                </span>
              )}
              <span className="navbar-text me-3">
                <i className="bi bi-person-circle me-1"></i>
                {session.user.email}
              </span>
              {/* Display user role for debugging */}
              <span className="navbar-text me-3 d-none d-md-inline">
                Role: {userRole || 'unknown'}
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