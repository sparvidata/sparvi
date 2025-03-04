import React from 'react';
import { Link, useParams, Navigate } from 'react-router-dom';

// Documentation pages will be imported here
import ValidationGuide from './docs/ValidationGuide';

function Documentation() {
  const { page } = useParams();

  // Define available documentation pages
  const pages = {
    'overview': {
      title: 'Documentation Overview',
      component: () => (
        <div>
          <h1>Sparvi Documentation</h1>
          <p>Welcome to the Sparvi documentation. Please select a topic from the menu.</p>
        </div>
      ),
      icon: 'bi-book'
    },
    'validations': {
      title: 'Validation Rules Guide',
      component: ValidationGuide,
      icon: 'bi-check-circle'
    }
    // Add more documentation pages as needed

  };

  // If no page is specified, redirect to overview
  if (!page) {
    return <Navigate to="/docs/overview" />;
  }

  // If invalid page is specified, redirect to overview
  if (!pages[page]) {
    return <Navigate to="/docs/overview" />;
  }

  // Get the component for the current page
  const PageComponent = pages[page].component;

  return (
    <div className="container-fluid mt-3">
      <div className="row">
        {/* Sidebar for navigation */}
        <div className="col-md-3 col-lg-2 d-md-block bg-light sidebar">
          <div className="position-sticky pt-3">
            <h5 className="sidebar-heading d-flex justify-content-between align-items-center px-3 mt-4 mb-1 text-muted">
              <span>Documentation</span>
            </h5>
            <ul className="nav flex-column">
              {Object.keys(pages).map((key) => (
                <li className="nav-item" key={key}>
                  <Link
                    to={`/docs/${key}`}
                    className={`nav-link ${page === key ? 'active' : ''}`}
                  >
                    <i className={`bi ${pages[key].icon} me-2`}></i>
                    {pages[key].title}
                  </Link>
                </li>
              ))}
            </ul>
          </div>
        </div>

        {/* Main content area */}
        <main className="col-md-9 ms-sm-auto col-lg-10 px-md-4">
          <div className="d-flex justify-content-between flex-wrap flex-md-nowrap align-items-center pt-3 pb-2 mb-3 border-bottom">
            <h1 className="h2">{pages[page].title}</h1>
            <div className="btn-toolbar mb-2 mb-md-0">
              <button className="btn btn-sm btn-outline-secondary" onClick={() => window.print()}>
                <i className="bi bi-printer me-1"></i>
                Print
              </button>
            </div>
          </div>

          {/* Render the page component */}
          <PageComponent />
        </main>
      </div>
    </div>
  );
}

export default Documentation;