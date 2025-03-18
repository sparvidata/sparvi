import React from 'react';
import { Link } from 'react-router-dom';

const Logo = ({ size = 'medium', withText = true, className = '' }) => {
  // Define size classes
  const sizeClasses = {
    small: 'h-6 w-6',
    medium: 'h-8 w-8',
    large: 'h-12 w-12',
  };

  // Define text size classes
  const textSizeClasses = {
    small: 'text-lg',
    medium: 'text-xl',
    large: 'text-2xl',
  };

  // Updated SVG logo
  const logoSvg = (
    <svg
      className={`${sizeClasses[size]} text-primary-600 ${className}`}
      width="32"
      height="32"
      viewBox="0 0 24 24"
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
    >
      <path d="M16 8V16L12 12L8 16V8L12 4L16 8Z" fill="currentColor"/>
      <path d="M12 12L3 21M12 12L21 21" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
    </svg>
  );

  if (!withText) {
    return logoSvg;
  }

  return (
    <Link to="/" className="flex items-center">
      {logoSvg}
      <span className={`ml-2 font-bold ${textSizeClasses[size]} text-primary-600`}>
        Sparvi
      </span>
    </Link>
  );
};

export default Logo;