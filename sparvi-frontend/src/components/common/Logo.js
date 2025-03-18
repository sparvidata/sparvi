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

  // SVG logo
  const logoSvg = (
    <svg
      className={`${sizeClasses[size]} ${className}`}
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <path
        d="M12 3C7.03 3 3 7.03 3 12C3 16.97 7.03 21 12 21C16.97 21 21 16.97 21 12C21 7.03 16.97 3 12 3ZM12 19C8.13 19 5 15.87 5 12C5 8.13 8.13 5 12 5C15.87 5 19 8.13 19 12C19 15.87 15.87 19 12 19Z"
        fill="currentColor"
        className="text-primary-600"
      />
      <path
        d="M12 7L9 11H15L12 7Z"
        fill="currentColor"
        className="text-primary-500"
      />
      <path
        d="M12 17L15 13H9L12 17Z"
        fill="currentColor"
        className="text-primary-700"
      />
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