import React, { useState, useCallback } from 'react';
import { MagnifyingGlassIcon } from '@heroicons/react/24/outline';
import useDebounce from '../../hooks/useDebounce';

const SearchInput = ({ onSearch, placeholder = "Search...", initialValue = "", delay = 300 }) => {
  const [searchTerm, setSearchTerm] = useState(initialValue);
  const debouncedSearchTerm = useDebounce(searchTerm, delay);

  // Send the debounced value to the parent component
  const handleChange = useCallback((event) => {
    setSearchTerm(event.target.value);
  }, []);

  // Effect to trigger search with debounced value
  React.useEffect(() => {
    onSearch(debouncedSearchTerm);
  }, [debouncedSearchTerm, onSearch]);

  return (
    <div className="relative">
      <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
        <MagnifyingGlassIcon className="h-5 w-5 text-secondary-400" aria-hidden="true" />
      </div>
      <input
        type="text"
        className="block w-full pl-10 pr-3 py-2 border border-secondary-300 rounded-md leading-5 bg-white placeholder-secondary-500 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
        placeholder={placeholder}
        value={searchTerm}
        onChange={handleChange}
      />
    </div>
  );
};

export default SearchInput;