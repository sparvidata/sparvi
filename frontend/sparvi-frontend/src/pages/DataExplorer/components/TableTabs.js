import React from 'react';
import {
  ChartBarIcon,
  TableCellsIcon,
  ClipboardDocumentCheckIcon,
  ClockIcon,
  EyeIcon
} from '@heroicons/react/24/outline';

const TableTabs = ({ activeTab, onChange, validationCount = 0 }) => {
  const tabs = [
    {
      id: 'profile',
      name: 'Profile',
      icon: ChartBarIcon,
      description: 'View data profile with statistics and distributions'
    },
    {
      id: 'columns',
      name: 'Columns',
      icon: TableCellsIcon,
      description: 'Explore column details and metadata'
    },
    {
      id: 'validations',
      name: 'Validations',
      icon: ClipboardDocumentCheckIcon,
      description: 'Manage data quality validation rules',
      count: validationCount
    },
    {
      id: 'history',
      name: 'History',
      icon: ClockIcon,
      description: 'View historical profiles and changes'
    },
    {
      id: 'preview',
      name: 'Preview Data',
      icon: EyeIcon,
      description: 'Preview sample rows from this table'
    }
  ];

  return (
    <div className="border-b border-secondary-200">
      <div className="sm:hidden">
        <select
          id="tabs"
          name="tabs"
          className="block w-full pl-3 pr-10 py-2 text-base border-secondary-300 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm rounded-md"
          value={activeTab}
          onChange={(e) => onChange(e.target.value)}
        >
          {tabs.map((tab) => (
            <option key={tab.id} value={tab.id}>
              {tab.name} {tab.count ? `(${tab.count})` : ''}
            </option>
          ))}
        </select>
      </div>

      <div className="hidden sm:block">
        <div className="border-b border-secondary-200">
          <nav className="-mb-px flex space-x-8 px-6" aria-label="Tabs">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => onChange(tab.id)}
                className={`
                  ${activeTab === tab.id
                    ? 'border-primary-500 text-primary-600'
                    : 'border-transparent text-secondary-500 hover:text-secondary-700 hover:border-secondary-300'
                  }
                  whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center
                `}
                aria-current={activeTab === tab.id ? 'page' : undefined}
                title={tab.description}
              >
                <tab.icon
                  className={`
                    ${activeTab === tab.id ? 'text-primary-500' : 'text-secondary-400'}
                    -ml-0.5 mr-2 h-5 w-5
                  `}
                  aria-hidden="true"
                />
                {tab.name}
                {tab.count ? (
                  <span
                    className={`ml-2 py-0.5 px-2.5 rounded-full text-xs font-medium ${
                      activeTab === tab.id
                        ? 'bg-primary-100 text-primary-600'
                        : 'bg-secondary-100 text-secondary-600'
                    }`}
                  >
                    {tab.count}
                  </span>
                ) : null}
              </button>
            ))}
          </nav>
        </div>
      </div>
    </div>
  );
};

export default TableTabs;