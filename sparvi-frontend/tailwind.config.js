module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        primary: {
          50: '#eef2ff',
          100: '#e0e7ff',
          200: '#c7d2fe',
          300: '#a5b4fc',
          400: '#818cf8',
          500: '#6366f1',
          600: '#4f46e5',
          700: '#4338ca',
          800: '#3730a3',
          900: '#312e81',
          950: '#1e1b4b',
        },
        // ... other color definitions ...
      }
    },
  },
  plugins: [require('daisyui')],
  daisyui: {
    themes: [
      {
        sparvi: {
          "primary": "#6366f1",
          "secondary": "#64748b",
          "accent": "#10b981",
          "neutral": "#1e293b",
          "base-100": "#f8fafc",
          "info": "#3abff8",
          "success": "#36d399",
          "warning": "#f59e0b",
          "error": "#ef4444",
        },
      },
      "light",
      "dark",
    ],
  },
}