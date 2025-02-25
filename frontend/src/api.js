// frontend/src/api.js
import axios from 'axios';

// Set the API base URL to your backend, e.g., http://localhost:5000
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:5000';

// Export the loginUser function
export const loginUser = async (username, password) => {
  const response = await axios.post(`${API_BASE_URL}/api/login`, { username, password });
  return response.data;
};

// Export the fetchProfile function
export const fetchProfile = async (token, connectionString, table) => {
  const response = await axios.get(`${API_BASE_URL}/api/profile`, {
    params: { connection_string: connectionString, table },
    headers: { Authorization: `Bearer ${token}` },
  });
  return response.data;
};
