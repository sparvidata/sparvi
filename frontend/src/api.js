// frontend/src/api.js
import axios from 'axios';

// Set the API base URL to your backend, e.g., http://localhost:5000
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:5000';

// Export the loginUser function
export const loginUser = async (email, password) => {
  const response = await axios.post(`${API_BASE_URL}/api/login`, { email, password });
  return response.data;
};

// Export the fetchProfile function
export const fetchProfile = async (token, connectionString, table) => {
  console.log("DEBUG: Using token for fetchProfile:", token);
  const response = await axios.get(`${API_BASE_URL}/api/profile`, {
    params: { connection_string: connectionString, table },
    headers: { Authorization: `Bearer ${token}` },
  });
  return response.data;
};

export const fetchAlertConfig = async (token) => {
  const response = await axios.get(`${API_BASE_URL}/api/alert-config`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return response.data;
};

export const updateAlertConfig = async (token, config) => {
  const response = await axios.post(`${API_BASE_URL}/api/alert-config`, config, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return response.data;
};

// Fetch all tables for a given connection string
export const fetchTables = async (token, connectionString) => {
  const response = await axios.get(`${API_BASE_URL}/api/tables`, {
    params: { connection_string: connectionString },
    headers: { Authorization: `Bearer ${token}` },
  });
  return response.data;
};

// Fetch validation rules for a table
export const fetchValidations = async (token, table) => {
  const response = await axios.get(`${API_BASE_URL}/api/validations`, {
    params: { table },
    headers: { Authorization: `Bearer ${token}` },
  });
  return response.data;
};

// Add a new validation rule
export const addValidationRule = async (token, table, rule) => {
  const response = await axios.post(`${API_BASE_URL}/api/validations`, rule, {
    params: { table },
    headers: { Authorization: `Bearer ${token}` },
  });
  return response.data;
};

// Delete a validation rule
export const deleteValidationRule = async (token, table, ruleName) => {
  const response = await axios.delete(`${API_BASE_URL}/api/validations`, {
    params: { table, rule_name: ruleName },
    headers: { Authorization: `Bearer ${token}` },
  });
  return response.data;
};

// Run all validation rules for a table
export const runValidations = async (token, connectionString, table) => {
  const response = await axios.post(
    `${API_BASE_URL}/api/run-validations`,
    { connection_string: connectionString, table },
    { headers: { Authorization: `Bearer ${token}` } }
  );
  return response.data;
};

// Get validation history for a table
export const fetchValidationHistory = async (token, table, limit = 10) => {
  const response = await axios.get(`${API_BASE_URL}/api/validation-history`, {
    params: { table, limit },
    headers: { Authorization: `Bearer ${token}` },
  });
  return response.data;
};

// Generate and add default validation rules for a table
export const generateDefaultValidations = async (token, connectionString, table) => {
  const response = await axios.post(
    `${API_BASE_URL}/api/generate-default-validations`,
    { connection_string: connectionString, table },
    { headers: { Authorization: `Bearer ${token}` } }
  );
  return response.data;
};