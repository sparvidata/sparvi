// frontend/src/components/Login.js
import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { loginUser } from "../api"; // This should correctly import the loginUser function

function Login() {
  const navigate = useNavigate();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      console.log("DEBUG: Submitting login", { username, password });
      const data = await loginUser(username, password);
      console.log("DEBUG: Received token", data.token);
      localStorage.setItem("token", data.token);
      navigate("/dashboard");
    } catch (err) {
      console.error("DEBUG: Error in login", err);
      setError("Invalid credentials");
    }
  };


  return (
    <div className="container mt-5">
      <h2>Login</h2>
      {error && <p className="text-danger">{error}</p>}
      <form onSubmit={handleSubmit}>
        <div className="mb-3">
          <label>Username:</label>
          <input
            type="text"
            className="form-control"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            required
          />
        </div>
        <div className="mb-3">
          <label>Password:</label>
          <input
            type="password"
            className="form-control"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
          />
        </div>
        <button type="submit" className="btn btn-primary">Login</button>
      </form>
    </div>
  );
}

export default Login;
