import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { signIn } from "../services/auth";

function Login() {
  const navigate = useNavigate();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      console.log("DEBUG: Submitting login", { username, password });
      const data = await loginUser(username, password);
      console.log("DEBUG: Received token", data.token);

      // Make sure to store the full token string
      const token = data.token;

      // Clear any existing token first
      localStorage.removeItem("token");

      // Store the new token
      localStorage.setItem("token", token);

      // Log the stored token to verify
      console.log("DEBUG: Stored token:", localStorage.getItem("token"));

      navigate("/dashboard");
    } catch (err) {
      console.error("DEBUG: Error in login", err);
      setError("Invalid credentials");
    }
  };

  return (
    <div className="container mt-5">
      <div className="row justify-content-center">
        <div className="col-md-6 col-lg-5">
          <div className="card shadow">
            <div className="card-header bg-primary text-white">
              <h4 className="mb-0">Login to Sparvi</h4>
            </div>
            <div className="card-body">
              {error && <div className="alert alert-danger">{error}</div>}
              <form onSubmit={handleSubmit}>
                <div className="mb-3">
                  <label htmlFor="email" className="form-label">Email:</label>
                  <input
                    type="email"
                    className="form-control"
                    id="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    required
                  />
                </div>
                <div className="mb-3">
                  <label htmlFor="password" className="form-label">Password:</label>
                  <input
                    type="password"
                    className="form-control"
                    id="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    required
                  />
                </div>
                <button 
                  type="submit" 
                  className="btn btn-primary w-100" 
                  disabled={loading}
                >
                  {loading ? (
                    <>
                      <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                      Logging in...
                    </>
                  ) : "Login"}
                </button>
              </form>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Login;