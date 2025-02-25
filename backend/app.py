from flask import Flask, render_template, jsonify, request
import datetime
import os
import jwt
from functools import wraps
from dotenv import load_dotenv
from flask_cors import CORS


# Load environment variables from .env file
load_dotenv()

# Import the profiler function from your existing module
from data_quality_engine.src.profiler.profiler import profile_table

app = Flask(__name__, template_folder="templates")
CORS(app)  # This enables CORS for all routes

# Set the secret key from environment variables
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "default_secret_key")

# Dummy user store for the MVP (replace with a real user database in production)
users = {
    "admin": "password123"
}

# Decorator to require a valid token for protected routes
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        # Get token from Authorization header (Bearer token)
        auth_header = request.headers.get("Authorization", None)
        if auth_header:
            parts = auth_header.split()
            if len(parts) == 2 and parts[0] == "Bearer":
                token = parts[1]
        if not token:
            return jsonify({"error": "Token is missing!"}), 401
        try:
            data = jwt.decode(token, app.config["SECRET_KEY"], algorithms=["HS256"])
            current_user = data["user"]
        except Exception as e:
            return jsonify({"error": "Token is invalid!", "message": str(e)}), 401
        return f(current_user, *args, **kwargs)
    return decorated

@app.before_request
def log_request_info():
    print("DEBUG: Received request:", request.method, request.url)
    print("DEBUG: Headers:", dict(request.headers))
    print("DEBUG: Body:", request.get_data(as_text=True))


# Login endpoint: verifies credentials and returns a JWT token
@app.route("/api/login", methods=["POST"])
def login():
    auth_data = request.get_json()
    print("DEBUG: Received auth_data:", auth_data)
    if not auth_data or not auth_data.get("username") or not auth_data.get("password"):
        return jsonify({"error": "Missing credentials"}), 400

    username = auth_data.get("username")
    password = auth_data.get("password")
    print("DEBUG: Parsed credentials - username:", username, "password:", password)

    if username not in users or users[username] != password:
        print("DEBUG: Invalid credentials for user:", username)
        return jsonify({"error": "Invalid credentials"}), 401

    # Generate token with PyJWT
    token = jwt.encode(
        {"user": username, "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=1)},
        app.config["SECRET_KEY"],
        algorithm="HS256"
    )
    print("DEBUG: Token generated for user:", username)
    return jsonify({"token": token})



# Protected profile endpoint: returns profiling results
@app.route("/api/profile", methods=["GET"])
@token_required
def get_profile(current_user):
    # Use the connection string from query parameters or environment variable
    connection_string = request.args.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
    table_name = request.args.get("table", "employees")
    try:
        result = profile_table(connection_string, table_name)
        result["timestamp"] = datetime.datetime.now().isoformat()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Optional: a simple index page (for testing or informational purposes)
@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    # In production, ensure you run with HTTPS (via a reverse proxy or WSGI server with SSL configured)
    app.run(debug=True)
