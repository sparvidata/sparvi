from flask import Flask, render_template, jsonify, request
import datetime
import os
import jwt
import traceback
from functools import wraps
from dotenv import load_dotenv
from flask_cors import CORS
from sqlalchemy import inspect, create_engine
from data_quality_engine.src.profiler.profiler import profile_table
from data_quality_engine.src.validations.validation_manager import ValidationManager
from data_quality_engine.src.validations.default_validations import add_default_validations

# Load environment variables from .env file
load_dotenv()

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
        auth_header = request.headers.get("Authorization", None)
        if auth_header:
            parts = auth_header.split()
            if len(parts) == 2 and parts[0] == "Bearer":
                token = parts[1]
        if not token:
            print("DEBUG: No token provided")
            return jsonify({"error": "Token is missing!"}), 401
        try:
            print("DEBUG: Token received:", token)
            data = jwt.decode(token, app.config["SECRET_KEY"], algorithms=["HS256"])
            current_user = data["user"]
            print("DEBUG: Token valid for user:", current_user)
        except Exception as e:
            print("DEBUG: Token decoding error:", str(e))
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
    connection_string = request.args.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
    table_name = request.args.get("table", "employees")
    try:
        result = profile_table(connection_string, table_name)
        result["timestamp"] = datetime.datetime.now().isoformat()
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()  # Print the full traceback to your console
        return jsonify({"error": str(e)}), 500

# Optional: a simple index page (for testing or informational purposes)
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/tables", methods=["GET"])
@token_required
def get_tables(current_user):
    connection_string = request.args.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
    try:
        # Create engine and get inspector
        engine = create_engine(connection_string)
        inspector = inspect(engine)

        # Get all table names
        tables = inspector.get_table_names()

        return jsonify({"tables": tables})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# Initialize validation manager
validation_manager = ValidationManager()


@app.route("/api/validations", methods=["GET"])
@token_required
def get_validations(current_user):
    """Get all validation rules for a table"""
    table_name = request.args.get("table")
    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    try:
        rules = validation_manager.get_rules(table_name)
        return jsonify({"rules": rules})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/validations", methods=["POST"])
@token_required
def add_validation(current_user):
    """Add a new validation rule"""
    table_name = request.args.get("table")
    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    rule_data = request.get_json()
    if not rule_data:
        return jsonify({"error": "Rule data is required"}), 400

    required_fields = ["name", "query", "operator", "expected_value"]
    for field in required_fields:
        if field not in rule_data:
            return jsonify({"error": f"Missing required field: {field}"}), 400

    try:
        rule_id = validation_manager.add_rule(table_name, rule_data)
        return jsonify({"success": True, "rule_id": rule_id})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/validations", methods=["DELETE"])
@token_required
def delete_validation(current_user):
    """Delete a validation rule"""
    table_name = request.args.get("table")
    rule_name = request.args.get("rule_name")

    if not table_name or not rule_name:
        return jsonify({"error": "Table name and rule name are required"}), 400

    try:
        deleted = validation_manager.delete_rule(table_name, rule_name)
        if deleted:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Rule not found"}), 404
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/run-validations", methods=["POST"])
@token_required
def run_validations(current_user):
    """Run all validation rules for a table"""
    data = request.get_json()

    if not data or "table" not in data:
        return jsonify({"error": "Table name is required"}), 400

    connection_string = data.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
    table_name = data["table"]

    try:
        results = validation_manager.execute_rules(connection_string, table_name)
        return jsonify({"results": results})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/validation-history", methods=["GET"])
@token_required
def get_validation_history(current_user):
    """Get validation history for a table"""
    table_name = request.args.get("table")
    limit = request.args.get("limit", 10, type=int)

    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    try:
        history = validation_manager.get_validation_history(table_name, limit)
        return jsonify({"history": history})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/generate-default-validations", methods=["POST"])
@token_required
def generate_default_validations(current_user):
    """Generate and add default validation rules for a table"""
    data = request.get_json()

    if not data or "table" not in data:
        return jsonify({"error": "Table name is required"}), 400

    connection_string = data.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
    table_name = data["table"]

    try:
        # Add default validations to the validation manager
        result = add_default_validations(validation_manager, connection_string, table_name)

        return jsonify({
            "success": True,
            "message": f"Added {result['added']} default validation rules ({result['skipped']} skipped as duplicates)",
            "count": result['added'],
            "skipped": result['skipped'],
            "total": result['total']
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # In production, ensure you run with HTTPS (via a reverse proxy or WSGI server with SSL configured)
    app.run(debug=True)
