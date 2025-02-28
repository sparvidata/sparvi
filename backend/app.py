from flask import Flask, render_template, jsonify, request
import datetime
import os
import jwt
import traceback
import logging
import sys
from functools import wraps
from dotenv import load_dotenv
from flask_cors import CORS
from sqlalchemy import inspect, create_engine
from supabase import create_client, Client
from data_quality_engine.src.profiler.profiler import profile_table
from data_quality_engine.src.validations.validation_manager import ValidationManager
from data_quality_engine.src.validations.default_validations import add_default_validations


def setup_comprehensive_logging():
    """
    Set up a comprehensive logging configuration that:
    - Logs to console (stderr)
    - Logs to a file
    - Captures DEBUG level logs
    - Includes detailed log formatting
    - Handles uncaught exceptions
    """
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Set to lowest level to capture everything

    # Console Handler - writes to stderr
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.DEBUG)  # Capture all levels

    # File Handler - writes to a log file
    try:
        file_handler = logging.FileHandler('app.log', mode='a')
        file_handler.setLevel(logging.DEBUG)
    except Exception as e:
        print(f"Could not create file handler: {e}")
        file_handler = None

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )

    # Set formatter for handlers
    console_handler.setFormatter(formatter)
    if file_handler:
        file_handler.setFormatter(formatter)

    # Remove any existing handlers to prevent duplicate logs
    logger.handlers.clear()

    # Add handlers
    logger.addHandler(console_handler)
    if file_handler:
        logger.addHandler(file_handler)

    # Add handler for uncaught exceptions
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = handle_exception

    return logger


# Set up logging early
logger = setup_comprehensive_logging()

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__, template_folder="templates")
CORS(app)  # This enables CORS for all routes

# Set the secret key from environment variables
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "default_secret_key")


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
            logger.warning("No token provided")
            return jsonify({"error": "Token is missing!"}), 401

        try:
            # Use Supabase to verify the token
            url = os.getenv("SUPABASE_URL")
            key = os.getenv("SUPABASE_SERVICE_KEY")

            # Log environment variables for debugging
            logger.debug(f"Supabase URL: {url}")
            logger.debug(f"Supabase Service Key: {bool(key)}")  # Don't log actual key

            if not url or not key:
                logger.error("Supabase URL or Service Key is missing")
                return jsonify({"error": "Server configuration error"}), 500

            supabase_client = create_client(url, key)

            # Verify the JWT token
            decoded = supabase_client.auth.get_user(token)
            current_user = decoded.user.id

            logger.info(f"Token valid for user: {current_user}")
        except Exception as e:
            logger.error(f"Token verification error: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": "Token is invalid!", "message": str(e)}), 401

        return f(current_user, *args, **kwargs)

    return decorated


@app.route("/api/login", methods=["POST"])
def login():
    try:
        # Add extensive logging
        logger.debug("Login attempt started")

        auth_data = request.get_json()
        logger.debug(f"Received auth data: {auth_data}")

        # Validate input
        if not auth_data or not auth_data.get("email") or not auth_data.get("password"):
            logger.warning("Missing credentials")
            return jsonify({"error": "Missing credentials"}), 400

        email = auth_data.get("email")
        password = auth_data.get("password")

        # Log environment variables (be careful in production!)
        logger.debug(f"Supabase URL: {os.getenv('SUPABASE_URL')}")
        logger.debug(f"Supabase Anon Key: {bool(os.getenv('SUPABASE_ANON_KEY'))}")  # Just check if it exists

        # Supabase authentication
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_ANON_KEY")

        if not url or not key:
            logger.error("Supabase URL or Anon Key is missing")
            return jsonify({"error": "Server configuration error"}), 500

        supabase_client = create_client(url, key)

        # Detailed logging around authentication
        logger.debug("Attempting Supabase sign in")
        response = supabase_client.auth.sign_in_with_password({
            "email": email,
            "password": password
        })

        logger.debug("Sign in successful")
        logger.debug(f"Session details: {response.session}")

        return jsonify({
            "token": response.session.access_token,
            "user": response.session.user.model_dump()
        })

    except Exception as e:
        # Catch and log all possible exceptions
        logger.error(f"Login error: {str(e)}", exc_info=True)
        # Log the full traceback
        logger.error(traceback.format_exc())
        return jsonify({"error": "Authentication failed", "message": str(e)}), 401


@app.route("/api/env-check")
def env_check():
    """Route to verify environment variables are set"""
    return jsonify({
        "SUPABASE_URL": bool(os.getenv("SUPABASE_URL")),
        "SUPABASE_ANON_KEY": bool(os.getenv("SUPABASE_ANON_KEY")),
        "SUPABASE_SERVICE_KEY": bool(os.getenv("SUPABASE_SERVICE_KEY"))
    })

# Refresh token endpoint
@app.route("/api/refresh-token", methods=["POST"])
@token_required
def refresh_token(current_user):
    """Create a new token with a renewed expiration time"""
    new_token = jwt.encode(
        {"user": current_user, "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=24)},
        app.config["SECRET_KEY"],
        algorithm="HS256"
    )
    logger.info(f"Token refreshed for user: {current_user}")
    return jsonify({"token": new_token})


# Protected profile endpoint: returns profiling results
@app.route("/api/profile", methods=["GET"])
@token_required
def get_profile(current_user):
    connection_string = request.args.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
    table_name = request.args.get("table", "employees")
    try:
        logger.info(f"Profiling table {table_name} with connection {connection_string}")
        result = profile_table(connection_string, table_name)
        result["timestamp"] = datetime.datetime.now().isoformat()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error profiling table: {str(e)}")
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
        logger.info(f"Getting tables for connection: {connection_string}")
        # Create engine and get inspector
        engine = create_engine(connection_string)
        inspector = inspect(engine)

        # Test connection
        with engine.connect() as conn:
            pass  # Just test that we can connect

        # Get all table names
        tables = inspector.get_table_names()
        logger.info(f"Found {len(tables)} tables")

        return jsonify({"tables": tables})
    except Exception as e:
        logger.error(f"Error getting tables: {str(e)}")
        traceback.print_exc()
        error_message = str(e)
        if "could not connect" in error_message.lower():
            return jsonify({"error": "Could not connect to database. Please check your connection string."}), 500
        return jsonify({"error": error_message}), 500


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
        logger.info(f"Getting validation rules for table: {table_name}")
        rules = validation_manager.get_rules(table_name)
        return jsonify({"rules": rules})
    except Exception as e:
        logger.error(f"Error getting validation rules: {str(e)}")
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
        logger.info(f"Adding validation rule {rule_data['name']} for table {table_name}")
        rule_id = validation_manager.add_rule(table_name, rule_data)
        return jsonify({"success": True, "rule_id": rule_id})
    except Exception as e:
        logger.error(f"Error adding validation rule: {str(e)}")
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
        logger.info(f"Deleting validation rule {rule_name} for table {table_name}")
        deleted = validation_manager.delete_rule(table_name, rule_name)
        if deleted:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Rule not found"}), 404
    except Exception as e:
        logger.error(f"Error deleting validation rule: {str(e)}")
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
        logger.info(f"Running validations for table {table_name}")
        results = validation_manager.execute_rules(connection_string, table_name)
        return jsonify({"results": results})
    except Exception as e:
        logger.error(f"Error running validations: {str(e)}")
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
        logger.info(f"Getting validation history for table {table_name}, limit {limit}")
        history = validation_manager.get_validation_history(table_name, limit)
        return jsonify({"history": history})
    except Exception as e:
        logger.error(f"Error getting validation history: {str(e)}")
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
        logger.info(f"Generating default validations for table {table_name}")
        # Add default validations to the validation manager
        result = add_default_validations(validation_manager, connection_string, table_name)

        logger.info(f"Added {result['added']} default validation rules ({result['skipped']} skipped as duplicates)")
        return jsonify({
            "success": True,
            "message": f"Added {result['added']} default validation rules ({result['skipped']} skipped as duplicates)",
            "count": result['added'],
            "skipped": result['skipped'],
            "total": result['total']
        })
    except Exception as e:
        logger.error(f"Error generating default validations: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # In production, ensure you run with HTTPS (via a reverse proxy or WSGI server with SSL configured)
    app.run(debug=True)