import urllib
import psutil
import supabase
from flask import Flask, render_template, jsonify, request
import datetime
import os
import traceback
import logging
import sys
from functools import wraps
from dotenv import load_dotenv
from flask_cors import CORS
from sqlalchemy import inspect, create_engine, text
from supabase import create_client, Client
from sparvi.profiler.profile_engine import profile_table
from sparvi.validations.default_validations import get_default_validations
from sparvi.validations.validator import run_validations as sparvi_run_validations

# Import from our cloud service components
from core.storage.supabase_manager import SupabaseManager
from core.validations.supabase_validation_manager import SupabaseValidationManager
from core.history.supabase_profile_history import SupabaseProfileHistoryManager


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
CORS(app,
     resources={r"/api/*": {"origins": ["https://cloud.sparvi.io", "http://localhost:3000"]}},
     supports_credentials=True,
     allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
     expose_headers=["Content-Type", "Authorization"],
     methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])

# Set the secret key from environment variables
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "default_secret_key")

# Initialize Supabase validation manager
validation_manager = SupabaseValidationManager()

run_validations_after_profile = True  # Set to True to automatically run validations after profiling

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

            # Get the user's organization ID
            supabase_mgr = SupabaseManager()
            organization_id = supabase_mgr.get_user_organization(current_user)

            if not organization_id:
                logger.error(f"No organization found for user: {current_user}")
                return jsonify({"error": "User has no associated organization"}), 403

        except Exception as e:
            logger.error(f"Token verification error: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": "Token is invalid!", "message": str(e)}), 401

        # Pass both user_id and organization_id to the decorated function
        return f(current_user, organization_id, *args, **kwargs)

    return decorated


def build_connection_string(connection_data):
    """Build connection string from stored connection details"""
    conn_type = connection_data["connection_type"]
    details = connection_data["connection_details"]

    if conn_type == "snowflake":
        username = details.get("username", "")
        password = details.get("password", "")
        account = details.get("account", "")
        database = details.get("database", "")
        schema = details.get("schema", "PUBLIC")
        warehouse = details.get("warehouse", "")

        # URL encode password
        encoded_password = urllib.parse.quote_plus(password)

        return f"snowflake://{username}:{encoded_password}@{account}/{database}/{schema}?warehouse={warehouse}"

    # Add other connection types as needed
    return None


def get_current_user():
    """Get current user from auth token"""
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return None

    token = auth_header.split(' ')[1]
    supabase_mgr = SupabaseManager()
    return supabase_mgr.verify_token(token)


def run_validation_rules_internal(user_id, organization_id, data):
    """Internal version of run_validation_rules that can be called from other functions"""
    if not data or "table" not in data:
        return {"error": "Table name is required"}

    connection_string = data.get("connection_string")
    table_name = data["table"]
    profile_history_id = data.get("profile_history_id")

    try:
        force_gc()
        # Get all rules
        rules = validation_manager.get_rules(organization_id, table_name)

        if not rules:
            return {"results": []}

        # Convert from Supabase format to sparvi-core format if needed
        validation_rules = []
        for rule in rules:
            validation_rules.append({
                "name": rule["rule_name"],
                "description": rule["description"],
                "query": rule["query"],
                "operator": rule["operator"],
                "expected_value": rule["expected_value"]
            })

        # Log memory usage before validation
        log_memory_usage("Before validation")
        force_gc()

        # Execute the rules in batches to manage memory
        results = []
        batch_size = 5  # Process in small batches

        for i in range(0, len(validation_rules), batch_size):
            batch = validation_rules[i:i + batch_size]
            logger.info(f"Processing validation batch {i // batch_size + 1} with {len(batch)} rules")

            # Execute this batch of rules
            batch_results = sparvi_run_validations(connection_string, batch)
            results.extend(batch_results)
            force_gc()

            # Store batch results in Supabase
            for j, result in enumerate(batch_results):
                if i + j < len(rules):  # Safety check
                    actual_value = result.get("actual_value", None)
                    validation_manager.store_validation_result(
                        organization_id,
                        rules[i + j]["id"],
                        result["is_valid"],
                        actual_value,
                        profile_history_id
                    )

            # Force garbage collection between batches
            force_gc()

        # Log memory usage after validation
        log_memory_usage("After validation")

        return {"results": results}

    except Exception as e:
        logger.error(f"Error running validations internal: {str(e)}")
        traceback.print_exc()
        return {"error": str(e)}


def log_memory_usage(label=""):
    """Log current memory usage"""
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        memory_mb = mem_info.rss / (1024 * 1024)
        logger.info(f"Memory Usage [{label}]: {memory_mb:.2f} MB")

        # Alert if memory is getting high (adjust threshold as needed for your environment)
        if memory_mb > 500:  # Alert if using more than 500 MB
            logger.warning(f"High memory usage detected: {memory_mb:.2f} MB")

        return memory_mb
    except ImportError:
        logger.warning("psutil not installed - cannot log memory usage")
        return 0
    except Exception as e:
        logger.warning(f"Error logging memory usage: {str(e)}")
        return 0

def force_gc():
    """Force garbage collection to free memory"""
    import gc
    collected = gc.collect()
    logger.debug(f"Garbage collection: collected {collected} objects")
    return collected

@app.route('/', defaults={'path': ''}, methods=['OPTIONS'])

@app.route('/<path:path>', methods=['OPTIONS'])
def options_handler(path):
    return app.make_default_options_response()

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


@app.route("/api/validations", methods=["GET"])
@token_required
def get_validations(current_user, organization_id):
    """Get all validation rules for a table"""
    table_name = request.args.get("table")
    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    try:
        logger.info(f"Getting validation rules for organization: {organization_id}, table: {table_name}")
        rules = validation_manager.get_rules(organization_id, table_name)
        logger.info(f"Retrieved {len(rules)} validation rules")
        logger.debug(f"Rules content: {rules}")
        return jsonify({"rules": rules})
    except Exception as e:
        logger.error(f"Error getting validation rules: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/validations", methods=["POST"])
@token_required
def add_validation_rule(current_user, organization_id):
    """Add a new validation rule for a table"""
    table_name = request.args.get("table")
    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    rule_data = request.get_json()
    if not rule_data:
        return jsonify({"error": "Rule data is required"}), 400

    required_fields = ["name", "description", "query", "operator", "expected_value"]
    for field in required_fields:
        if field not in rule_data:
            return jsonify({"error": f"Missing required field: {field}"}), 400

    try:
        logger.info(f"Adding validation rule for organization: {organization_id}, table: {table_name}")
        rule_id = validation_manager.add_rule(organization_id, table_name, rule_data)

        if rule_id:
            return jsonify({"success": True, "id": rule_id})
        else:
            return jsonify({"error": "Failed to add validation rule"}), 500
    except Exception as e:
        logger.error(f"Error adding validation rule: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/validations", methods=["DELETE"])
@token_required
def delete_validation_rule(current_user, organization_id):
    """Delete a validation rule"""
    table_name = request.args.get("table")
    rule_name = request.args.get("rule_name")

    if not table_name:
        return jsonify({"error": "Table name is required"}), 400
    if not rule_name:
        return jsonify({"error": "Rule name is required"}), 400

    try:
        logger.info(f"Deleting validation rule {rule_name} for organization: {organization_id}, table: {table_name}")
        success = validation_manager.delete_rule(organization_id, table_name, rule_name)

        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Rule not found or delete failed"}), 404
    except Exception as e:
        logger.error(f"Error deleting validation rule: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/run-validations", methods=["POST"])
@token_required
def run_validation_rules(current_user, organization_id):
    """Run all validation rules for a table"""
    data = request.get_json()
    logger.info(f"Run validations request: {data}")

    log_memory_usage()

    if not data or "table" not in data:
        logger.warning("Table name missing in request")
        return jsonify({"error": "Table name is required"}), 400

    connection_string = data.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
    table_name = data["table"]
    profile_history_id = data.get("profile_history_id")  # Get profile_history_id if provided

    logger.info(f"Running validations with profile_history_id: {profile_history_id}")

    try:
        logger.info(
            f"Running validations for org: {organization_id}, table: {table_name}, connection: {connection_string}")

        log_memory_usage()

        # Get all rules first to check if there are any
        rules = validation_manager.get_rules(organization_id, table_name)
        logger.info(f"Found {len(rules)} rules to execute")

        # Convert from Supabase format to sparvi-core format if needed
        validation_rules = []
        for rule in rules:
            validation_rules.append({
                "name": rule["rule_name"],
                "description": rule["description"],
                "query": rule["query"],
                "operator": rule["operator"],
                "expected_value": rule["expected_value"]
            })

        # Execute the rules using the sparvi-core package
        if validation_rules:
            results = sparvi_run_validations(connection_string, validation_rules)

            # Store results in Supabase
            for i, result in enumerate(results):
                # Check if actual_value exists in the result
                actual_value = result.get("actual_value", None)

                validation_manager.store_validation_result(
                    organization_id,
                    rules[i]["id"],
                    result["is_valid"],
                    actual_value,
                    profile_history_id  # Pass the profile_history_id
                )

            log_memory_usage()
            logger.info(f"Validation execution complete, got {len(results)} results")
            logger.info(f"Validation results: {results}")

            return jsonify({"results": results})
        else:
            return jsonify({"results": [], "message": "No validation rules found"})

    except Exception as e:
        logger.error(f"Error running validations: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/generate-default-validations", methods=["POST"])
@token_required
def generate_default_validations(current_user, organization_id):
    data = request.get_json()

    logger.info(f"Received data: {data}")

    if not data or "table" not in data:
        return jsonify({"error": "Table name is required"}), 400

    # Extract values and provide fallbacks
    connection_string = data.get("connection_string")
    if not connection_string:
        connection_string = os.getenv("DEFAULT_CONNECTION_STRING")

    table_name = data["table"]

    try:
        # Get existing rules
        existing_rules = validation_manager.get_rules(organization_id, table_name)
        existing_rule_names = {rule['rule_name'] for rule in existing_rules}

        # Generate potential new validations using sparvi-core
        validations = get_default_validations(connection_string, table_name)

        count_added = 0
        count_skipped = 0

        for validation in validations:
            try:
                # Skip if rule with same name already exists
                if validation['name'] in existing_rule_names:
                    count_skipped += 1
                    continue

                validation_manager.add_rule(organization_id, table_name, validation)
                count_added += 1
            except Exception as e:
                logger.error(f"Failed to add validation rule {validation['name']}: {str(e)}")

        result = {
            "added": count_added,
            "skipped": count_skipped,
            "total": count_added + count_skipped
        }

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


@app.route("/api/tables", methods=["GET"])
@token_required
def get_tables(current_user, organization_id):
    """Get all tables for a connection"""
    try:
        connection_string = request.args.get("connection_string")

        if not connection_string:
            return jsonify({"error": "Connection string is required"}), 400

        # Parse connection string to extract components
        parts = connection_string.replace('snowflake://', '').split('@')
        username = parts[0].split(':')[0]  # Get username
        connection_details = parts[1]

        # Get connection details from Supabase
        supabase_mgr = SupabaseManager()
        db_connections = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("organization_id", organization_id) \
            .execute()

        if not db_connections.data:
            return jsonify({"error": "Connection not found"}), 404

        # Get credentials from connection_details JSON
        conn_details = db_connections.data[0]["connection_details"]
        password = conn_details["password"]

        # Build proper connection string with password
        proper_connection_string = f"snowflake://{username}:{password}@{connection_details}"

        # Create engine with complete connection string
        engine = create_engine(proper_connection_string)
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        return jsonify({"tables": tables})

    except Exception as e:
        logger.error(f"Error getting tables: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    return render_template("index.html", version=os.getenv("SPARVI_CORE_VERSION", "Unknown"))


# In app.py, find the get_profile function (around line 245)
# Update it with more detailed logging:

@app.route("/api/profile", methods=["GET"])
@token_required
def get_profile(current_user, organization_id):
    connection_string = request.args.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
    table_name = request.args.get("table", "employees")

    # Explicitly set include_samples to false - no row data in profiles
    include_samples = False  # Override to always be false regardless of request

    try:
        logger.info(f"========== PROFILING STARTED ==========")
        logger.info(f"Profiling table {table_name} with connection {connection_string[:20]}...")
        logger.info(f"User ID: {current_user}, Organization ID: {organization_id}")

        # Log memory usage at start
        log_memory_usage("Before profiling")
        force_gc()

        # Check if connection string is properly formatted
        if "://" not in connection_string:
            logger.error(f"Invalid connection string format: {connection_string[:20]}...")
            return jsonify({"error": "Invalid connection string format"}), 400

        # Resolve any environment variable references in connection string
        from core.utils.connection_utils import resolve_connection_string, detect_connection_type
        resolved_connection = resolve_connection_string(connection_string)
        db_type = detect_connection_type(resolved_connection)
        logger.info(f"Database type detected: {db_type}")

        # Create profile history manager
        profile_history = SupabaseProfileHistoryManager()
        logger.info("Created SupabaseProfileHistoryManager")

        # Try to get previous profile to detect changes
        previous_profile = profile_history.get_latest_profile(organization_id, table_name)
        logger.info(f"Previous profile found: {previous_profile is not None}")

        # Set reasonable timeouts for Snowflake queries
        if db_type == "snowflake":
            # Set query timeout options
            from urllib.parse import parse_qs, urlparse, urlencode, urlunparse

            # Parse the connection string
            parsed_url = urlparse(resolved_connection)

            # Get existing query parameters
            query_params = parse_qs(parsed_url.query)

            # Add timeout parameters if not already present
            if 'statement_timeout_in_seconds' not in query_params:
                query_params['statement_timeout_in_seconds'] = ['300']  # 5 minutes

            # Rebuild the query string
            new_query = urlencode(query_params, doseq=True)

            # Rebuild the connection string
            resolved_connection = urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                parsed_url.params,
                new_query,
                parsed_url.fragment
            ))

            logger.info(f"Added timeout parameters to Snowflake connection")

        # Run the profiler using the sparvi-core package - with no samples
        logger.info(f"Starting profile_table call")
        result = profile_table(resolved_connection, table_name, previous_profile, include_samples=include_samples)
        result["timestamp"] = datetime.datetime.now().isoformat()
        logger.info(f"Profile completed with {len(result)} keys")

        # Log memory usage after profiling
        log_memory_usage("After profiling")
        force_gc()

        # Check for problematic fields
        problematic_fields = [k for k in result.keys() if not isinstance(k, str)]
        if problematic_fields:
            logger.warning(f"Found problematic field keys: {problematic_fields}")
            # Try to clean up problematic fields
            for field in problematic_fields:
                logger.warning(f"Removing problematic field: {field}")
                del result[field]

        # Save the profile to Supabase - use original connection string to avoid storing credentials
        from core.utils.connection_utils import sanitize_connection_string
        sanitized_connection = sanitize_connection_string(connection_string)
        logger.info("About to save profile to Supabase")

        # Force garbage collection before saving profile
        log_memory_usage("After garbage collection, before saving profile")

        profile_id = None
        try:
            profile_id = profile_history.save_profile(current_user, organization_id, result, sanitized_connection)
            logger.info(f"Profile save result: {profile_id}")
            force_gc()

            # Now that we have the profile_id, we can run validations with it
            if profile_id and run_validations_after_profile:
                logger.info(f"Automatically running validations for profile {profile_id}")
                try:
                    validation_data = {
                        "table": table_name,
                        "connection_string": connection_string,
                        "profile_history_id": profile_id  # Pass the profile ID
                    }
                    validation_results = run_validation_rules_internal(current_user, organization_id, validation_data)
                    if validation_results and "results" in validation_results:
                        result["validation_results"] = validation_results["results"]
                        logger.info(f"Added {len(validation_results['results'])} validation results to profile")
                except Exception as val_error:
                    logger.error(f"Error running automatic validations: {str(val_error)}")
                    # Continue even if validation fails

        except Exception as save_error:
            logger.error(f"Exception saving profile to Supabase: {str(save_error)}")
            logger.error(traceback.format_exc())
            # Continue execution even if save fails

        # Get trend data from history
        trends = profile_history.get_trends(organization_id, table_name)
        if not isinstance(trends, dict) or "error" not in trends:
            result["trends"] = trends
            logger.info(f"Added trends data with {len(trends.get('timestamps', []))} points")
        else:
            logger.warning(f"Could not get trends: {trends.get('error', 'Unknown error')}")

        # Final memory usage check
        log_memory_usage("End of profile endpoint")
        force_gc()

        return jsonify(result)
    except Exception as e:
        logger.error(f"Error profiling table: {str(e)}")
        traceback.print_exc()  # Print the full traceback to your console
        return jsonify({"error": str(e)}), 500

@app.route("/api/validations/<rule_id>", methods=["PUT"])
@token_required
def update_validation(current_user, organization_id, rule_id):
    """Update an existing validation rule"""
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
        logger.info(f"Updating validation rule {rule_id} for table {table_name}")
        success = validation_manager.update_rule(organization_id, rule_id, rule_data)

        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Rule not found or update failed"}), 404
    except Exception as e:
        logger.error(f"Error updating validation rule: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/setup-user", methods=["POST"])
def setup_user():
    data = request.get_json()

    user_id = data.get("user_id")
    email = data.get("email")
    first_name = data.get("first_name", "")
    last_name = data.get("last_name", "")
    org_name = data.get("organization_name") or f"{first_name or email.split('@')[0]}'s Organization"

    if not user_id or not email:
        logger.error(f"Missing required fields for setup_user: user_id={user_id}, email={email}")
        return jsonify({"error": "Missing required fields"}), 400

    try:
        # Use service role key for admin privileges
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_KEY")

        if not url or not key:
            logger.error("Missing Supabase configuration")
            return jsonify({"error": "Server configuration error"}), 500

        supabase_client = create_client(url, key)

        # Check if profile exists
        profile_check = supabase_client.table("profiles").select("*").eq("id", user_id).execute()

        if profile_check.data and len(profile_check.data) > 0:
            logger.info(f"User already has a profile: {profile_check.data[0]}")

            # Check if organization exists
            if profile_check.data[0].get("organization_id"):
                org_id = profile_check.data[0].get("organization_id")
                org_check = supabase_client.table("organizations").select("*").eq("id", org_id).execute()

                if org_check.data and len(org_check.data) > 0:
                    logger.info(f"User has an organization: {org_check.data[0]}")
                    return jsonify({"success": True, "message": "User already set up"})

                logger.info(f"Organization {org_id} referenced by profile doesn't exist. Creating new organization...")

        # Create organization
        logger.info(f"Creating organization: {org_name}")

        try:
            org_response = supabase_client.table("organizations").insert({"name": org_name}).execute()

            if not org_response.data or len(org_response.data) == 0:
                logger.error("Failed to create organization: No data returned")
                logger.error(f"Response: {org_response}")
                return jsonify({"error": "Failed to create organization"}), 500

            org_id = org_response.data[0]["id"]
            logger.info(f"Created organization with ID: {org_id}")

            # Create or update profile
            if profile_check.data and len(profile_check.data) > 0:
                logger.info(f"Updating existing profile with org_id: {org_id}")
                profile_response = supabase_client.table("profiles").update({
                    "organization_id": org_id,
                    "first_name": first_name,
                    "last_name": last_name
                }).eq("id", user_id).execute()
            else:
                logger.info(f"Creating new profile for user: {user_id}")
                profile_response = supabase_client.table("profiles").insert({
                    "id": user_id,
                    "email": email,
                    "first_name": first_name,
                    "last_name": last_name,
                    "organization_id": org_id,
                    "role": "admin"
                }).execute()

            if not profile_response.data or len(profile_response.data) == 0:
                logger.error("Failed to create/update profile: No data returned")
                logger.error(f"Response: {profile_response}")
                return jsonify({"error": "Failed to create/update profile"}), 500

            logger.info(f"Profile operation successful: {profile_response.data[0]}")

            # Verify success
            verification_profile = supabase_client.table("profiles").select("*").eq("id", user_id).execute()
            if verification_profile.data and len(verification_profile.data) > 0:
                logger.info(
                    f"Verification successful - user now has profile with organization: {verification_profile.data[0]}")
            else:
                logger.warning("Verification failed - profile still not visible")

            return jsonify({"success": True})

        except Exception as e:
            logger.error(f"Error in organization/profile operation: {str(e)}")
            return jsonify({"error": str(e)}), 500

    except Exception as e:
        logger.error(f"Error in setup-user: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


# These routes will be added to your backend/app.py file

@app.route("/api/admin/users", methods=["GET"])
@token_required
def get_users(current_user, organization_id):
    """Get all users in the organization (admin only)"""
    # Check if user is an admin
    supabase_mgr = SupabaseManager()
    user_role = supabase_mgr.get_user_role(current_user)

    if user_role != 'admin':
        logger.warning(f"Non-admin user {current_user} attempted to access admin endpoint")
        return jsonify({"error": "Admin access required"}), 403

    try:
        users = supabase_mgr.get_organization_users(organization_id)
        return jsonify({"users": users})
    except Exception as e:
        logger.error(f"Error getting users: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/users/<user_id>", methods=["PUT"])
@token_required
def update_user(current_user, organization_id, user_id):
    """Update a user's details (admin only)"""
    # Check if user is an admin
    supabase_mgr = SupabaseManager()
    user_role = supabase_mgr.get_user_role(current_user)

    if user_role != 'admin':
        logger.warning(f"Non-admin user {current_user} attempted to access admin endpoint")
        return jsonify({"error": "Admin access required"}), 403

    # Get update data
    data = request.get_json()
    if not data:
        return jsonify({"error": "No update data provided"}), 400

    try:
        # Ensure user belongs to the same organization
        user_details = supabase_mgr.get_user_details(user_id)
        if user_details.get('organization_id') != organization_id:
            return jsonify({"error": "User not in your organization"}), 403

        # Update the user
        success = supabase_mgr.update_user(user_id, data)
        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to update user"}), 500
    except Exception as e:
        logger.error(f"Error updating user: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/users", methods=["POST"])
@token_required
def invite_user(current_user, organization_id):
    """Invite a new user to the organization (admin only)"""
    # Check if user is an admin
    supabase_mgr = SupabaseManager()
    user_role = supabase_mgr.get_user_role(current_user)

    if user_role != 'admin':
        logger.warning(f"Non-admin user {current_user} attempted to access admin endpoint")
        return jsonify({"error": "Admin access required"}), 403

    # Get invite data
    data = request.get_json()
    if not data or 'email' not in data:
        return jsonify({"error": "Email address is required"}), 400

    try:
        # Generate a unique invite link
        invite_data = supabase_mgr.create_user_invite(organization_id, data['email'],
                                                      data.get('role', 'member'),
                                                      data.get('first_name', ''),
                                                      data.get('last_name', ''))

        # TODO: Send email with invite link (would typically use a service like SendGrid)
        # For now, just return the invite data that would be included in the email

        return jsonify({"success": True, "invite": invite_data})
    except Exception as e:
        logger.error(f"Error inviting user: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/users/<user_id>", methods=["DELETE"])
@token_required
def remove_user(current_user, organization_id, user_id):
    """Remove a user from the organization (admin only)"""
    # Check if user is an admin
    supabase_mgr = SupabaseManager()
    user_role = supabase_mgr.get_user_role(current_user)

    if user_role != 'admin':
        logger.warning(f"Non-admin user {current_user} attempted to access admin endpoint")
        return jsonify({"error": "Admin access required"}), 403

    # Prevent users from removing themselves
    if user_id == current_user:
        return jsonify({"error": "Cannot remove yourself"}), 400

    try:
        # Ensure user belongs to the same organization
        user_details = supabase_mgr.get_user_details(user_id)
        if user_details.get('organization_id') != organization_id:
            return jsonify({"error": "User not in your organization"}), 403

        # Remove the user
        success = supabase_mgr.remove_user_from_organization(user_id)
        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to remove user"}), 500
    except Exception as e:
        logger.error(f"Error removing user: {str(e)}")
        return jsonify({"error": str(e)}), 500


# Organization management routes
@app.route("/api/admin/organization", methods=["GET"])
@token_required
def get_organization(current_user, organization_id):
    """Get organization details"""
    try:
        supabase_mgr = SupabaseManager()
        org_details = supabase_mgr.get_organization_details(organization_id)
        return jsonify({"organization": org_details})
    except Exception as e:
        logger.error(f"Error getting organization details: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/organization", methods=["PUT"])
@token_required
def update_organization(current_user, organization_id):
    """Update organization details (admin only)"""
    # Check if user is an admin
    supabase_mgr = SupabaseManager()
    user_role = supabase_mgr.get_user_role(current_user)

    if user_role != 'admin':
        logger.warning(f"Non-admin user {current_user} attempted to access admin endpoint")
        return jsonify({"error": "Admin access required"}), 403

    # Get update data
    data = request.get_json()
    if not data:
        return jsonify({"error": "No update data provided"}), 400

    try:
        success = supabase_mgr.update_organization(organization_id, data)
        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to update organization"}), 500
    except Exception as e:
        logger.error(f"Error updating organization: {str(e)}")
        return jsonify({"error": str(e)}), 500


# Add this new endpoint for data preview
@app.route("/api/preview", methods=["GET"])
@token_required
def get_data_preview(current_user, organization_id):
    """Get a preview of data without storing it"""
    connection_string = request.args.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
    table_name = request.args.get("table")

    if not connection_string or not table_name:
        return jsonify({"error": "Connection string and table name are required"}), 400

    # Get preview settings
    supabase_mgr = SupabaseManager()
    org_settings = supabase_mgr.get_organization_settings(organization_id)
    preview_settings = org_settings.get("preview_settings", {})

    # Check if previews are enabled
    if not preview_settings.get("enable_previews", True):
        return jsonify({"error": "Data previews are disabled for your organization"}), 403

    # Get maximum allowed rows (system limit and org-specific limit)
    system_max_rows = int(os.getenv("MAX_PREVIEW_ROWS", 50))
    org_max_rows = int(preview_settings.get("max_preview_rows", system_max_rows))
    max_rows = min(
        int(request.args.get("max_rows", org_max_rows)),
        org_max_rows,
        system_max_rows
    )

    # Get restricted columns for this table
    restricted_columns = preview_settings.get("restricted_preview_columns", {}).get(table_name, [])

    try:
        # Create engine
        engine = create_engine(connection_string)
        inspector = inspect(engine)

        # Get table columns
        table_columns = [col['name'] for col in inspector.get_columns(table_name)]

        # Filter restricted columns
        allowed_columns = [col for col in table_columns if col not in restricted_columns]

        if not allowed_columns:
            return jsonify({"error": "No viewable columns available for this table"}), 403

        # Log access (without storing the actual data)
        sanitized_conn = supabase_mgr._sanitize_connection_string(connection_string)
        supabase_mgr.log_preview_access(current_user, organization_id, table_name, sanitized_conn)

        # Construct and execute the query
        query = f"SELECT {', '.join(allowed_columns)} FROM {table_name} LIMIT {max_rows}"

        with engine.connect() as conn:
            result = conn.execute(text(query))
            preview_data = [dict(zip(result.keys(), row)) for row in result.fetchall()]

        logger.info(f"Preview data fetched for {table_name}, returned {len(preview_data)} rows")

        # Return the data directly (not stored)
        return jsonify({
            "preview_data": preview_data,
            "row_count": len(preview_data),
            "preview_max": max_rows,
            "restricted_columns": restricted_columns if restricted_columns else [],
            "all_columns": table_columns
        })

    except Exception as e:
        logger.error(f"Error generating data preview: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": f"Failed to generate preview: {str(e)}"}), 500


# Connection management routes
@app.route("/api/connections", methods=["GET"])
@token_required
def get_connections(current_user, organization_id):
    """Get all connections for the organization"""
    try:
        # Query connections for this organization
        supabase_mgr = SupabaseManager()

        # Use the Supabase client to query the database_connections table
        connections_response = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("organization_id", organization_id) \
            .order("created_at") \
            .execute()

        connections = connections_response.data if connections_response.data else []

        # For security, remove passwords from the response
        for conn in connections:
            if 'connection_details' in conn and 'password' in conn['connection_details']:
                conn['connection_details'].pop('password', None)

        return jsonify({"connections": connections})
    except Exception as e:
        logger.error(f"Error fetching connections: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections", methods=["POST"])
@token_required
def create_connection(current_user, organization_id):
    """Create a new database connection"""
    data = request.get_json()

    # Validate input
    required_fields = ["name", "connection_type", "connection_details"]
    for field in required_fields:
        if field not in data:
            return jsonify({"error": f"Missing required field: {field}"}), 400

    try:
        # Check if this is the first connection - if so, make it the default
        is_default = False
        supabase_mgr = SupabaseManager()

        # Count existing connections
        count_response = supabase_mgr.supabase.table("database_connections") \
            .select("id", count="exact") \
            .eq("organization_id", organization_id) \
            .execute()

        # If no connections exist, make this one the default
        if count_response.count == 0:
            is_default = True

        # Insert the new connection
        connection_data = {
            "organization_id": organization_id,
            "created_by": current_user,
            "name": data["name"],
            "connection_type": data["connection_type"],
            "connection_details": data["connection_details"],
            "is_default": is_default
        }

        response = supabase_mgr.supabase.table("database_connections") \
            .insert(connection_data) \
            .execute()

        # If successful, return the new connection
        if response.data and len(response.data) > 0:
            new_connection = response.data[0]

            # For security, remove password from the response
            if 'connection_details' in new_connection and 'password' in new_connection['connection_details']:
                new_connection['connection_details'].pop('password', None)

            return jsonify({"connection": new_connection})
        else:
            return jsonify({"error": "Failed to create connection"}), 500

    except Exception as e:
        logger.error(f"Error creating connection: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>", methods=["PUT"])
@token_required
def update_connection(current_user, organization_id, connection_id):
    """Update an existing database connection"""
    data = request.get_json()

    # Validate input
    required_fields = ["name", "connection_type", "connection_details"]
    for field in required_fields:
        if field not in data:
            return jsonify({"error": f"Missing required field: {field}"}), 400

    try:
        supabase_mgr = SupabaseManager()

        # First check if the connection exists and belongs to this organization
        get_response = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not get_response.data or len(get_response.data) == 0:
            return jsonify({"error": "Connection not found or you don't have permission to update it"}), 404

        # Handle password updates specially - don't update password if not provided
        existing_connection = get_response.data[0]
        update_data = {
            "name": data["name"],
            "connection_type": data["connection_type"],
            "connection_details": data["connection_details"],
            "updated_at": datetime.datetime.now().isoformat()
        }

        # If password is empty and we're updating, keep the existing password
        if not data["connection_details"].get("password") and \
                "password" in existing_connection["connection_details"]:
            update_data["connection_details"]["password"] = existing_connection["connection_details"]["password"]

        # Update the connection
        response = supabase_mgr.supabase.table("database_connections") \
            .update(update_data) \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        # If successful, return the updated connection
        if response.data and len(response.data) > 0:
            updated_connection = response.data[0]

            # For security, remove password from the response
            if 'connection_details' in updated_connection and 'password' in updated_connection['connection_details']:
                updated_connection['connection_details'].pop('password', None)

            return jsonify({"connection": updated_connection})
        else:
            return jsonify({"error": "Failed to update connection"}), 500

    except Exception as e:
        logger.error(f"Error updating connection: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>", methods=["DELETE"])
@token_required
def delete_connection(current_user, organization_id, connection_id):
    """Delete a database connection"""
    try:
        supabase_mgr = SupabaseManager()

        # First check if the connection exists and belongs to this organization
        get_response = supabase_mgr.supabase.table("database_connections") \
            .select("is_default") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not get_response.data or len(get_response.data) == 0:
            return jsonify({"error": "Connection not found or you don't have permission to delete it"}), 404

        # Don't allow deleting the default connection
        if get_response.data[0].get("is_default", False):
            return jsonify(
                {"error": "Cannot delete the default connection. Make another connection the default first."}), 400

        # Delete the connection
        delete_response = supabase_mgr.supabase.table("database_connections") \
            .delete() \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        # Check if any rows were deleted
        if delete_response.data and len(delete_response.data) > 0:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to delete connection"}), 500

    except Exception as e:
        logger.error(f"Error deleting connection: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/default", methods=["PUT"])
@token_required
def set_default_connection(current_user, organization_id, connection_id):
    """Set a connection as the default"""
    try:
        supabase_mgr = SupabaseManager()

        # First check if the connection exists and belongs to this organization
        get_response = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not get_response.data or len(get_response.data) == 0:
            return jsonify({"error": "Connection not found or you don't have permission to update it"}), 404

        # First, set all connections for this organization to not default
        update_all_response = supabase_mgr.supabase.table("database_connections") \
            .update({"is_default": False}) \
            .eq("organization_id", organization_id) \
            .execute()

        # Then set this connection as default
        update_response = supabase_mgr.supabase.table("database_connections") \
            .update({"is_default": True}) \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        # If successful, return success
        if update_response.data and len(update_response.data) > 0:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to set default connection"}), 500

    except Exception as e:
        logger.error(f"Error setting default connection: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/test", methods=["POST"])
@token_required
def test_connection(current_user, organization_id):
    """Test a database connection without saving it"""
    data = request.get_json()

    # Validate input
    required_fields = ["connection_type", "connection_details"]
    for field in required_fields:
        if field not in data:
            return jsonify({"error": f"Missing required field: {field}"}), 400

    # Build connection string based on connection type
    connection_string = ""
    connection_type = data["connection_type"]
    details = data["connection_details"]

    try:
        if connection_type == "snowflake":
            if details.get("useEnvVars", False):
                # Use environment variables
                prefix = details.get("envVarPrefix", "SNOWFLAKE")
                # Create connection string using environment variables
                from core.utils.connection_utils import get_snowflake_connection_from_env
                connection_string = get_snowflake_connection_from_env(prefix)

                if not connection_string:
                    return jsonify({
                        "error": f"Required environment variables for {prefix} connection not found"
                    }), 400
            else:
                # Create direct connection string
                try:
                    username = details.get("username", "")
                    password = details.get("password", "")
                    account = details.get("account", "")
                    database = details.get("database", "")
                    schema = details.get("schema", "PUBLIC")
                    warehouse = details.get("warehouse", "")

                    # Validate required fields
                    if not all([username, password, account, database, warehouse]):
                        return jsonify({
                            "error": "Missing required Snowflake connection parameters"
                        }), 400

                    # URL encode password to handle special characters
                    import urllib.parse
                    encoded_password = urllib.parse.quote_plus(password)

                    # Build connection string
                    connection_string = f"snowflake://{username}:{encoded_password}@{account}/{database}/{schema}?warehouse={warehouse}"
                except Exception as e:
                    return jsonify({
                        "error": f"Error building Snowflake connection string: {str(e)}"
                    }), 400

        elif connection_type == "duckdb":
            path = details.get("path", "")
            if not path:
                return jsonify({"error": "Missing required DuckDB path"}), 400

            connection_string = f"duckdb:///{path}"

        elif connection_type == "postgresql":
            try:
                username = details.get("username", "")
                password = details.get("password", "")
                host = details.get("host", "")
                port = details.get("port", "5432")
                database = details.get("database", "")

                # Validate required fields
                if not all([username, password, host, database]):
                    return jsonify({
                        "error": "Missing required PostgreSQL connection parameters"
                    }), 400

                # URL encode password to handle special characters
                import urllib.parse
                encoded_password = urllib.parse.quote_plus(password)

                # Build connection string
                connection_string = f"postgresql://{username}:{encoded_password}@{host}:{port}/{database}"
            except Exception as e:
                return jsonify({
                    "error": f"Error building PostgreSQL connection string: {str(e)}"
                }), 400

        else:
            return jsonify({"error": f"Unsupported connection type: {connection_type}"}), 400

        # Now test the connection by trying to connect to the database
        try:
            # Create SQLAlchemy engine
            engine = create_engine(connection_string)

            # Try to connect and get database info
            with engine.connect() as conn:
                # Get different information based on database type
                if connection_type == "snowflake":
                    # Get Snowflake info
                    result = conn.execute(text("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()"))
                    row = result.fetchone()

                    return jsonify({
                        "message": "Connection successful!",
                        "details": {
                            "user": row[0],
                            "role": row[1],
                            "database": row[2],
                            "schema": row[3],
                            "warehouse": row[4]
                        }
                    })

                elif connection_type == "duckdb":
                    # Get DuckDB info - simple version check
                    result = conn.execute(text("SELECT sqlite_version()"))
                    row = result.fetchone()

                    # Get table count
                    tables_result = conn.execute(text("SELECT COUNT(*) FROM sqlite_master WHERE type='table'"))
                    tables_row = tables_result.fetchone()

                    return jsonify({
                        "message": "Connection successful!",
                        "details": {
                            "version": row[0],
                            "table_count": tables_row[0]
                        }
                    })

                elif connection_type == "postgresql":
                    # Get PostgreSQL info
                    result = conn.execute(text("SELECT current_user, current_database(), version()"))
                    row = result.fetchone()

                    return jsonify({
                        "message": "Connection successful!",
                        "details": {
                            "user": row[0],
                            "database": row[1],
                            "version": row[2]
                        }
                    })

        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": f"Connection failed: {str(e)}"}), 400

    except Exception as e:
        logger.error(f"Error testing connection: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route('/api/connections/<connection_id>/credentials', methods=['GET'])
def get_connection_credentials(connection_id):
    """Get decrypted credentials for a connection"""
    try:
        # Get current user from auth token
        current_user = get_current_user()
        if not current_user:
            return jsonify({"error": "Authentication required"}), 401

        # Get connection details from Supabase
        supabase_mgr = SupabaseManager()
        connection = supabase_mgr.get_connection(connection_id)

        if not connection:
            return jsonify({"error": "Connection not found"}), 404

        # Return decrypted credentials
        return jsonify(connection.get("connection_details", {}))

    except Exception as e:
        logger.error(f"Error getting connection credentials: {str(e)}")
        logger.error(traceback.format_exc())  # Log the full stack trace
        return jsonify({"error": "Failed to get connection credentials"}), 500


@app.route("/api/profile-history", methods=["GET"])
@token_required
def get_profile_history(current_user, organization_id):
    """Get history of profile runs for a table"""
    table_name = request.args.get("table")
    limit = request.args.get("limit", 10, type=int)

    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    try:
        logger.info(f"Getting profile history for organization: {organization_id}, table: {table_name}, limit: {limit}")

        # Create profile history manager
        profile_history = SupabaseProfileHistoryManager()

        # Get history data
        history_data = profile_history.get_profile_history(organization_id, table_name, limit)

        logger.info(f"Retrieved {len(history_data)} profile history records")
        return jsonify({"history": history_data})
    except Exception as e:
        logger.error(f"Error getting profile history: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/validation-history/<profile_id>", methods=["GET"])
@token_required
def get_validation_history_by_profile(current_user, organization_id, profile_id):
    """Get validation results for a specific profile run"""
    try:
        logger.info(f"Fetching validation history for profile ID: {profile_id}")

        # Get direct Supabase client credentials
        import os
        from supabase import create_client

        # Get credentials from environment
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

        if not supabase_url or not supabase_key:
            logger.error("Missing Supabase configuration")
            return jsonify({"error": "Server configuration error"}), 500

        # Create direct client
        direct_client = create_client(supabase_url, supabase_key)

        # Query validation results linked to this profile
        response = direct_client.table("validation_results") \
            .select("*, validation_rules(*)") \
            .eq("organization_id", organization_id) \
            .eq("profile_history_id", profile_id) \
            .execute()

        if not response.data:
            logger.info(f"No validation results found for profile ID: {profile_id}")
            return jsonify({"results": []})

        logger.info(f"Found {len(response.data)} validation results for profile ID: {profile_id}")
        return jsonify({"results": response.data})
    except Exception as e:
        logger.error(f"Error getting validation history: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

# @app.before_request
# def log_memory_before():
#     memory_usage = psutil.virtual_memory()
#     logger.info(f"Before Request - Memory Usage: {memory_usage.percent}% used")
#
# @app.after_request
# def log_memory_after(response):
#     memory_usage = psutil.virtual_memory()
#     logger.info(f"After Request - Memory Usage: {memory_usage.percent}% used")
#     return response

if __name__ == "__main__":
    # In production, ensure you run with HTTPS (via a reverse proxy or WSGI server with SSL configured)
    app.run(debug=True)