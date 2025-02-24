from flask import Flask, render_template, jsonify, request
import datetime
from data_quality_engine.src.profiler import profiler

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/profile", methods=["GET"])
def profile():
    # Get connection string and table name from query parameters (with defaults)
    connection_string = request.args.get(
        "connection_string",
        r"duckdb:///C:\Users\mhard\PycharmProjects\HawkDB\my_database.duckdb"
    )
    table_name = request.args.get("table", "employees")
    try:
        result = profiler.profile_table(connection_string, table_name)
        # Add a timestamp to the results
        result["timestamp"] = datetime.datetime.now().isoformat()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
