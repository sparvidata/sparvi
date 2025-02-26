import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
import sqlite3


class ProfileHistoryManager:
    """Manages the storage and retrieval of historical profiling data"""

    def __init__(self, storage_path="./history.db"):
        """Initialize the history manager with a SQLite database path"""
        self.storage_path = storage_path
        self._init_database()

    def _init_database(self):
        """Initialize the SQLite database schema if it doesn't exist"""
        conn = sqlite3.connect(self.storage_path)
        cursor = conn.cursor()

        # Create tables if they don't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS profile_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            connection_string TEXT,
            table_name TEXT,
            timestamp TEXT,
            row_count INTEGER,
            duplicate_count INTEGER
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS column_stats (
            run_id INTEGER,
            column_name TEXT,
            null_count INTEGER,
            null_percentage REAL,
            distinct_count INTEGER,
            FOREIGN KEY (run_id) REFERENCES profile_runs(id)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS numeric_stats (
            run_id INTEGER,
            column_name TEXT,
            min_value REAL,
            max_value REAL,
            avg_value REAL,
            median_value REAL,
            FOREIGN KEY (run_id) REFERENCES profile_runs(id)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS anomalies (
            run_id INTEGER,
            type TEXT,
            column_name TEXT,
            description TEXT,
            severity TEXT,
            FOREIGN KEY (run_id) REFERENCES profile_runs(id)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS schema_shifts (
            run_id INTEGER,
            type TEXT,
            column_name TEXT,
            description TEXT,
            detected_at TEXT,
            FOREIGN KEY (run_id) REFERENCES profile_runs(id)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS validation_results (
            run_id INTEGER,
            rule_name TEXT,
            is_valid INTEGER,
            actual_value TEXT,
            expected_value TEXT,
            FOREIGN KEY (run_id) REFERENCES profile_runs(id)
        )
        ''')

        # Create indices for faster querying
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_profile_runs_table ON profile_runs(table_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_column_stats_run ON column_stats(run_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_numeric_stats_run ON numeric_stats(run_id)')

        conn.commit()
        conn.close()

    def save_profile(self, profile: Dict, connection_string: str) -> int:
        """
        Save a profile to the history database
        Returns the run_id of the saved profile
        """
        conn = sqlite3.connect(self.storage_path)
        cursor = conn.cursor()

        # Insert the main profile run
        cursor.execute(
            'INSERT INTO profile_runs (connection_string, table_name, timestamp, row_count, duplicate_count) VALUES (?, ?, ?, ?, ?)',
            (
                connection_string,
                profile['table'],
                profile['timestamp'],
                profile['row_count'],
                profile.get('duplicate_count', 0)
            )
        )
        run_id = cursor.lastrowid

        # Insert column stats
        for col_name, stats in profile['completeness'].items():
            cursor.execute(
                'INSERT INTO column_stats (run_id, column_name, null_count, null_percentage, distinct_count) VALUES (?, ?, ?, ?, ?)',
                (
                    run_id,
                    col_name,
                    stats['nulls'],
                    stats['null_percentage'],
                    stats['distinct_count']
                )
            )

        # Insert numeric stats
        for col_name, stats in profile.get('numeric_stats', {}).items():
            cursor.execute(
                'INSERT INTO numeric_stats (run_id, column_name, min_value, max_value, avg_value, median_value) VALUES (?, ?, ?, ?, ?, ?)',
                (
                    run_id,
                    col_name,
                    stats['min'],
                    stats['max'],
                    stats['avg'],
                    stats.get('median', None)
                )
            )

        # Insert anomalies
        for anomaly in profile.get('anomalies', []):
            cursor.execute(
                'INSERT INTO anomalies (run_id, type, column_name, description, severity) VALUES (?, ?, ?, ?, ?)',
                (
                    run_id,
                    anomaly['type'],
                    anomaly.get('column', ''),
                    anomaly['description'],
                    anomaly['severity']
                )
            )

        # Insert schema shifts
        for shift in profile.get('schema_shifts', []):
            cursor.execute(
                'INSERT INTO schema_shifts (run_id, type, column_name, description, detected_at) VALUES (?, ?, ?, ?, ?)',
                (
                    run_id,
                    shift['type'],
                    shift.get('column', ''),
                    shift['description'],
                    profile['timestamp']
                )
            )

        # Insert validation results
        for result in profile.get('validation_results', []):
            cursor.execute(
                'INSERT INTO validation_results (run_id, rule_name, is_valid, actual_value, expected_value) VALUES (?, ?, ?, ?, ?)',
                (
                    run_id,
                    result['rule_name'],
                    1 if result['is_valid'] else 0,
                    str(result['actual_value']),
                    str(result['expected_value'])
                )
            )

        conn.commit()
        conn.close()

        return run_id

    def get_latest_profile(self, table_name: str) -> Optional[Dict]:
        """Retrieve the latest profile for a given table"""
        conn = sqlite3.connect(self.storage_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get the most recent run
        cursor.execute(
            'SELECT * FROM profile_runs WHERE table_name = ? ORDER BY id DESC LIMIT 1',
            (table_name,)
        )
        run_row = cursor.fetchone()

        if not run_row:
            conn.close()
            return None

        run = dict(run_row)
        run_id = run['id']

        # Get column stats
        cursor.execute('SELECT * FROM column_stats WHERE run_id = ?', (run_id,))
        col_stats = {row['column_name']: dict(row) for row in cursor.fetchall()}

        # Get numeric stats
        cursor.execute('SELECT * FROM numeric_stats WHERE run_id = ?', (run_id,))
        num_stats = {row['column_name']: dict(row) for row in cursor.fetchall()}

        # Get anomalies
        cursor.execute('SELECT * FROM anomalies WHERE run_id = ?', (run_id,))
        anomalies = [dict(row) for row in cursor.fetchall()]

        # Get schema shifts
        cursor.execute('SELECT * FROM schema_shifts WHERE run_id = ?', (run_id,))
        schema_shifts = [dict(row) for row in cursor.fetchall()]

        # Get validation results
        cursor.execute('SELECT * FROM validation_results WHERE run_id = ?', (run_id,))
        validation_results = [dict(row) for row in cursor.fetchall()]

        conn.close()

        # Reconstruct the profile format
        return {
            'run_id': run_id,
            'table': run['table_name'],
            'timestamp': run['timestamp'],
            'row_count': run['row_count'],
            'duplicate_count': run['duplicate_count'],
            'completeness': {
                name: {
                    'nulls': stats['null_count'],
                    'null_percentage': stats['null_percentage'],
                    'distinct_count': stats['distinct_count']
                } for name, stats in col_stats.items()
            },
            'numeric_stats': {
                name: {
                    'min': stats['min_value'],
                    'max': stats['max_value'],
                    'avg': stats['avg_value'],
                    'median': stats['median_value']
                } for name, stats in num_stats.items()
            },
            'anomalies': anomalies,
            'schema_shifts': schema_shifts,
            'validation_results': validation_results
        }

    def get_trends(self, table_name: str, num_periods: int = 10) -> Dict[str, Any]:
        """Get time-series trend data for a specific table"""
        conn = sqlite3.connect(self.storage_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get recent runs
        cursor.execute(
            'SELECT id, timestamp, row_count, duplicate_count FROM profile_runs WHERE table_name = ? ORDER BY id DESC LIMIT ?',
            (table_name, num_periods)
        )
        runs = [dict(row) for row in cursor.fetchall()]

        if not runs:
            conn.close()
            return {"error": "No historical data found"}

        # Prepare containers for trend data
        trends = {
            "timestamps": [run["timestamp"] for run in reversed(runs)],
            "row_counts": [run["row_count"] for run in reversed(runs)],
            "duplicate_counts": [run["duplicate_count"] for run in reversed(runs)],
            "null_rates": {},
            "validation_success_rates": []
        }

        # Get column names
        cursor.execute(
            'SELECT DISTINCT column_name FROM column_stats WHERE run_id IN ({})'.format(
                ','.join(['?'] * len(runs))
            ),
            [run["id"] for run in runs]
        )
        columns = [row["column_name"] for row in cursor.fetchall()]

        # Get null rates for each column over time
        for column in columns:
            null_rates = []
            for run in reversed(runs):
                cursor.execute(
                    'SELECT null_percentage FROM column_stats WHERE run_id = ? AND column_name = ?',
                    (run["id"], column)
                )
                row = cursor.fetchone()
                null_rates.append(row["null_percentage"] if row else None)

            trends["null_rates"][column] = null_rates

        # Get validation success rates
        for run in reversed(runs):
            cursor.execute(
                'SELECT COUNT(*) as total, SUM(is_valid) as valid FROM validation_results WHERE run_id = ?',
                (run["id"],)
            )
            row = cursor.fetchone()
            if row and row["total"] > 0:
                success_rate = (row["valid"] / row["total"]) * 100
            else:
                success_rate = None
            trends["validation_success_rates"].append(success_rate)

        conn.close()
        return trends

    def delete_old_profiles(self, table_name: str, keep_latest: int = 30):
        """Delete older profiles, keeping only the specified number of latest runs"""
        conn = sqlite3.connect(self.storage_path)
        cursor = conn.cursor()

        # Find IDs to delete
        cursor.execute(
            'SELECT id FROM profile_runs WHERE table_name = ? ORDER BY id DESC LIMIT -1 OFFSET ?',
            (table_name, keep_latest)
        )
        ids_to_delete = [row[0] for row in cursor.fetchall()]

        if not ids_to_delete:
            conn.close()
            return

        # Delete related data
        tables = ['column_stats', 'numeric_stats', 'anomalies', 'schema_shifts', 'validation_results']
        for table in tables:
            cursor.execute(
                f'DELETE FROM {table} WHERE run_id IN ({",".join(["?"] * len(ids_to_delete))})',
                ids_to_delete
            )

        # Delete the runs themselves
        cursor.execute(
            f'DELETE FROM profile_runs WHERE id IN ({",".join(["?"] * len(ids_to_delete))})',
            ids_to_delete
        )

        conn.commit()
        conn.close()