import pytest
import json
from unittest.mock import patch, MagicMock
import sys
import os

# Fix import paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

# Import app and the specific function we want to test
from backend.app import app as flask_app, schedule_metadata_task


def test_schedule_metadata_task_direct():
    """Test the function directly with application context"""
    # Mock dependencies
    mock_task_manager = MagicMock()
    mock_task_manager.submit_collection_task.return_value = "task-123"

    mock_supabase = MagicMock()
    mock_supabase.supabase.table().select().eq().eq().execute.return_value = MagicMock(
        data=[{"id": "conn-123"}]
    )

    # Mock Flask request
    mock_request = MagicMock()
    mock_request.get_json.return_value = {
        "task_type": "full_collection",
        "priority": "high"
    }

    with flask_app.app_context():  # Create application context
        with patch('backend.app.metadata_task_manager', mock_task_manager), \
                patch('backend.app.SupabaseManager', return_value=mock_supabase), \
                patch('backend.app.request', mock_request), \
                patch('backend.app.token_required', lambda f: f):  # Bypass the decorator completely

            # Call the function directly
            result = schedule_metadata_task("fake-user", "fake-org", "conn-123")

            # Check result
            assert result[0].status_code == 200  # Access the response tuple's first element
            data = json.loads(result[0].get_data(as_text=True))
            assert data["task_id"] == "task-123"