# test_e2e.py
import os

os.environ['TESTING'] = 'True'  # Set this before importing app

import pytest
import json
from unittest.mock import patch, MagicMock
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

# Import app
from backend.app import app as flask_app


@pytest.fixture
def auth_headers():
    """Fixture to create authentication headers"""
    return {
        'Authorization': 'Bearer test-token',
        'Content-Type': 'application/json'
    }


@pytest.fixture
def client():
    """Create a test client"""
    # Configure app for testing
    flask_app.config['TESTING'] = True

    # Mock the task manager
    with patch('backend.app.metadata_task_manager') as mock_task_manager:
        mock_task_manager.submit_collection_task.return_value = "test-task-id"
        mock_task_manager.get_task_status.return_value = {
            "task": {"id": "test-task-id", "status": "completed"},
            "result": {"tables_processed": 5}
        }

        # Set it on the app
        flask_app.metadata_task_manager = mock_task_manager

        # Return the test client
        with flask_app.test_client() as client:
            yield client


@pytest.fixture
def mock_supabase():
    """Mock the SupabaseManager"""
    with patch('backend.app.SupabaseManager') as mock_class:
        # Create mock instance with the necessary methods
        mock_instance = MagicMock()
        mock_instance.supabase.table().select().eq().eq().execute.return_value = MagicMock(
            data=[{"id": "test-conn-id"}]
        )
        mock_class.return_value = mock_instance

        yield mock_instance


def test_metadata_workflow(client, auth_headers, mock_supabase):
    """Test the complete metadata collection workflow"""
    # 1. Schedule a collection task
    response = client.post(
        '/api/connections/test-conn-id/metadata/tasks',
        json={
            "task_type": "full_collection",
            "priority": "high",
            "depth": "medium",
            "table_limit": 50
        },
        headers=auth_headers
    )

    print(f"Response data: {response.data}")
    assert response.status_code == 200, f"Expected status 200, got {response.status_code}"

    data = json.loads(response.data)
    assert "task_id" in data, f"Response missing task_id: {data}"
    task_id = data["task_id"]

    # 2. Check the task status
    response = client.get(
        f'/api/connections/test-conn-id/metadata/tasks/{task_id}',
        headers=auth_headers
    )

    assert response.status_code == 200
    status_data = json.loads(response.data)
    assert "task" in status_data
    assert status_data["task"]["status"] == "completed"