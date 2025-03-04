# backend/scripts/setup_db.py
from data_quality_engine.src.history.profile_history import ProfileHistoryManager

# Initialize database schema
history_manager = ProfileHistoryManager()
print("Database schema initialized successfully!")