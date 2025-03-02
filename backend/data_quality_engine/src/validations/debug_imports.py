import os
import sys

# Print current directory
print(f"Current directory: {os.getcwd()}")

# Print the directory of this script
print(f"Script directory: {os.path.dirname(os.path.abspath(__file__))}")

# List the directory structure to find core/storage
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..'))
print(f"Project root: {project_root}")

backend_dir = os.path.join(project_root, 'backend')
print(f"Backend directory: {backend_dir}")

core_dir = os.path.join(backend_dir, 'core')
print(f"Core directory exists: {os.path.exists(core_dir)}")

if os.path.exists(core_dir):
    storage_dir = os.path.join(core_dir, 'storage')
    print(f"Storage directory exists: {os.path.exists(storage_dir)}")

    if os.path.exists(storage_dir):
        print("Files in storage directory:")
        for file in os.listdir(storage_dir):
            print(f"  - {file}")

# Try to locate the supabase_manager.py file
for root, dirs, files in os.walk(backend_dir):
    if 'supabase_manager.py' in files:
        print(f"Found supabase_manager.py at: {root}")