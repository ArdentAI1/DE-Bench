# conftest.py
import sys
import os
from dotenv import load_dotenv
import pytest

def pytest_configure(config):
    print("Configuring pytest...")

    load_dotenv()  # Load environment variables from the .env file


    #This allows absolute imports to work correctly by setting the current working directory to the root of the project
    #This fixes import errors and make sure the directory doesn't shift when pytest runs

    try:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if not os.path.exists(project_root):
            raise FileNotFoundError(f"Project root path does not exist: {project_root}")
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
    except Exception as e:
        raise Exception(f"Error setting project root path: {str(e)}")







def pytest_sessionfinish(session, exitstatus):


    

    print("Session finished")
