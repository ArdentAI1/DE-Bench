# conftest.py
import sys
import os
import pytest
import json
from datetime import datetime
from multiprocessing import Manager
from dotenv import load_dotenv


# Initialize a manager for a thread-safe list to store test results
manager = Manager()
test_results = manager.list()

def pytest_configure(config):
    print("Configuring pytest...")

    # Load environment variables from the .env file
    load_dotenv()

    # Set the current working directory to the root of the project
    try:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if not os.path.exists(project_root):
            raise FileNotFoundError(f"Project root path does not exist: {project_root}")
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
    except Exception as e:
        raise Exception(f"Error setting project root path: {str(e)}")

    # Initialize the model
    from model.Initialize_Model import initialize_model

    initialize_model()

def pytest_runtest_logreport(report):
    #print("Report")
    #print(report)
    if report.when == 'call':
        test_result = {
            'nodeid': report.nodeid,
            'outcome': report.outcome,
            'duration': report.duration,
            'longrepr': str(report.longrepr) if report.failed else None
        }
        test_results.append(test_result)

def pytest_sessionfinish(session, exitstatus):
    from Configs.ArdentConfig import Ardent_Client

    # Only the main process should aggregate and display results
    if os.environ.get("PYTEST_XDIST_WORKER") is None:
        total = len(test_results)
        passed = sum(1 for result in test_results if result['outcome'] == 'passed')
        failed = sum(1 for result in test_results if result['outcome'] == 'failed')
        skipped = sum(1 for result in test_results if result['outcome'] == 'skipped')
        total_duration = sum(result['duration'] for result in test_results)

        #print("\nTest Session Summary:")
        #print(f"Total tests: {total}")
        #print(f"Passed: {passed}")
        #print(f"Failed: {failed}")
        #print(f"Skipped: {skipped}")
        #print(f"Total duration: {total_duration:.2f} seconds")

        #print("\nDetailed Test Results:")


        results_json = {
            "session_id": Ardent_Client.session_id,
            "test_results": list(test_results)
        }

        for result in test_results:
            #print("Result Object")
            #print(result)


            status = result['outcome'].capitalize()
            #print(f"{status}: {result['nodeid']} ({result['duration']:.2f} seconds)")
            #if result['longrepr']:
            #    print(f"  Failure Reason: {result['longrepr']}")

        # Optionally, save detailed results to a JSON file
        with open("Results/Test_Results.json", "w") as f:
            json.dump(results_json, f, indent=4)
