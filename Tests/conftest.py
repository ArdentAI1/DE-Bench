# conftest.py
import sys
import os
import pytest
import json
from datetime import datetime
from multiprocessing import Manager
from dotenv import load_dotenv
import sqlite3



try:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if not os.path.exists(project_root):
        raise FileNotFoundError(f"Project root path does not exist: {project_root}")
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
except Exception as e:
    raise Exception(f"Error setting project root path: {str(e)}")

# Import all fixtures from central hub
from Fixtures.base_resources import *

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
    from Environment.Airflow.Airflow import Airflow_Local


    os.makedirs(".tmp", exist_ok=True)

    # SQLite will create the database file automatically
    with sqlite3.connect(".tmp/resources.db") as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS resources (id INTEGER PRIMARY KEY AUTOINCREMENT, resource_id TEXT, type TEXT, creation_time REAL, worker_pid INTEGER, creation_duration REAL, description TEXT, status TEXT, custom_info TEXT)")

    #with open(".tmp/resources.json", "w") as f:
    #    json.dump([], f, indent=2)


    # set up the airflow docker container

    initialize_model()

    # initialize local airflow instance
    #airflow_local = Airflow_Local()

    #print("Initializing Airflow")
    # start the airflow docker container
    #airflow_local.Start_Airflow()


def pytest_runtest_logreport(report):
    if report.when == "call":
        # Initialize variables with default values
        model_runtime = None
        user_query = None
        test_steps = None
        
        # Get values from user_properties if they exist
        for name, value in report.user_properties:
            if name == "model_runtime":
                model_runtime = value
            elif name == "user_query":
                user_query = value
            elif name == "test_steps":
                test_steps = value

        test_result = {
            "nodeid": report.nodeid,
            "user_query": user_query,
            "outcome": report.outcome,
            "duration": report.duration,
            "model_runtime": model_runtime,
            "longrepr": str(report.longrepr) if report.failed else None,
            "test_steps": test_steps,
        }
        test_results.append(test_result)


def pytest_sessionfinish(session, exitstatus):
    from Configs.ArdentConfig import Ardent_Client
    from Environment.Airflow.Airflow import Airflow_Local
    from Fixtures.session_spindown import session_spindown
    import shutil


    if os.path.exists(".tmp"):
        print("TMP directory exists")


        # input("Waiting here")

        session_spindown()

        #input("Waiting here")

        if os.path.exists(".tmp"):
            shutil.rmtree(".tmp/")



        #now we want to check for information in there? on resources?


    #airflow_local = Airflow_Local()

    #airflow_local.Stop_Airflow()

    #airflow_local.Cleanup_Airflow_Directories()

    # Only the main process should aggregate and display results
    if os.environ.get("PYTEST_XDIST_WORKER") is None:
        total = len(test_results)
        passed = sum(1 for result in test_results if result["outcome"] == "passed")
        failed = sum(1 for result in test_results if result["outcome"] == "failed")
        skipped = sum(1 for result in test_results if result["outcome"] == "skipped")
        total_duration = sum(result["duration"] for result in test_results)

        # print("\nTest Session Summary:")
        # print(f"Total tests: {total}")
        # print(f"Passed: {passed}")
        # print(f"Failed: {failed}")
        # print(f"Skipped: {skipped}")
        # print(f"Total duration: {total_duration:.2f} seconds")

        # print("\nDetailed Test Results:")

        results_json = {
            "session_id": Ardent_Client.session_id,
            "test_results": list(test_results),
        }

        for result in test_results:
            # print("Result Object")
            # print(result)

            status = result["outcome"].capitalize()
            # print(f"{status}: {result['nodeid']} ({result['duration']:.2f} seconds)")
            # if result['longrepr']:
            #    print(f"  Failure Reason: {result['longrepr']}")

        # Optionally, save detailed results to a JSON file
        with open("Results/Test_Results.json", "w") as f:
            json.dump(results_json, f, indent=4)
