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
    from Environment.Airflow.Airflow import Airflow_Local

    # set up the airflow docker container

    initialize_model()
    # Convert config object to JSON and print
    
    try:
        config_dict = vars(config)
        #print(json.dumps(config_dict, indent=2, default=str))

        print(config_dict.get("args"))
    except Exception as e:
        print(f"Error converting config to JSON: {e}")

    # initialize local airflow instance
    #airflow_local = Airflow_Local()

    #print("Initializing Airflow")
    # start the airflow docker container
    #airflow_local.Start_Airflow()


def pytest_sessionstart(session):
    """This runs in the main process at the start of the session"""
    if os.environ.get("PYTEST_XDIST_WORKER") is None:
        with open("main_process_sessionstart.txt", "w") as f:
            f.write("==== MAIN PROCESS SESSION START ====\n")
            f.write(f"PYTEST_XDIST_WORKER: {os.environ.get('PYTEST_XDIST_WORKER')}\n")
            f.write("Session started in main process\n")
            f.write("==== END MAIN PROCESS SESSION START ====\n")


def pytest_collection_modifyitems(session, config, items):
    # This runs in workers with xdist
    with open(f"worker_{os.environ.get('PYTEST_XDIST_WORKER', 'unknown')}_compilation.txt", "w") as f:
        f.write(f"==== WORKER {os.environ.get('PYTEST_XDIST_WORKER', 'unknown')} COMPILATION ====\n")
        f.write(f"Worker has {len(items)} tests:\n")
        
        # Analyze markers for each test
        airflow_tests = [item for item in items if item.get_closest_marker('airflow')]
        databricks_tests = [item for item in items if item.get_closest_marker('databricks')]
        mysql_tests = [item for item in items if item.get_closest_marker('mysql')]
        mongodb_tests = [item for item in items if item.get_closest_marker('mongodb')]
        
        f.write(f"Airflow tests: {len(airflow_tests)}\n")
        f.write(f"Databricks tests: {len(databricks_tests)}\n")
        f.write(f"MySQL tests: {len(mysql_tests)}\n")
        f.write(f"MongoDB tests: {len(mongodb_tests)}\n\n")
        
        for item in items:
            markers = [mark.name for mark in item.iter_markers()]
            f.write(f"  - {item.nodeid} (markers: {markers})\n")
        
        # Resource compilation logic
        if airflow_tests:
            f.write(f"\n🚀 Need Airflow cluster for: {len(airflow_tests)} tests\n")
        if databricks_tests:
            f.write(f"🚀 Need Databricks cluster for: {len(databricks_tests)} tests\n")
        if mysql_tests:
            f.write(f"🚀 Need MySQL database for: {len(mysql_tests)} tests\n")
        if mongodb_tests:
            f.write(f"🚀 Need MongoDB database for: {len(mongodb_tests)} tests\n")
        
        f.write("==== END WORKER COMPILATION ====\n")


def pytest_collection_finish(session):
    """This runs AFTER collection is complete - better for xdist"""
    
    print(f"🔍 pytest_collection_finish() called!")
    print(f"   PYTEST_XDIST_WORKER: {os.environ.get('PYTEST_XDIST_WORKER')}")
    
    if os.environ.get("PYTEST_XDIST_WORKER") is None:
        print("🔍 COLLECTION FINISHED (MAIN PROCESS):")
        print(f"📊 Total tests in session: {len(session.items)}")
        
        # Analyze all tests in the session
        airflow_tests = [item for item in session.items if item.get_closest_marker('airflow')]
        print(f"   Airflow tests: {len(airflow_tests)}")
        
        for test in airflow_tests:
            print(f"     - {test.nodeid}")
        
        print("✅ Collection finish analysis complete!")
    else:
        print(f"🔍 WORKER {os.environ.get('PYTEST_XDIST_WORKER')} FINISHED")
        print(f"   Worker has {len(session.items)} items")


def pytest_runtest_logreport(report):
    if report.when == "call":
        # Get model_runtime from user_properties
        model_runtime = None
        for name, value in report.user_properties:
            if name == "model_runtime":
                model_runtime = value
            if name == "user_query":
                user_query = value
            if name == "test_steps":
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
