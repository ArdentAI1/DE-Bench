import os
import signal
import sys
import time
import requests
import argparse
import re
import threading
import builtins
from typing import Dict, List, Any, Optional, Callable
from contextlib import contextmanager
import braintrust
from dotenv import load_dotenv
from model.Run_Model import run_model
from extract_test_configs import (
    extract_test_configuration,
    get_test_validator,
    discover_session_fixtures,
    setup_session_fixtures,
    cleanup_session_fixtures,
    setup_test_resources,
    cleanup_supabase_account_resource,
)
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
from braintrust import traced
from pydantic import BaseModel, validate_call
import traceback
from utils import map_func

# Note: set_up_model_configs and cleanup_model_artifacts are now used inside run_de_bench_task

# Load environment variables
load_dotenv(override=True)

# Thread-local storage for resource_id prefix
_thread_local = threading.local()

# Store the original print function
_original_print = builtins.print


def _prefixed_print(*args, **kwargs):
    """Custom print function that adds resource_id prefix if set in thread-local storage."""
    prefix = getattr(_thread_local, "resource_id_prefix", None)
    if prefix:
        # Convert all args to strings and join them
        message = " ".join(str(arg) for arg in args)
        # Only add prefix if the message doesn't already start with a bracket
        if not message.startswith("["):
            _original_print(f"[{prefix}]", *args, **kwargs)
        else:
            # Message already has a prefix, just print it
            _original_print(*args, **kwargs)
    else:
        # No prefix set, use original print
        _original_print(*args, **kwargs)


@contextmanager
def resource_id_context(resource_id_prefix: str):
    """Context manager that sets resource_id prefix for all print statements in this thread."""
    # Save the old prefix (if any)
    old_prefix = getattr(_thread_local, "resource_id_prefix", None)

    # Set the new prefix
    _thread_local.resource_id_prefix = resource_id_prefix

    # Temporarily replace the built-in print with our prefixed version
    builtins.print = _prefixed_print

    try:
        yield
    finally:
        # Restore the old prefix
        if old_prefix is None:
            if hasattr(_thread_local, "resource_id_prefix"):
                delattr(_thread_local, "resource_id_prefix")
        else:
            _thread_local.resource_id_prefix = old_prefix


# Global cleanup flag to prevent double cleanup
cleanup_already_run = False
active_session_fixtures = []
active_session_data = {}
active_tests_with_fixtures = []


@traced(name="teardown_test_fixtures")
def _teardown_test_fixtures(test_name, fixtures, test_resources=None):
    """
    Helper function to clean up test resources for a specific task.
    Note: Prints are automatically prefixed by resource_id_context.
    """
    try:
        if fixtures:
            print(
                f"🧹 Tearing down {len(fixtures)} fixtures (fixtures: {', '.join([f.get_resource_type() for f in fixtures])})"
            )

            for fixture in reversed(fixtures):
                try:
                    fixture._test_teardown()
                except Exception as e:
                    print(
                        f"⚠️ Error tearing down fixture: {fixture.get_resource_type()}: {e}\n{traceback.format_exc()}"
                    )
                    continue

                print(f"...✅ Tore down fixture: {fixture.get_resource_type()}")

        # Always clean up Supabase account separately (legacy resource)
        if test_resources and "supabase_account_resource" in test_resources:
            cleanup_supabase_account_resource(
                test_resources["supabase_account_resource"]
            )

        # Unregister test from global tracking after cleanup
        unregister_test_with_fixtures(test_name)

    except Exception as e:
        print(f"⚠️ Error tearing down fixtures: {e}, {traceback.format_exc()}")


def full_model_run(
    test_name,
    mode,
    test_resources,
    fixture_instances,
    model_configs,
    task_description,
    resource_id_prefix=None,
    **kwargs,
):
    """
    Step 3 and 4: Set up model configurations if needed and execute the model.
    Note: Prints are automatically prefixed by resource_id_context.
    """
    config_results = None
    custom_info = {"mode": mode}

    # 4. Execute the model (check skip_model_run FIRST to avoid unnecessary config setup)
    if kwargs.get("skip_model_run"):
        print(
            f"⚠️ Skipping model run because 'skip_model_run' was set and evaluated to True",
            flush=True,
        )
        model_result = None
        config_results = None
    else:
        # Only set up configs if we're actually running the model
        if mode == "Ardent" and "supabase_account_resource" in test_resources:
            print(f"🔧 Setting up model configs...")

            custom_info.update(
                {
                    "api_key": test_resources["supabase_account_resource"]["api_key"],
                }
            )

            config_results = set_up_model_configs(
                Configs=model_configs,
                custom_info=custom_info,
            )
            print(f"✅ Model configs set up")

        elif mode == "Claude_Code":
            print(f"🔧 Setting up Kubernetes for Claude Code...")

            # Set up Kubernetes infrastructure for Claude Code
            config_results = set_up_model_configs(
                Configs=model_configs,
                custom_info=custom_info,
            )

            # Add the Kubernetes objects to custom_info for the model
            if config_results:
                custom_info.update(config_results)

            print(f"✅ Kubernetes setup completed")

        elif mode == "OpenAI_Codex":
            print(f"🔧 Setting up Kubernetes for OpenAI Codex...")

            # Set up Kubernetes infrastructure for OpenAI Codex
            config_results = set_up_model_configs(
                Configs=model_configs,
                custom_info=custom_info,
            )

            # Add the Kubernetes objects to custom_info for the model
            if config_results:
                custom_info.update(config_results)

            print(f"✅ Kubernetes setup completed")
        else:
            config_results = None

        print(f"🤖 Running model...", flush=True)
        model_result = run_model(
            container=None,
            task=task_description,
            configs=model_configs,
            extra_information=custom_info,
        )
        print(f"✅ Model execution completed", flush=True)

    # Clean up model artifacts first (but keep test resources for validation)
    if config_results:
        if mode == "Ardent" and "supabase_account_resource" in test_resources:
            print(f"🧹 Cleaning up model artifacts...")
            cleanup_model_artifacts(
                Configs=model_configs,
                custom_info=custom_info,
            )
            print(f"✅ Model artifacts cleaned up")
        elif mode == "Claude_Code":
            print(f"🧹 Cleaning up Kubernetes resources...")
            cleanup_model_artifacts(
                Configs=model_configs,
                custom_info=custom_info,
            )
            print(f"✅ Kubernetes resources cleaned up")
        elif mode == "OpenAI_Codex":
            print(f"🧹 Cleaning up Kubernetes resources...")
            cleanup_model_artifacts(
                Configs=model_configs,
                custom_info=custom_info,
            )
            print(f"✅ Kubernetes resources cleaned up")

    return {
        "result": model_result,
        "fixtures": fixture_instances,
        "test_name": test_name,
        "test_resources": test_resources,
        "model_configs": model_configs,
        "custom_info": custom_info,
    }


def run_de_bench_task(test_input):
    """
    Convert DE-Bench test to Braintrust task function with per-test resource management.
    Each task execution is now self-contained with its own setup/teardown.
    """
    try:
        # Extract test configuration from input
        task_description = test_input["task"]
        mode = test_input.get("mode", "Ardent")
        test_name = test_input.get("test_name", "Unknown")
        instance_id = test_input.get("instance_id", test_name)
        instance_num = test_input.get("instance_num", 1)
        session_data = test_input.get("session_data", {})

        test_resources = {}
        fixture_instances = []

        # Will be set after modifying fixture configs
        resource_id_prefix = instance_id

        # 1. Extract test configuration and set up per-test resources
        test_data = extract_test_configuration(test_name)

        # Modify fixture configs to use instance_id for unique resource identifiers
        if "custom_fixtures" in test_data["resource_configs"]:
            for fixture in test_data["resource_configs"]["custom_fixtures"]:
                if hasattr(fixture, "custom_config") and fixture.custom_config:
                    # Update resource_id to include instance identifier
                    original_resource_id = fixture.custom_config.get("resource_id", "")
                    if instance_num > 1:
                        # Append instance number to make it unique
                        fixture.custom_config["resource_id"] = (
                            f"{original_resource_id}_inst_{instance_num}"
                        )
                        resource_id_prefix = fixture.custom_config["resource_id"]
                    else:
                        # For instance 1, use the original resource_id as prefix
                        resource_id_prefix = (
                            original_resource_id
                            if original_resource_id
                            else instance_id
                        )

        # Use the resource_id_context to automatically prefix ALL print statements
        with resource_id_context(resource_id_prefix):
            # Now all print statements in this context will be automatically prefixed
            if instance_num > 1:
                # Log the resource modifications
                for fixture in test_data["resource_configs"].get("custom_fixtures", []):
                    if hasattr(fixture, "custom_config") and fixture.custom_config:
                        if "resource_id" in fixture.custom_config:
                            print(
                                f"Updated resource_id: {original_resource_id} → {fixture.custom_config['resource_id']}"
                            )
                        if "ecs_namespace" in fixture.custom_config:
                            print(
                                f"Updated ecs_namespace: {fixture.custom_config['ecs_namespace']}"
                            )
                        if "kubernetes_namespace" in fixture.custom_config:
                            print(
                                f"Updated kubernetes_namespace: {fixture.custom_config['kubernetes_namespace']}"
                            )

            print(
                f"🚀 Starting self-contained test execution (instance {instance_num})",
                flush=True,
            )
            print(f"📋 Setting up resources...", flush=True)

            # Set up per-test resources (using shared session data if available)
            test_resources, fixture_instances = setup_test_resources(
                test_data["resource_configs"], session_data=session_data, mode=mode
            )
            print(f"✅ Resources set up", flush=True)

            # Pause for infrastructure inspection if requested
            if test_input.get("infrastructure_only"):
                # Build model inputs to show what the agent would see
                create_model_inputs_func = test_data["resource_configs"].get(
                    "create_model_inputs_func"
                )
                if create_model_inputs_func:
                    model_inputs_base = {
                        "test_name": test_name,
                        "mode": mode,
                        "test_resources": test_resources,
                        "fixture_instances": fixture_instances,
                        "task_description": task_description,
                        "skip_model_run": test_input.get("skip_model_run", False),
                    }
                    final_model_inputs = create_model_inputs_func(
                        model_inputs_base, fixture_instances
                    )

                    # Build custom_info (what would be passed to agent)
                    custom_info = {"mode": mode}
                    if (
                        mode == "Ardent"
                        and "supabase_account_resource" in test_resources
                    ):
                        custom_info.update(
                            {
                                "api_key": test_resources["supabase_account_resource"][
                                    "api_key"
                                ],
                            }
                        )

                    # Print all infrastructure details
                    print("\n" + "=" * 70)
                    print("📦 INFRASTRUCTURE DETAILS (What the agent would see)")
                    print("=" * 70)

                    # Define resource-specific field whitelists
                    AIRFLOW_KEEP = {
                        "base_url",
                        "api_url",
                        "username",
                        "password",
                        "provider",
                        "modal_app_name",
                    }
                    GITHUB_KEEP = {"branch_name", "access_token", "repo_url"}
                    POSTGRES_KEEP = {"host", "port", "user", "password"}
                    MYSQL_KEEP = {"host", "port", "user", "password"}
                    MONGO_KEEP = {"connection_string"}
                    SNOWFLAKE_KEEP = {
                        "account",
                        "user",
                        "password",
                        "warehouse",
                        "role",
                        "database",
                        "schema",
                    }

                    print("\n🔧 TEST RESOURCES:")
                    for resource_name, resource_data in test_resources.items():
                        if not isinstance(resource_data, dict):
                            continue

                        # Skip supabase entirely
                        if "supabase" in resource_name.lower():
                            continue

                        print(f"\n  📋 {resource_name}:")

                        # Airflow resource
                        if "airflow" in resource_name.lower():
                            for key in AIRFLOW_KEEP:
                                if key in resource_data and resource_data[key]:
                                    print(f"     {key}: {resource_data[key]}")

                        # GitHub resource
                        elif "github" in resource_name.lower():
                            for key in GITHUB_KEEP:
                                if key in resource_data:
                                    print(f"     {key}: {resource_data[key]}")

                        # PostgreSQL resource
                        elif "postgres" in resource_name.lower():
                            if "connection_params" in resource_data:
                                for key in POSTGRES_KEEP:
                                    if key in resource_data["connection_params"]:
                                        print(
                                            f"     {key}: {resource_data['connection_params'][key]}"
                                        )
                            if "created_resources" in resource_data:
                                databases = [
                                    db.get("name")
                                    for db in resource_data["created_resources"]
                                    if db.get("type") == "database"
                                ]
                                if databases:
                                    print(f"     databases: {', '.join(databases)}")

                        # MySQL resource
                        elif "mysql" in resource_name.lower():
                            # MySQL stores connection details differently, try both approaches
                            for key in MYSQL_KEEP:
                                if key in resource_data:
                                    print(f"     {key}: {resource_data[key]}")
                            if "created_resources" in resource_data:
                                databases = [
                                    db.get("name")
                                    for db in resource_data["created_resources"]
                                    if db.get("type") == "database"
                                ]
                                if databases:
                                    print(f"     databases: {', '.join(databases)}")

                        # MongoDB resource
                        elif "mongo" in resource_name.lower():
                            for key in MONGO_KEEP:
                                if key in resource_data:
                                    print(f"     {key}: {resource_data[key]}")
                            if "created_resources" in resource_data:
                                collections = [
                                    f"{r.get('db')}.{r.get('collection')}"
                                    for r in resource_data["created_resources"]
                                ]
                                if collections:
                                    print(f"     collections: {', '.join(collections)}")

                        # Snowflake resource
                        elif "snowflake" in resource_name.lower():
                            for key in SNOWFLAKE_KEEP:
                                if key in resource_data:
                                    print(f"     {key}: {resource_data[key]}")
                            if "created_resources" in resource_data:
                                for res in resource_data["created_resources"]:
                                    if res.get("tables"):
                                        print(
                                            f"     tables: {', '.join(res['tables'])}"
                                        )

                    print("\n🎯 MODEL CONFIGS:")
                    if "model_configs" in final_model_inputs:
                        for key, value in final_model_inputs["model_configs"].items():
                            print(f"   {key}: {value}")

                    print("\n📝 CUSTOM INFO (Agent context):")
                    for key, value in custom_info.items():
                        print(f"   {key}: {value}")

                    print("\n📖 TASK DESCRIPTION:")
                    print(
                        f"   {final_model_inputs.get('task_description', task_description)}"
                    )

                    print("\n" + "=" * 70)

                input("\n⏸️  Press Enter to tear down infrastructure...\n")

                # Return early to skip model run and go straight to teardown
                return {
                    "status": "infrastructure_only",
                    "message": "Infrastructure inspected, skipping model run",
                    "resources": test_resources,
                }

            # Register test with fixtures for global cleanup tracking
            if fixture_instances:
                register_test_with_fixtures(
                    test_name, fixture_instances, has_started=True
                )

            model_inputs_base = {
                "test_name": test_name,
                "mode": mode,
                "test_resources": test_resources,
                "fixture_instances": fixture_instances,
                "task_description": task_description,
                "skip_model_run": test_input.get("skip_model_run", False),
            }

            # 3. Modify inputs if needed
            create_model_inputs_func = test_data["resource_configs"].get(
                "create_model_inputs_func"
            )
            if create_model_inputs_func:
                final_full_model_run_args = create_model_inputs_func(
                    model_inputs_base, fixture_instances
                )
            else:
                raise ValueError(
                    f"❌ Test {test_name} is missing create_model_inputs_func function"
                )

            # Validate that model_configs and task_description are in the final_full_model_run_args
            if "model_configs" not in final_full_model_run_args:
                raise ValueError(
                    f"❌ Test {test_name} did not return model_configs from create_model_inputs_func"
                )
            if "task_description" not in final_full_model_run_args:
                raise ValueError(
                    f"❌ Test {test_name} did not return task_description from create_model_inputs_func"
                )

            # 3 & 4. Set up model configs and run model
            result = full_model_run(
                **final_full_model_run_args, resource_id_prefix=resource_id_prefix
            )

            # Note: Tear down doesn't happen here, it happens in the validator because we need to access the fixture instances
            return result

    except Exception as e:
        # Try to get resource_id_prefix if it was set
        prefix = (
            resource_id_prefix
            if "resource_id_prefix" in locals()
            else instance_id
            if "instance_id" in locals()
            else test_name
            if "test_name" in locals()
            else "Unknown"
        )

        # Use context for error messages too
        with resource_id_context(prefix):
            print(f"❌ Error in test execution: {e}", flush=True)
            print(f"   Traceback: {traceback.format_exc()}", flush=True)

            # Tear down test fixtures on error
            if fixture_instances:
                _teardown_test_fixtures(test_name, fixture_instances, test_resources)

        # Return a failure result instead of raising to prevent terminating other tests
        return {
            "result": {
                "status": "failed",
                "error": str(e),
                "traceback": traceback.format_exc(),
            },
            "fixtures": fixture_instances if "fixture_instances" in locals() else [],
            "test_name": test_name if "test_name" in locals() else "Unknown",
            "test_resources": test_resources if "test_resources" in locals() else {},
            "model_configs": {},
            "custom_info": {"mode": mode if "mode" in locals() else "Unknown"},
            "execution_error": True,
        }


def cleanup_handler() -> None:
    """Cleanup function that runs on exit or interrupt - preserves existing logic"""
    global \
        cleanup_already_run, \
        active_session_fixtures, \
        active_session_data, \
        active_tests_with_fixtures

    if cleanup_already_run:
        print("🔄 Cleanup already completed, skipping...")
        return

    cleanup_already_run = True

    try:
        # Teardown all test-level fixtures first
        if active_tests_with_fixtures:
            try:
                print("🧹 Tearing down all test-level fixtures...")
                teardown_all_fixtures(
                    TeardownAllFixturesArgs(all_active_tests=active_tests_with_fixtures)
                )
                print("✅ Test-level fixtures torn down")
                active_tests_with_fixtures.clear()
            except Exception as e:
                print(f"❌ Error tearing down test fixtures: {e}")

        # Note: Per-test resources are now cleaned up inside run_de_bench_task
        # Only session-level cleanup is needed here

        # Clean up session-level fixtures
        if active_session_fixtures:
            try:
                print("🧹 Cleaning up session-level fixtures...")
                cleanup_session_fixtures(active_session_fixtures, active_session_data)
                print("✅ Session-level fixtures cleaned up")
            except Exception as e:
                print(f"❌ Error cleaning up session fixtures: {e}")

        # Use existing session spindown logic
        from Fixtures.session_spindown import session_spindown

        session_spindown()
        print("✅ Session spindown completed")
    except Exception as e:
        print(f"❌ Error during session spindown: {e}")

    # Clean up temp directory (preserve existing logic)
    import shutil

    if os.path.exists(".tmp"):
        try:
            shutil.rmtree(".tmp/")
            print("✅ Temp directory cleaned up")
        except Exception as e:
            print(f"❌ Error cleaning temp directory: {e}")


class TestWithFixtures(BaseModel):
    """Test with session-level fixtures"""

    test_name: str
    fixtures: List[Any]
    has_started_initialization: bool


class TeardownAllFixturesArgs(BaseModel):
    """Teardown all session-level fixtures"""

    all_active_tests: List[TestWithFixtures] = []


def register_test_with_fixtures(
    test_name: str, fixtures: List[Any], has_started: bool = True
) -> None:
    """Register a test with its fixtures for global cleanup tracking."""
    global active_tests_with_fixtures

    test_with_fixtures = TestWithFixtures(
        test_name=test_name, fixtures=fixtures, has_started_initialization=has_started
    )
    active_tests_with_fixtures.append(test_with_fixtures)
    print(
        f"📝 Registered test {test_name} with {len(fixtures)} fixtures for cleanup tracking"
    )


def unregister_test_with_fixtures(test_name: str) -> None:
    """Remove a test from the global cleanup tracking after it's been cleaned up."""
    global active_tests_with_fixtures

    active_tests_with_fixtures = [
        test for test in active_tests_with_fixtures if test.test_name != test_name
    ]
    print(f"📝 Unregistered test {test_name} from cleanup tracking")


@traced(name="teardown_all_fixtures")
def teardown_all_fixtures(args: TeardownAllFixturesArgs) -> None:
    """Teardown all test-level fixtures, in the opposite order of their declaration in the test."""
    if args.all_active_tests:
        for test in args.all_active_tests:
            if test.has_started_initialization:
                print(f"🧹 Tearing down fixtures for test: {test.test_name}")
                for fixture in reversed(test.fixtures):
                    try:
                        if hasattr(fixture, "test_teardown"):
                            fixture.test_teardown()
                            print(
                                f"...✅ Tore down fixture: {fixture.get_resource_type()}"
                            )
                    except Exception as e:
                        print(f"⚠️ Error tearing down fixture for {test.test_name}: {e}")
                        continue


def signal_handler(signum: int, frame: Any) -> None:
    """Handle Ctrl+C (SIGINT) gracefully - preserves existing behavior"""
    print("\n🛑 Evaluation interrupted by user -- Running cleanup...")
    cleanup_handler()
    print("🔄 Cleanup completed. Exiting...")
    sys.exit(0)


def discover_available_tests(
    filter_patterns: Optional[List[str]] = None,
) -> Dict[str, List[str]]:
    """
    Dynamically discover available tests for Braintrust evaluation with optional filtering.

    Scans the Tests directory and finds all tests that follow the new pattern:
    - Have a test file with get_fixtures() and create_model_inputs() functions
    - Have a Test_Configs.py with User_Input

    Args:
        filter_patterns: List of regex patterns to filter test names

    Returns:
        Dict with 'all_tests' (all discovered valid tests) and 'filtered_tests' (tests matching filter patterns)
    """
    available_tests = []
    tests_dir = "Tests"

    if not os.path.exists(tests_dir):
        print(f"⚠️  Tests directory '{tests_dir}' not found")
        return {"all_tests": [], "filtered_tests": []}

    # Scan all directories in Tests/
    for item in os.listdir(tests_dir):
        test_dir_path = os.path.join(tests_dir, item)

        # Skip if not a directory or starts with . or __
        if (
            not os.path.isdir(test_dir_path)
            or item.startswith(".")
            or item.startswith("__")
        ):
            continue

        # Check if this test follows the new pattern
        if _is_valid_new_pattern_test(item):
            available_tests.append(item)
            print(f"✅ Discovered test: {item}")
        else:
            print(f"⚠️  Skipping {item} - doesn't follow new pattern or has errors")

    # Sort for consistent ordering
    available_tests.sort()

    # Apply filters if provided
    filtered_tests = available_tests.copy()  # Default to all tests
    if filter_patterns:
        filtered_tests = []
        for test_name in available_tests:
            if isinstance(filter_patterns, str):
                filter_patterns = [filter_patterns]
            for pattern in filter_patterns:
                try:
                    if re.search(pattern, test_name, re.IGNORECASE):
                        filtered_tests.append(test_name)
                        break  # Stop checking other patterns for this test
                except re.error as e:
                    print(f"⚠️  Invalid regex pattern '{pattern}': {e}")
                    continue

    return {"all_tests": available_tests, "filtered_tests": filtered_tests}


def _is_valid_new_pattern_test(test_name: str) -> bool:
    """
    Check if a test follows the new pattern with get_fixtures() and create_model_inputs().

    Args:
        test_name: Name of the test directory

    Returns:
        True if test follows new pattern, False otherwise
    """
    import importlib

    try:
        # Check if Test_Configs.py exists and has User_Input
        config_module_path = f"Tests.{test_name}.Test_Configs"
        config_module = importlib.import_module(config_module_path)

        if not hasattr(config_module, "User_Input"):
            return False

        # Find test files in the directory
        test_dir = f"Tests/{test_name}"
        test_files = []

        for file in os.listdir(test_dir):
            if file.startswith("test_") and file.endswith(".py"):
                test_files.append(file[:-3])  # Remove .py extension

        if not test_files:
            return False

        # Check the first test file for required functions
        test_module_path = f"Tests.{test_name}.{test_files[0]}"
        try:
            test_module = importlib.import_module(test_module_path)
        except Exception as e:
            print(
                f"Test '{test_name}' does not match pattern: failed to import '{test_module_path}': {e}"
            )
            return False

        # Must have get_fixtures function
        if not hasattr(test_module, "get_fixtures"):
            print(f"Test '{test_name}' does not match pattern: missing 'get_fixtures'")
            return False

        # Must have create_model_inputs function
        if not hasattr(test_module, "create_model_inputs"):
            print(
                f"Test '{test_name}' does not match pattern: missing 'create_model_inputs'"
            )
            return False

        # Must have validate_test function
        if not hasattr(test_module, "validate_test"):
            print(f"Test '{test_name}' does not match pattern: missing 'validate_test'")
            return False

        # Verify get_fixtures returns a list
        get_fixtures_func = getattr(test_module, "get_fixtures")
        if not callable(get_fixtures_func):
            print(
                f"Test '{test_name}' does not match pattern: 'get_fixtures' is not callable"
            )
            return False

        # All checks passed
        return True

    except Exception as e:
        # Any import errors or missing attributes mean it's not a valid test
        print(f"Test '{test_name}' does not match pattern: {e}")
        return False


def fetch_git_info() -> Dict[str, Any]:
    """
    Fetch git information from the Ardent API to use in experiment naming.

    Returns:
        Dict with git info (branch, error)
    """
    try:
        base_url = os.getenv("ARDENT_BASE_URL", "http://localhost:8080")
        response = requests.get(f"{base_url}/v1/system/git-info", timeout=5)

        if response.status_code == 200:
            git_info = response.json()
            branch = git_info.get("branch")
            print(f"📋 Git info: branch={branch if branch else 'N/A'}")
            return git_info
        else:
            print(f"⚠️  Failed to fetch git info: HTTP {response.status_code}")
            return {
                "branch": None,
                "error": f"HTTP {response.status_code}",
            }

    except Exception as e:
        print(f"⚠️  Could not fetch git info: {e}")
        return {
            "branch": None,
            "error": str(e),
        }


def construct_experiment_name(mode: str) -> str:
    """
    Construct a meaningful experiment name using git information and mode.

    Format: multi-test-{mode}-{branch}
    Fallback: multi-test-{mode}-{timestamp} if no git info available

    Args:
        mode: The execution mode (e.g., "Ardent", "Claude_Code")
        git_info: Git information from the Ardent API

    Returns:
        Experiment name string
    """
    # If ardent, we worry about git info
    if mode == "Ardent":
        # Fetch git info for experiment naming
        git_info = fetch_git_info()
        return f"Ardent/{git_info.get('branch')}"
    else:
        return f"{mode.lower()}"


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Run DE-Bench Braintrust evaluation with session-level fixture support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_braintrust_eval.py Ardent                    # Run all tests in Ardent mode
  python run_braintrust_eval.py Ardent Claude_Code OpenAI_Codex  # Run all tests in all modes
  python run_braintrust_eval.py --filter "MongoDB.*"     # Run only MongoDB tests
  python run_braintrust_eval.py --filter ".*Hello.*"     # Run only Hello World tests
  python run_braintrust_eval.py --filter "MongoDB.*" "MySQL.*" Ardent  # MongoDB & MySQL in Ardent mode
  python run_braintrust_eval.py --filter "MongoDB_Agent_Add_Record" OpenAI_Codex  # Single test with Codex
  python run_braintrust_eval.py --num-trials 3 Ardent    # Run all tests 3 times (Braintrust trials)
  python run_braintrust_eval.py -n 5 --filter ".*Hello.*" Claude_Code  # Run Hello tests 5 times with Claude
  python run_braintrust_eval.py --num-instances 10 --filter ".*Hello.*" Ardent  # Create 10 parallel instances
  python run_braintrust_eval.py -i 5 --filter ".*Hello.*" Ardent  # Create 5 parallel instances with unique IDs
  python run_braintrust_eval.py --full-concurrency Ardent  # Run all tests with max concurrency = number of tests
        """,
    )

    parser.add_argument(
        "modes",
        nargs="*",
        default=["Ardent"],
        help="Execution modes to run (e.g., Ardent, Claude_Code, OpenAI_Codex). Default: ['Ardent']",
    )

    parser.add_argument(
        "--filter",
        action="append",
        dest="filter_patterns",
        help="Filter test names using regex patterns. Can be used multiple times. Case-insensitive.",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output with additional debugging information",
    )

    parser.add_argument(
        "--skip-model-run",
        action="store_true",
        help="Skip model run for all tests, useful for debugging",
    )

    parser.add_argument(
        "--infrastructure-only",
        action="store_true",
        help="Set up infrastructure and pause for inspection before teardown",
    )

    parser.add_argument(
        "--num-trials",
        "-n",
        type=int,
        default=1,
        help="Number of trials to run for each test (default: 1). Braintrust will run each test this many times.",
    )

    parser.add_argument(
        "--num-instances",
        "-i",
        type=int,
        default=1,
        help="Number of parallel instances to create for each test (default: 1). Each instance gets unique identifiers and isolated resources.",
    )

    parser.add_argument(
        "--full-concurrency",
        action="store_true",
        help="Use the total number of tests as max_concurrency instead of default 20",
    )

    return parser.parse_args()


def run_multi_test_evaluation(
    modes: List[str] = ["Ardent"],
    test_names: Optional[List[str]] = None,
    all_valid_tests: Optional[List[str]] = None,
    verbose: bool = False,
    skip_model_run: bool = False,
    infrastructure_only: bool = False,
    trial_count: int = 1,
    num_instances: int = 1,
    full_concurrency: bool = False,
) -> Dict[str, Any]:
    """Run multiple tests as Braintrust evaluation for specified modes"""
    global active_session_fixtures, active_session_data, active_tests_with_fixtures

    # Set up signal handler for graceful cleanup
    signal.signal(signal.SIGINT, signal_handler)

    # Initialize model (preserve existing logic)
    from model.Initialize_Model import initialize_model

    initialize_model()

    # Discover available tests if not specified
    if test_names is None:
        test_discovery = discover_available_tests()
        test_names = test_discovery["filtered_tests"]
        if all_valid_tests is None:
            all_valid_tests = test_discovery["all_tests"]

    # Ensure all_valid_tests is set (fallback if not provided)
    if all_valid_tests is None:
        all_valid_tests = test_names

    if verbose:
        print(f"🔍 All valid tests discovered: {all_valid_tests}")
        print(f"🔍 Tests to run (after filtering): {test_names}")
        print(f"🔍 Number of instances per test: {num_instances}")

    if not test_names:
        print("❌ No tests found matching the filter criteria")
        return {}

    if num_instances > 1:
        print(f"🔢 Creating {num_instances} parallel instances for each test...")
        print(
            f"   Total test instances: {len(test_names)} tests × {num_instances} instances = {len(test_names) * num_instances} total"
        )

    print(f"🚀 Starting DE-Bench Braintrust evaluation for tests: {test_names}...")

    # Collect all test configurations and discover session fixtures
    all_test_configs = []
    all_fixtures = []

    try:
        # First pass: Extract configurations from all tests and collect fixtures for session discovery
        for test_name in test_names:
            print(f"📋 Preparing {test_name}...")

            # Extract test configuration
            test_data = extract_test_configuration(test_name)

            # Collect fixtures for session discovery (if any exist)
            if "custom_fixtures" in test_data.get("resource_configs", {}):
                all_fixtures.extend(test_data["resource_configs"]["custom_fixtures"])

            # Store base test configs (resources will be managed per-task now)
            # Create num_instances copies of each test case with unique identifiers
            for case in test_data["test_cases"]:
                for instance_num in range(num_instances):
                    # Generate unique instance identifier
                    instance_id = (
                        f"{test_name}_instance_{instance_num + 1}"
                        if num_instances > 1
                        else test_name
                    )

                    all_test_configs.append(
                        {
                            "test_name": test_name,
                            "instance_id": instance_id,
                            "instance_num": instance_num + 1,
                            "case": case,
                        }
                    )

        # Discover and set up session-level fixtures only
        session_fixtures, session_configs_map = discover_session_fixtures(all_fixtures)
        if session_fixtures:
            print(f"🌐 Found {len(session_fixtures)} session-level fixture types...")
            active_session_fixtures = session_fixtures
            active_session_data = setup_session_fixtures(
                session_fixtures, session_configs_map
            )
            print("✅ Session-level fixtures set up successfully")
        else:
            print("📝 No session-level fixtures required")
            active_session_data = {}

        def run_experiment_in_mode(mode: str):
            """Run experiment for a single mode and return (mode, result) tuple."""
            try:
                print(f"\n🧪 Running Braintrust experiment for {mode} mode...")
                experiment_name = construct_experiment_name(mode)

                # Create samples for this mode from all tests
                mode_samples = []
                for config in all_test_configs:
                    sample = {
                        "input": {
                            **config["case"]["input"],
                            "mode": mode,
                            "test_name": config["test_name"],
                            "instance_id": config.get(
                                "instance_id", config["test_name"]
                            ),
                            "instance_num": config.get("instance_num", 1),
                            "session_data": active_session_data,  # Pass session data for per-task resource setup
                            "skip_model_run": skip_model_run,
                            "infrastructure_only": infrastructure_only,
                        },
                        "metadata": {
                            **config["case"]["metadata"],
                            "mode": mode,
                            "instance_id": config.get(
                                "instance_id", config["test_name"]
                            ),
                            "instance_num": config.get("instance_num", 1),
                        },
                    }
                    mode_samples.append(sample)

                # Create unified validator that can handle all test types
                def unified_validator(input, output, expected=None):
                    test_name = "Unknown"
                    fixtures = []
                    test_resources = {}

                    try:
                        # Braintrust passes the full sample as 'input', so get test_name from there
                        test_name = input.get("test_name", "Unknown")
                        if test_name == "Unknown":
                            # Fallback: check in metadata if it exists
                            test_name = input.get("metadata", {}).get(
                                "test_name", "Unknown"
                            )

                        print(f"🔍 Validating test: {test_name}", flush=True)

                        # Handle case where output is None or task execution failed
                        if output is None:
                            print(f"❌ Task output is None for {test_name}", flush=True)
                            return {
                                "name": "validator",
                                "score": 0.0,
                                "metadata": {
                                    "error": "Task output is None",
                                    "test_steps": [],
                                },
                            }

                        # Handle case where task had execution error
                        if isinstance(output, dict) and output.get("execution_error"):
                            print(
                                f"❌ Task execution error for {test_name}: {output.get('result', {}).get('error', 'Unknown error')}",
                                flush=True,
                            )
                            return {
                                "name": "validator",
                                "score": 0.0,
                                "metadata": {
                                    "error": f"Task execution failed: {output.get('result', {}).get('error', 'Unknown error')}",
                                    "test_steps": [
                                        {
                                            "name": "Task Execution",
                                            "status": "failed",
                                            "Result_Message": f"❌ Task failed: {output.get('result', {}).get('error', 'Unknown error')}",
                                        }
                                    ],
                                },
                            }

                        # Extract model result and fixtures from the task output
                        model_result = (
                            output.get("result") if isinstance(output, dict) else output
                        )
                        fixtures_data = (
                            output.get("fixtures", {})
                            if isinstance(output, dict)
                            else {}
                        )
                        fixtures = (
                            fixtures_data if isinstance(fixtures_data, list) else []
                        )

                        # Extract test_resources from output if available for cleanup
                        test_resources = (
                            output.get("test_resources", {})
                            if isinstance(output, dict)
                            else {}
                        )

                        # Get and run the validator
                        validator = get_test_validator(test_name)
                        result = validator(model_result, expected, fixtures=fixtures)

                        # Ensure result has a "name" field for Braintrust
                        if isinstance(result, dict) and "name" not in result:
                            result["name"] = "validator"

                        print(
                            f"✅ Score for {test_name}: {result.get('score', 'N/A')}",
                            flush=True,
                        )
                        print(
                            f"✅ Metadata for {test_name}: {result.get('metadata', {})}",
                            flush=True,
                        )
                        return result

                    except Exception as e:
                        print(
                            f"❌ Validation error for {test_name}: {e}\n {traceback.format_exc()}",
                            flush=True,
                        )
                        # Return a proper failure result instead of False to maintain consistency
                        return {
                            "name": "validator",
                            "score": 0.0,
                            "metadata": {
                                "error": f"Validation failed: {str(e)}",
                                "traceback": traceback.format_exc(),
                                "test_steps": [
                                    {
                                        "name": "Validation",
                                        "status": "failed",
                                        "Result_Message": f"❌ Validation error: {str(e)}",
                                    }
                                ],
                            },
                        }
                    finally:
                        # Always attempt cleanup, even if validation failed
                        try:
                            _teardown_test_fixtures(test_name, fixtures, test_resources)
                        except Exception as cleanup_error:
                            print(
                                f"⚠️ Cleanup error for {test_name}: {cleanup_error}",
                                flush=True,
                            )
                            # Don't re-raise cleanup errors - they shouldn't stop other tests

                print(
                    f"🔍 Running Braintrust.Eval for {mode} mode with {len(mode_samples)} samples"
                )

                # Calculate max_concurrency based on flag
                max_concurrency_value = len(mode_samples) if full_concurrency else 20
                print(
                    f"🔧 Using max_concurrency={max_concurrency_value} {'(due to full-concurrency flag)' if full_concurrency else ''}"
                )

                # Run Braintrust.Eval for this mode with all tests
                result = braintrust.Eval(
                    name="DE-Bench",
                    experiment_name=experiment_name,
                    data=mode_samples,
                    task=run_de_bench_task,
                    scores=[unified_validator],
                    metadata={
                        "mode": mode,
                        "test_types": test_names,
                        "timestamp": str(time.time()),
                        "num_tests_included": len(mode_samples),
                        "num_tests_excluded": len(all_valid_tests) - len(test_names),
                        "all_valid_tests": all_valid_tests,
                    },
                    max_concurrency=max_concurrency_value,
                    trial_count=trial_count,
                )

                print(
                    f"✅ Completed {mode} experiment with {len(mode_samples)} samples"
                )
                print(f"   Summary: {result.summary}")

                # Note: Model artifacts and test resources are now cleaned up inside run_de_bench_task
                return (mode, result)

            except Exception as e:
                print(f"❌ Error running experiment for {mode} mode: {e}")
                print(f"   Traceback: {traceback.format_exc()}")
                # Create a dummy failed result to maintain consistency
                mock_result = type(
                    "MockResult",
                    (),
                    {
                        "summary": f"Failed to run experiment for {mode}: {str(e)}",
                        "scores": [],
                    },
                )()
                print(
                    f"⚠️ Skipping {mode} mode due to error, continuing with other modes..."
                )
                return (mode, mock_result)

        # Run mode experiments in parallel and collect results
        print(f"🔄 Running experiments for {len(modes)} mode(s) in parallel...")
        mode_results = map_func(run_experiment_in_mode, modes)

        # Build results dictionary from returned tuples
        results = {}
        for mode, result in mode_results:
            results[mode] = result
            print(f"📊 Collected result for {mode} mode")

        return results

    finally:
        # Note: Per-test resource cleanup now happens inside run_de_bench_task
        # Only session-level cleanup is needed here

        cleanup_handler()


if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()

    if args.verbose:
        print("🔧 Parsed arguments:")
        print(f"   Modes: {args.modes}")
        print(f"   Filter patterns: {args.filter_patterns}")
        print(f"   Verbose: {args.verbose}")
        print(f"   Number of trials: {args.num_trials}")

    try:
        # Discover tests with filtering
        if args.filter_patterns:
            print(f"🔍 Filtering tests with patterns: {args.filter_patterns}")
            test_discovery = discover_available_tests(args.filter_patterns)
            filtered_tests = test_discovery["filtered_tests"]
            all_tests = test_discovery["all_tests"]
        else:
            test_discovery = discover_available_tests()
            filtered_tests = test_discovery["filtered_tests"]
            all_tests = test_discovery["all_tests"]

        # Run evaluation on filtered tests
        results = run_multi_test_evaluation(
            modes=args.modes,
            test_names=filtered_tests,
            all_valid_tests=all_tests,
            verbose=args.verbose,
            skip_model_run=args.skip_model_run,
            infrastructure_only=args.infrastructure_only,
            trial_count=args.num_trials,
            num_instances=args.num_instances,
            full_concurrency=args.full_concurrency,
        )

        if results:
            print(f"\n🎉 Completed {len(results)} multi-test experiments!")

            # Print summary for each mode
            for mode, result in results.items():
                print(f"\n📊 {mode} Mode Results:")
                print(f"   Summary: {result.summary}")
        else:
            print("📝 No experiments were run")

    except KeyboardInterrupt:
        print("\n🛑 Evaluation interrupted by user")
    except Exception as e:
        print(f"❌ Evaluation failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # Ensure final cleanup
        cleanup_handler()
