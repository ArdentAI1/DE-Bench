# Braintrust-only Simple Hello World test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This simple test doesn't need complex resources, so we use a basic MongoDB fixture
    just to provide the standard interface (mainly for Supabase account in Ardent mode).
    """
    from Fixtures.MongoDB.mongo_resources import MongoDBFixture

    # Use default MongoDB fixture config since this test doesn't actually use MongoDB
    # This just ensures we have a consistent fixture interface
    mongo_fixture = MongoDBFixture()  # Uses default config
    return [mongo_fixture]


def create_model_inputs(
    base_model_inputs: Dict[str, Any], fixtures: List[DEBenchFixture]
) -> Dict[str, Any]:
    """
    Create test-specific config using the set-up fixtures.
    For this simple test, we don't need any real config since it's just a hello world test.
    """
    from extract_test_configs import create_config_from_fixtures

    # Use the helper to automatically create config from all fixtures
    # MongoDB fixture will provide minimal config even though we don't use it
    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully generated both:
    1. A text response containing 'hello world'
    2. A Python script that returns 'hello world'

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test (unused for this simple test)

    Returns:
        dict: Contains 'success' boolean and 'test_steps' list with validation details
    """
    import re

    # Create test steps for this validation
    test_steps = [
        {
            "name": "Hello World Text Response",
            "description": "Getting a text response containing 'hello world'",
            "status": "running",
            "Result_Message": "Checking for 'hello world' in text response...",
        },
        {
            "name": "Hello World Python Script",
            "description": "Creating a Python script that returns 'hello world'",
            "status": "running",
            "Result_Message": "Checking for Python script that returns 'hello world'...",
        },
    ]

    if not model_result:
        test_steps[0]["status"] = "failed"
        test_steps[0]["Result_Message"] = "No model result received"
        test_steps[1]["status"] = "failed"
        test_steps[1]["Result_Message"] = "No model result received"
        return {"success": False, "test_steps": test_steps}

    # Extract the response text based on the result format
    response_text = ""
    if isinstance(model_result, dict):
        # For Ardent mode, look for response in various possible fields
        if "response" in model_result:
            response_text = str(model_result["response"])
        elif "result" in model_result:
            response_text = str(model_result["result"])
        elif "output" in model_result:
            response_text = str(model_result["output"])
        else:
            # Convert entire result to string as fallback
            response_text = str(model_result)
    else:
        response_text = str(model_result)

    print(f"Checking response text: '{response_text}'", flush=True)

    # VALIDATION 1: Check if 'hello world' appears in the text response
    text_validation_passed = False
    if "hello world" in response_text.lower():
        test_steps[0]["status"] = "passed"
        test_steps[0][
            "Result_Message"
        ] = f"✅ Successfully found 'hello world' in text response"
        text_validation_passed = True
        print("✅ Text response validation: PASSED", flush=True)
    else:
        test_steps[0]["status"] = "failed"
        test_steps[0][
            "Result_Message"
        ] = f"❌ Did not find 'hello world' in text response"
        print("❌ Text response validation: FAILED", flush=True)

    # VALIDATION 2: Check for Python script that returns 'hello world'
    script_validation_passed = False

    # Look for Python code in the response (common patterns)
    python_code_patterns = [
        r"```python\s*(.*?)\s*```",  # ```python code```
        r"```\s*(.*?)\s*```",  # ```code```
        r'def.*?hello.*?world.*?:.*?return.*?["\']hello world["\']',  # function patterns
        r'print\s*\(\s*["\']hello world["\']\s*\)',  # print statements
        r'return\s+["\']hello world["\']',  # return statements
    ]

    extracted_code = None
    for pattern in python_code_patterns:
        matches = re.findall(pattern, response_text, re.DOTALL | re.IGNORECASE)
        if matches:
            extracted_code = matches[0].strip()
            print(f"Found Python code pattern: {extracted_code}", flush=True)
            break

    if extracted_code:
        try:
            # Create a safe execution environment
            exec_globals = {"__builtins__": {"print": print, "str": str, "len": len}}
            exec_locals = {}

            # Execute the code
            exec(extracted_code, exec_globals, exec_locals)

            # Check if there's a main function or return value
            result = None
            if "main" in exec_locals and callable(exec_locals["main"]):
                result = exec_locals["main"]()
            elif "hello_world" in exec_locals and callable(exec_locals["hello_world"]):
                result = exec_locals["hello_world"]()
            else:
                # Look for any function that might return hello world
                for name, value in exec_locals.items():
                    if callable(value):
                        try:
                            result = value()
                            break
                        except:
                            continue

            # Check if the result is 'hello world'
            if result and isinstance(result, str) and "hello world" in result.lower():
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = f"✅ Python script successfully returned 'hello world': {result}"
                script_validation_passed = True
                print("✅ Python script validation: PASSED", flush=True)
            else:
                # Fallback: check if the code contains a simple return statement
                if (
                    "return" in extracted_code.lower()
                    and "hello world" in extracted_code.lower()
                ):
                    test_steps[1]["status"] = "passed"
                    test_steps[1][
                        "Result_Message"
                    ] = f"✅ Python script contains valid return statement with 'hello world'"
                    script_validation_passed = True
                    print(
                        "✅ Python script validation: PASSED (contains return statement, flush=True)"
                    )
                else:
                    test_steps[1]["status"] = "failed"
                    test_steps[1][
                        "Result_Message"
                    ] = f"❌ Python script did not return 'hello world', got: {result}"
                    print(f"❌ Python script validation: FAILED - returned {result}", flush=True)

        except Exception as e:
            test_steps[1]["status"] = "failed"
            test_steps[1]["Result_Message"] = f"❌ Error executing Python script: {e}"
            print(f"❌ Python script validation: ERROR - {e}", flush=True)
    else:
        test_steps[1]["status"] = "failed"
        test_steps[1]["Result_Message"] = "❌ No Python script found in response"
        print("❌ Python script validation: FAILED - no code found", flush=True)

    # Overall validation: both must pass
    overall_success = text_validation_passed and script_validation_passed

    if overall_success:
        print("🎉 Overall validation: PASSED", flush=True)
    else:
        failed_parts = []
        if not text_validation_passed:
            failed_parts.append("text response")
        if not script_validation_passed:
            failed_parts.append("Python script")

        error_msg = f"Validation failed for: {', '.join(failed_parts)}"
        print(f"❌ Overall validation: FAILED - {error_msg}", flush=True)

    # Calculate score as the fraction of steps that passed
    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
