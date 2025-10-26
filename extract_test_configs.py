import os
import importlib
import importlib.util
import time
import uuid
from typing import Dict, List, Any, Optional, Union, Tuple
from typing_extensions import TypedDict, NotRequired
from Fixtures.Supabase_Account.supabase_account_resource import supabase_client
import requests
import jwt
from braintrust import traced
import traceback

# Type definitions for better code clarity and IDE support


class TestStepConfig(TypedDict):
    name: str
    description: str
    status: str
    Result_Message: str


class TestCaseInput(TypedDict):
    task: str
    configs: Dict[str, Any]
    mode: NotRequired[str]
    test_resources: NotRequired[Dict[str, Any]]
    test_name: NotRequired[str]


class TestCaseMetadata(TypedDict):
    test_name: str
    test_function: str
    resource_configs: Dict[str, Any]
    mode: NotRequired[str]


class TestCase(TypedDict):
    input: TestCaseInput
    metadata: TestCaseMetadata


class TestConfiguration(TypedDict):
    test_cases: List[TestCase]
    resource_configs: Dict[str, Any]


class ResourceData(TypedDict):
    resource_id: str
    type: str
    creation_time: float
    creation_duration: float
    description: str
    status: str


class SupabaseAccountResource(TypedDict):
    userID: str
    publicKey: NotRequired[str]
    secretKey: NotRequired[str]
    jwt_token: NotRequired[str]


@traced(name="extract_test_configuration")
def extract_test_configuration(test_name: str) -> TestConfiguration:
    """Generic test configuration extraction for any test following the standard pattern"""
    try:
        # Dynamically import the test config
        config_module_path = f"Tests.{test_name}.Test_Configs"
        Test_Configs = importlib.import_module(config_module_path)

        # Check if the test provides its own fixtures
        resource_configs = {}
        custom_fixtures = None

        # Try to import the test module to check for get_fixtures function
        try:
            test_files = []
            # Use absolute path to avoid working directory issues during parallel execution
            current_dir = os.path.dirname(os.path.abspath(__file__))
            test_dir = os.path.join(current_dir, "Tests", test_name)

            for file in os.listdir(test_dir):
                if file.startswith("test_") and file.endswith(".py"):
                    test_files.append(file[:-3])  # Remove .py extension

            if test_files:
                test_module_path = f"Tests.{test_name}.{test_files[0]}"
                test_module = importlib.import_module(test_module_path)

                # Check if test defines its own fixtures and config creation (REQUIRED)
                if hasattr(test_module, "get_fixtures"):
                    custom_fixtures = test_module.get_fixtures()
                    print(
                        f"📦 {test_name} provides custom fixtures: {[f.get_resource_type() for f in custom_fixtures]}"
                    )

                    create_model_inputs_func = None
                    if hasattr(test_module, "create_model_inputs"):
                        create_model_inputs_func = test_module.create_model_inputs
                        print(
                            f"⚙️  {test_name} provides custom inputs modification function"
                        )

                    resource_configs = {
                        "custom_fixtures": custom_fixtures,
                        "create_model_inputs_func": create_model_inputs_func,
                    }
                else:
                    # Legacy test - not supported anymore
                    raise ValueError(
                        f"❌ {test_name} uses legacy pattern. All tests must provide get_fixtures() and create_model_inputs() functions."
                    )
            else:
                # No test files found
                raise ValueError(f"❌ No test files found for {test_name}")

        except Exception as e:
            print(f"Error: Could not check for custom fixtures in {test_name}: {e}")
            raise ValueError(f"❌ Failed to load test {test_name}: {e}")

        # Find the test function name by convention
        test_function_name = get_test_function_name(test_name)

        # Create the test case data
        # All tests now use create_config function - no more base configs needed
        test_case = {
            "input": {"task": Test_Configs.User_Input, "configs": {}},
            "metadata": {
                "test_name": test_name,
                "test_function": test_function_name,
                "resource_configs": resource_configs,
                "custom_fixtures": custom_fixtures,
            },
        }

        return {"test_cases": [test_case], "resource_configs": resource_configs}

    except ImportError as e:
        raise ValueError(f"Could not import config for test {test_name}: {e}")


def get_test_function_name(test_name: str) -> str:
    """Convert test name to expected function name by convention"""
    # Convert CamelCase to snake_case and add test_ prefix
    import re

    snake_case = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", test_name)
    snake_case = re.sub("([a-z0-9])([A-Z])", r"\1_\2", snake_case).lower()
    return f"test_{snake_case}"


# ========== Session-Level Fixture Management ==========


def discover_session_fixtures(all_fixtures: List[Any]) -> List[Any]:
    """
    Discover which fixtures require session-level setup across all tests.

    Args:
        all_fixtures: List of all DEBenchFixture instances from all tests

    Returns:
        List of unique fixture classes that require session setup
    """
    from Fixtures.base_fixture import DEBenchFixture

    session_fixture_classes = set()

    for fixture in all_fixtures:
        if (
            isinstance(fixture, DEBenchFixture)
            and fixture.__class__.requires_session_setup()
        ):
            session_fixture_classes.add(fixture.__class__)

    # Return one instance of each unique session fixture class
    return [fixture_class() for fixture_class in session_fixture_classes]


def setup_session_fixtures(session_fixtures: List[Any]) -> Dict[str, Any]:
    """
    Set up session-level fixtures that will be shared across all tests.

    Args:
        session_fixtures: List of unique session fixtures to set up

    Returns:
        Dictionary mapping resource_type -> session_data
    """
    from Fixtures.base_fixture import DEBenchFixture

    session_data = {}

    for fixture in session_fixtures:
        if not isinstance(fixture, DEBenchFixture):
            continue

        resource_type = fixture.get_resource_type()

        try:
            print(f"🔧 Setting up session-level {resource_type}...")
            fixture_session_data = fixture.session_setup()
            session_data[resource_type] = fixture_session_data
            print(f"✅ Session-level {resource_type} set up successfully")
        except Exception as e:
            print(
                f"❌ Failed to set up session-level {resource_type}: {e}, {traceback.format_exc()}"
            )

    return session_data


def cleanup_session_fixtures(
    session_fixtures: List[Any], session_data: Dict[str, Any]
) -> None:
    """
    Clean up session-level fixtures after all tests complete.

    Args:
        session_fixtures: List of session fixtures to clean up
        session_data: Session data returned from setup_session_fixtures
    """
    from Fixtures.base_fixture import DEBenchFixture

    for fixture in session_fixtures:
        if not isinstance(fixture, DEBenchFixture):
            continue

        resource_type = fixture.get_resource_type()

        try:
            print(f"🧹 Cleaning up session-level {resource_type}...")
            fixture.session_teardown(session_data.get(resource_type))
            print(f"✅ Session-level {resource_type} cleaned up successfully")
        except Exception as e:
            print(f"❌ Failed to clean up session-level {resource_type}: {e}")


def setup_test_resources_from_fixtures(
    fixtures: List[Any],
    fixture_configs: Dict[str, Any] = None,
    session_data: Dict[str, Any] = None,
) -> Tuple[Dict[str, Any], List[Any]]:
    """
    Set up test resources from a provided list of DEBenchFixture instances.
    This allows tests to provide their own initialized fixture objects.

    Args:
        fixtures: List of DEBenchFixture instances
        fixture_configs: Optional mapping of resource_type -> config for each fixture
        session_data: Optional session data from session-level fixtures

    Returns:
        Tuple of (resource_data_dict, fixture_instances_list)
        - resource_data_dict: Dictionary mapping resource_type -> resource_data
        - fixture_instances_list: List of fixture instances with _resource_data set
    """
    from Fixtures.base_fixture import DEBenchFixture

    resources = {}
    fixture_configs = fixture_configs or {}
    session_data = session_data or {}

    for fixture in fixtures:
        if not isinstance(fixture, DEBenchFixture):
            raise ValueError(f"Fixture {fixture} does not implement DEBenchFixture")

        resource_type = fixture.get_resource_type()

        # Pass session data to fixture if available
        if resource_type in session_data:
            fixture.session_data = session_data[resource_type]

        # Use provided config or default config
        config = fixture_configs.get(resource_type, fixture.get_default_config())

        # Check if fixture was initialized with custom config
        if hasattr(fixture, "custom_config") and fixture.custom_config is not None:
            # Fixture has custom config, let it use that
            resource_data = fixture._test_setup()
            print(f"✅ Set up {resource_type} using fixture's custom config")
        else:
            # Use provided config or default
            resource_data = fixture._test_setup(config)
            print(f"✅ Set up {resource_type} using provided fixture")

        # Store resource data in the fixture AND the resources dict
        resources[resource_type] = resource_data

    return resources, fixtures


@traced(name="setup_supabase_account_resource")
def setup_supabase_account_resource(mode: str = "Ardent") -> SupabaseAccountResource:
    """Set up Supabase account resource"""
    print(f"Setting up Supabase account resource for mode: {mode}")

    # Create unique email for this test to avoid conflicts
    test_id = str(uuid.uuid4())[:8]
    unique_email = f"test-{test_id}@example.com"

    # Create user with shared admin client
    resp = supabase_client.auth.admin.create_user(
        {"email": unique_email, "password": "Str0ngP@ss!", "email_confirm": True}
    )

    response = {}
    user_id = resp.user.id
    response["userID"] = user_id

    # Generate JWT and API keys for Ardent mode
    if mode == "Ardent":
        jwt_payload = {
            "sub": user_id,
            "email": unique_email,
            "role": "authenticated",
            "aud": "authenticated",
            "iss": "supabase",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
            "session_id": str(uuid.uuid4()),
        }

        jwt_token = jwt.encode(
            jwt_payload, os.environ["SUPABASE_JWT_SECRET"], algorithm="HS256"
        )

        response["jwt_token"] = jwt_token

        # Get org_id using V2 API
        my_orgs_response = requests.get(
            f"{os.getenv('ARDENT_BASE_URL')}/v2/my-orgs",
            headers={
                "Authorization": f"Bearer {jwt_token}",
            },
            timeout=120,
        )

        if not my_orgs_response.ok:
            raise requests.exceptions.ConnectionError(
                f"Failed to get orgs: HTTP {my_orgs_response.status_code} - {my_orgs_response.text}"
            )

        orgs_data = my_orgs_response.json()
        
        # If no orgs exist, create one for the new user
        if not orgs_data.get("orgs") or len(orgs_data["orgs"]) == 0:
            print(f"No orgs found for user {user_id}, creating default org...")
            
            create_org_response = requests.post(
                f"{os.getenv('ARDENT_BASE_URL')}/v2/orgs",
                json={
                    "name": f"DE-Bench Test Org {test_id}"
                },
                headers={
                    "Authorization": f"Bearer {jwt_token}",
                    "Content-Type": "application/json",
                },
                timeout=120,
            )
            
            if not create_org_response.ok:
                raise requests.exceptions.ConnectionError(
                    f"Failed to create org: HTTP {create_org_response.status_code} - {create_org_response.text}"
                )
            
            org_data = create_org_response.json()
            org_id = org_data["id"]
            print(f"Created new org: {org_id}")
        else:
            org_id = orgs_data["orgs"][0]["org_id"]
            print(f"Using existing org_id: {org_id}")

        # Create API keys using V2 API
        token_creation_response = requests.post(
            f"{os.getenv('ARDENT_BASE_URL')}/v2/orgs/{org_id}/api-keys",
            json={
                "name": f"DE-Bench Test Key {test_id}",
                "scopes": [
                    "jobs.create", "jobs.read", "jobs.update", "jobs.delete",
                    "connectors.create", "connectors.read", "connectors.update", "connectors.delete"
                ],
                "expires_days": None  # Never expire
            },
            headers={
                "Authorization": f"Bearer {jwt_token}",
                "Content-Type": "application/json",
            },
            timeout=120,
        )

        if not token_creation_response.ok:
            raise requests.exceptions.ConnectionError(
                f"Failed to create keys: HTTP {token_creation_response.status_code} - {token_creation_response.text}"
            )

        token_data = token_creation_response.json()
        response["publicKey"] = token_data["public_key"]  # V2 uses public_key
        response["secretKey"] = token_data["secret_key"]  # V2 uses secret_key
        response["api_key_id"] = token_data["api_key_id"]  # Store for cleanup
        response["org_id"] = org_id  # Store org_id for job creation

    print(f"Supabase account resource created successfully for user: {user_id}")
    return response


@traced(name="setup_test_resources")
def setup_test_resources(
    resource_configs: Dict[str, Any], session_data: Dict[str, Any] = None
) -> Tuple[Dict[str, Any], List[Any]]:
    """Generic setup for all test resources using DEBenchFixture instances"""
    resources = {}
    session_data = session_data or {}
    fixtures = []

    # Check if test provides custom fixtures
    if "custom_fixtures" in resource_configs and resource_configs["custom_fixtures"]:
        custom_fixtures = resource_configs["custom_fixtures"]

        # Set up Supabase account if needed (always needed for Ardent mode)
        resources["supabase_account_resource"] = setup_supabase_account_resource()

        # Set up custom fixtures with session data
        fixture_resources, fixture_instances = setup_test_resources_from_fixtures(
            custom_fixtures, session_data=session_data
        )
        resources.update(fixture_resources)
        fixtures.extend(fixture_instances)

        return resources, fixtures

    # All tests must now use the new pattern with get_fixtures() and create_model_inputs()
    # This fallback should never be reached since we enforce the new pattern above
    raise ValueError(
        f"❌ Invalid test configuration for {resource_configs}"
    )  # This shouldn't happen


@traced(name="cleanup_supabase_account_resource")
def cleanup_supabase_account_resource(
    supabase_resource_data: SupabaseAccountResource,
) -> None:
    """Clean up Supabase account resource"""
    try:
        user_id = supabase_resource_data["userID"]

        # Delete API keys if they exist (V2 API)
        if (
            "api_key_id" in supabase_resource_data
            and "jwt_token" in supabase_resource_data
        ):
            # Get org_id from V2 API
            my_orgs_response = requests.get(
                f"{os.getenv('ARDENT_BASE_URL')}/v2/my-orgs",
                headers={
                    "Authorization": f"Bearer {supabase_resource_data['jwt_token']}",
                },
                timeout=10,
            )
            
            if my_orgs_response.ok:
                orgs_data = my_orgs_response.json()
                if orgs_data.get("orgs") and len(orgs_data["orgs"]) > 0:
                    org_id = orgs_data["orgs"][0]["org_id"]
                    
                    # Delete API key using V2 API
                    delete_key_response = requests.delete(
                        f"{os.getenv('ARDENT_BASE_URL')}/v2/orgs/{org_id}/api-keys/{supabase_resource_data['api_key_id']}",
                        headers={
                            "Authorization": f"Bearer {supabase_resource_data['jwt_token']}",
                        },
                        timeout=10,
                    )

        # Always delete the user
        supabase_client.auth.admin.delete_user(user_id)
        print(f"Supabase account resource for user {user_id} cleaned up successfully")

    except Exception as e:
        print(f"Error cleaning up Supabase account resource: {e}")


def get_test_validator(test_name: str) -> callable:
    """Get a generic validator function for any test following the standard pattern"""

    def generic_validator(
        output: Dict[str, Any],
        expected: Optional[Any] = None,
        fixtures: Optional[List] = None,
    ) -> bool:
        """Generic test validation using the validate_test function from the test file"""
        try:
            # Check if the model execution was successful first
            if not output or output.get("status") == "failed":
                return {
                    "name": "validator",
                    "score": 0.0,
                    "metadata": {
                        "test_steps": [],
                        "error": "Model execution failed",
                    },
                }

            # Dynamically import the validate_test function from the test file
            test_files = []
            # Use absolute path to avoid working directory issues during parallel execution
            current_dir = os.path.dirname(os.path.abspath(__file__))
            test_dir = os.path.join(current_dir, "Tests", test_name)

            # Find test files in the directory
            for file in os.listdir(test_dir):
                if file.startswith("test_") and file.endswith(".py"):
                    test_files.append(file[:-3])  # Remove .py extension

            if not test_files:
                print(f"No test files found for {test_name}")
                return {
                    "name": test_name,
                    "score": 0.0,
                    "metadata": {
                        "test_steps": [],
                        "error": "No test files found",
                    },
                }

            # Import the first test file found
            test_module_path = f"Tests.{test_name}.{test_files[0]}"
            test_module = importlib.import_module(test_module_path)

            # Get the validate_test function
            if not hasattr(test_module, "validate_test"):
                print(f"No validate_test function found in {test_module_path}")
                return {
                    "name": "validator",
                    "score": 0.0,
                    "metadata": {
                        "test_steps": [],
                        "error": "No validate_test function found",
                    },
                }

            validate_test = test_module.validate_test

            # Call the validation function - pass fixtures if available
            validation_result = validate_test(output, fixtures=fixtures)

            # validate_test should return either:
            # - A boolean (simple pass/fail)
            # - A dict with 'score' float and 'metadata' dict, with optional 'test_steps' list inside
            if isinstance(validation_result, bool):
                return {
                    "score": 1.0 if validation_result else 0.0,
                    "metadata": {"test_steps": []},
                }
            elif isinstance(validation_result, dict):
                score = validation_result.get("score", 0.0)
                test_steps = validation_result.get("metadata", {}).get("test_steps", [])

                # Log the test steps for debugging
                print(f"📋 Test steps for {test_name}:")
                for step in test_steps:
                    status = step.get("status", "unknown")
                    name = step.get("name", "unnamed step")
                    print(f"   • {name}: {status}")

                return {
                    "name": "validator",
                    "score": score,
                    "metadata": validation_result.get("metadata", {}),
                }
            else:
                print(f"Invalid validation result type: {type(validation_result)}")
                return {
                    "name": "validator",
                    "score": 0.0,
                    "metadata": {
                        "test_steps": [],
                        "error": "Invalid validation result type: "
                        + str(type(validation_result)),
                    },
                }

        except Exception as e:
            print(f"Validation error for {test_name}: {e}")
            return {
                "name": "validator",
                "score": 0.0,
                "metadata": {
                    "test_steps": [],
                    "error": "Validation error: " + str(e),
                },
            }

    return generic_validator


def create_config_from_fixtures(fixtures: List) -> Dict[str, Any]:
    """
    Helper function to create a complete config from multiple fixtures.

    Args:
        fixtures: List of DEBenchFixture instances with resource data populated

    Returns:
        Complete configuration dictionary with all fixture config sections
    """
    from Fixtures.base_fixture import DEBenchFixture

    config = {"services": {}}

    for fixture in fixtures:
        if isinstance(fixture, DEBenchFixture):
            try:
                fixture_config = fixture.create_config_section()
                # Merge the fixture's config section into the main config
                config["services"].update(fixture_config)
            except Exception as e:
                print(
                    f"⚠️  Warning: Could not create config for {fixture.get_resource_type()}: {e}"
                )

    return config
