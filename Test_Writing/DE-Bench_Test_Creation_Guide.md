# DE-Bench Test Creation Guide

This document explains how to create tests in the DE-Bench framework. The framework follows a standardized structure designed for testing data engineering agents with real-world scenarios.

## 📁 Directory Structure

Every test must follow this directory structure:

```
Tests/
├── Your_Test_Name/
│   ├── test_your_test_name.py      # Main test file
│   ├── Test_Configs.py             # Configuration and user input
│   ├── README.md                   # Test documentation (recommended)
│   └── Dockerfile                  # Optional: if custom Docker setup needed
```

### Required Files

1. **`test_*.py`** - Main test implementation
2. **`Test_Configs.py`** - Configuration and user query definition

### Optional Files

1. **`README.md`** - Test documentation (highly recommended)
2. **`Dockerfile`** - Custom Docker configuration if needed
3. **`docker-compose.yml`** - Multi-service Docker setup

## 🏗️ Test Architecture

DE-Bench uses a **standardized isolated test structure** to prevent resource conflicts:

### 🔐 **Standard Test Pattern** (Per-Test User Isolation)
- **Isolation**: Each test gets a unique Supabase user + API keys to prevent config clashing
- **Resource Separation**: Backend configs are stored per-user, so parallel tests don't interfere
- **Backend Integration**: Full integration with Ardent backend for realistic scenarios
- **Automatic Cleanup**: Users, API keys, and backend configurations cleaned automatically
- **Parallel Safe**: Multiple tests can run simultaneously without resource conflicts

## 🏗️ Standard Test Structure

All DE-Bench tests follow this **per-user isolation pattern** to prevent config conflicts:

```python
# Import from the Model directory
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, remove_model_configs
import os
import importlib
import pytest
import time

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

@pytest.mark.your_technology    # e.g., @pytest.mark.mongodb
@pytest.mark.your_category     # e.g., @pytest.mark.database
@pytest.mark.difficulty        # e.g., @pytest.mark.three
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
def test_your_function_name(request, resource_fixture, supabase_account_resource):
    """Your test description with backend authentication."""
    
    # Set up test tracking
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    
    test_steps = [
        {
            "name": "Step 1 Name", 
            "description": "What this step validates",
            "status": "did not reach",
            "Result_Message": "",
        },
        # Add more steps as needed
    ]
    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    config_results = None
    try:
        # Set up model configurations with test-specific API keys
        config_results = set_up_model_configs(
            Configs=Test_Configs.Configs,
            custom_info={
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        model_result = run_model(
            container=None, 
            task=Test_Configs.User_Input, 
            configs=Test_Configs.Configs,
            extra_information={
                "useArdent": True,
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))

        # SECTION 3: VERIFY THE OUTCOMES
        # Your validation logic here
        # Update test_steps with results
        
        # Example validation:
        if validation_passes:
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = "Success message"
            assert True, "Test passed"
        else:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "Failure reason"
            raise AssertionError("Test failed because...")

    finally:
        # CLEANUP - Include API keys for proper cleanup
        if config_results:
            remove_model_configs(
                Configs=Test_Configs.Configs, 
                custom_info={
                    **config_results,  # Spread all config results
                    "publicKey": supabase_account_resource["publicKey"],
                    "secretKey": supabase_account_resource["secretKey"],
                }
            )
```

## ⚙️ Test_Configs.py Structure

The `Test_Configs.py` file defines the user query and system configuration:

```python
import os

# The task/query for the AI agent to solve
User_Input = """
Clear, specific instructions for the AI agent.
Include:
1. What to create/modify
2. Specific requirements
3. Expected outputs
4. Any naming conventions
"""

# System configuration for the test environment
Configs = {
    "services": {
        "service_name": {
            "param1": os.getenv("ENV_VAR_NAME"),
            "param2": "static_value",
            "nested_config": {
                "sub_param": os.getenv("ANOTHER_ENV_VAR")
            }
        }
    }
}
```

### Examples

**MongoDB Test Config:**
```python
import os

User_Input = "Go to test_collection in MongoDB and add another record. Please add the record with the name 'John Doe' and the age 30."

Configs = {
    "services": {
        "mongodb": {
            "connection_string": os.getenv("MONGODB_URI"),
            "databases": [
                {"name": "test_database", "collections": [{"name": "test_collection"}]}
            ],
        }
    }
}
```

**Airflow Test Config:**
```python
import os

User_Input = """
Create a simple Airflow DAG that:
1. Prints "Hello World" to the logs
2. Runs daily at midnight
3. Has a single task named 'print_hello'
4. Name the DAG 'hello_world_dag'
5. Create it in a branch called 'feature/hello_world_dag'
6. Name the PR 'Add Hello World DAG'
"""

Configs = {
    "services": {
        "airflow": {
            "github_token": os.getenv("AIRFLOW_GITHUB_TOKEN"),
            "repo": os.getenv("AIRFLOW_REPO"),
            "dag_path": os.getenv("AIRFLOW_DAG_PATH"),
            "host": os.getenv("AIRFLOW_HOST", "http://localhost:8080"),
            "username": os.getenv("AIRFLOW_USERNAME", "airflow"),
            "password": os.getenv("AIRFLOW_PASSWORD", "airflow"),
        }
    }
}
```

## 🏷️ Test Markers

Use appropriate pytest markers to categorize your tests:

### Difficulty Markers (Required)
```python
@pytest.mark.one     # Difficulty 1 (easiest)
@pytest.mark.two     # Difficulty 2
# ... up to ...
@pytest.mark.ten     # Difficulty 10 (hardest)
```

### Technology Markers
```python
@pytest.mark.mongodb
@pytest.mark.airflow
@pytest.mark.postgres
@pytest.mark.mysql
@pytest.mark.databricks
@pytest.mark.aws
@pytest.mark.s3
```

### Category Markers
```python
@pytest.mark.code_writing
@pytest.mark.database
@pytest.mark.pipeline
@pytest.mark.api_integration
@pytest.mark.environment_management
```

## 🔧 Fixtures and Resource Management

### Understanding Fixture Scopes

DE-Bench uses **three types of fixtures** based on resource lifecycle needs:

#### **📍 Per-Test Fixtures (`scope="function"`)** 
- **Creates fresh resources for EACH test**
- **Automatic cleanup after EACH test**
- **Use when**: Tests need isolation, modify data, or can't share safely

#### **🌍 Global/Shared Fixtures (`scope="session"`)** 
- **Creates ONE resource shared across ALL tests with same ID**
- **Cleanup only at session end**
- **Use when**: Resources are expensive, tests only read data, or sharing is safe

#### **🔐 Authentication Fixtures (`scope="function"`)** 
- **Creates unique user + API keys for EACH test**
- **Complete isolation between parallel tests**
- **Use when**: Tests require backend authentication via Ardent

### Available Fixtures

#### **Per-Test Fixtures (Function-Scoped)**

**MongoDB (Fresh per test):**
```python
@pytest.mark.parametrize("mongo_resource", [{
    "resource_id": "your_test_mongo_resource",
    "databases": [
        {
            "name": "test_database",
            "collections": [
                {
                    "name": "test_collection",
                    "data": []  # Initial data
                }
            ]
        }
    ]
}], indirect=True)
def test_mongodb_function(request, mongo_resource):
    # Each test gets fresh MongoDB collections
```

**Airflow (Fresh per test):**
```python
@pytest.mark.airflow
def test_airflow_function(request, airflow_resource):
    # Each test gets its own Airflow instance
    base_url = airflow_resource["base_url"]
    api_token = airflow_resource["api_token"]
```

#### **Authentication Fixtures (Function-Scoped)**

**Supabase Account Resource (NEW):**
```python
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
def test_with_backend_auth(request, mongo_resource, supabase_account_resource):
    # Each test gets unique Supabase user + API keys
    public_key = supabase_account_resource["publicKey"]    # e.g., "ardent_pk_..."
    secret_key = supabase_account_resource["secretKey"]    # e.g., "ardent_sk_..."  
    user_id = supabase_account_resource["userID"]          # Unique user ID
    
    # Use in your test for backend authentication
    # Automatic cleanup handles user + API key deletion
```

**Key Benefits:**
- ✅ **Complete isolation**: Each test gets its own user account
- ✅ **Parallel-safe**: No conflicts between concurrent tests  
- ✅ **Automatic cleanup**: Users and API keys automatically deleted
- ✅ **Secure**: Fresh credentials per test, no shared secrets

#### **Shared Fixtures (Session-Scoped)**

**Shared Resource (Multiple tests share same instance):**
```python
@pytest.mark.parametrize("shared_resource", ["shared_setup_id"], indirect=True)
def test_read_operation_1(request, shared_resource):
    # Uses shared resource with ID "shared_setup_id"
    pass

@pytest.mark.parametrize("shared_resource", ["shared_setup_id"], indirect=True)  
def test_read_operation_2(request, shared_resource):
    # Reuses SAME resource as test_read_operation_1
    pass

@pytest.mark.parametrize("shared_resource", ["different_setup_id"], indirect=True)
def test_isolated_operation(request, shared_resource):
    # Gets separate resource with ID "different_setup_id"
    pass
```

### Choosing the Right Fixture Type

| Scenario | Use Fixture Type | Reason |
|----------|------------------|---------|
| Tests modify/delete data | **Per-Test** (`mongo_resource`) | Need clean slate each test |
| Tests only read data | **Shared** (`shared_resource`) | Safe to share, faster execution |
| Expensive resource setup | **Shared** (`shared_resource`) | Amortize cost across tests |
| Test isolation critical | **Per-Test** (`mongo_resource`) | Prevent test interference |
| Multiple similar tests | **Shared** (`shared_resource`) | Resource efficiency |
| **Backend authentication needed** | **Auth** (`supabase_account_resource`) | **Secure, isolated API access** |
| **Parallel test execution** | **Auth** (`supabase_account_resource`) | **Prevent credential conflicts** |



**Step 2: Will the test modify/delete data?**
- ✅ **YES** → Use **Per-Test Fixtures** (`mongo_resource`, `airflow_resource`)
- ❌ **NO** → Continue to Step 3

**Step 3: Is resource setup expensive (>10 seconds)?**
- ✅ **YES** → Use **Shared Fixtures** (`shared_resource` with descriptive ID)
- ❌ **NO** → Use **Per-Test Fixtures** for simplicity

## ✅ Validation Patterns

### Multi-Layer Validation
Implement multiple validation layers for robust testing:

```python
# SECTION 3: VERIFY THE OUTCOMES
validation_passed = False

try:
    # Layer 1: Basic existence checks
    if basic_resource_exists():
        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "Resource created successfully"
        
        # Layer 2: Content validation
        if content_is_correct():
            test_steps[1]["status"] = "passed" 
            test_steps[1]["Result_Message"] = "Content validation passed"
            
            # Layer 3: Functional validation
            if functionality_works():
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = "Functional test passed"
                validation_passed = True
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = "Functionality test failed"
        else:
            test_steps[1]["status"] = "failed"
            test_steps[1]["Result_Message"] = "Content validation failed"
    else:
        test_steps[0]["status"] = "failed"
        test_steps[0]["Result_Message"] = "Resource creation failed"

    if validation_passed:
        assert True, "All validations passed"
    else:
        raise AssertionError("One or more validations failed")

except Exception as e:
    # Update any remaining test steps
    for step in test_steps:
        if step["status"] == "did not reach":
            step["status"] = "failed"
            step["Result_Message"] = f"Exception occurred: {str(e)}"
    raise
```

### Test Isolation
Ensure each test run is completely isolated:

```python
import uuid
from datetime import datetime

# Generate unique identifiers
test_id = f"{int(datetime.now().timestamp())}_{str(uuid.uuid4())[:8]}"
unique_name = f"test_resource_{test_id}"

# Use in your test
User_Input = f"Create a resource named '{unique_name}' with..."
```

## 🌍 Environment Variables

All DE-Bench tests require these environment variables in your test's README:

```markdown
## Environment Requirements

Set the following environment variables for test isolation:
- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY`: Supabase service role key (for user management)
- `SUPABASE_JWT_SECRET`: JWT secret for token generation
- `ARDENT_BASE_URL`: Your Ardent backend base URL


- `SERVICE_DB` (optional): Database name (defaults to 'test')
```

Add them to the main project's `.env` template in the root README.

## 📝 Test Documentation (README.md)

Create a README for each test with this structure:

```markdown
# Your Test Name

Brief description of what this test validates.

## Test Overview

The test:
1. Step 1 description
2. Step 2 description
3. Step 3 description

## Validation

The test validates:
- ✅ Criterion 1
- ✅ Criterion 2  
- ✅ Criterion 3

## Running the Test

```bash
pytest Tests/Your_Test_Name/test_your_test_name.py -v
```

## Environment Requirements

- `ENV_VAR_1`: Description
- `ENV_VAR_2`: Description

## What This Test Validates

1. **Agent Capability**: Specific capability being tested
2. **Technical Integration**: What systems are integrated
3. **Real-world Scenario**: What real problem this represents
```

## 🚀 Running Tests

```bash
# Run specific test
pytest Tests/Your_Test_Name/test_your_test_name.py -v

# Run by markers
pytest -m "mongodb and database" -v

# Run by difficulty
pytest -m "three" -v

# Run with keywords
pytest -k "your_keyword" -v

# Run all tests
pytest -n auto -sv
```

## ✨ Best Practices

### 1. **Clear User Instructions**
- Write specific, actionable instructions in `User_Input`
- Include expected outputs and naming conventions
- Break complex tasks into numbered steps

### 2. **Robust Validation**
- Implement multiple validation layers
- Check both existence and correctness
- Provide clear success/failure messages

### 3. **Proper Resource Management**
- Always use `try/finally` blocks
- Clean up resources even if test fails
- Use appropriate fixtures for complex resources

### 4. **Test Isolation**
- Use unique identifiers for each test run
- Don't depend on previous test state
- Clean up completely between runs

### 5. **Error Handling**
- Catch and log meaningful errors
- Update `test_steps` with failure reasons
- Provide debugging information

### 6. **Documentation**
- Include a README for each test
- Document environment requirements
- Explain validation criteria clearly

## 📊 Common Patterns

### Database Tests
```python
# Validate data was created
record = collection.find_one({"name": "John Doe"})
assert record is not None, "Record not found"
assert record["age"] == 30, "Age doesn't match"
```

### API Tests  
```python
# Validate API response
response = requests.get(f"{api_url}/endpoint", headers=headers)
assert response.status_code == 200, f"API call failed: {response.status_code}"
data = response.json()
assert "expected_field" in data, "Response missing expected field"
```

### File/Resource Tests
```python
# Validate file/resource exists
assert os.path.exists(expected_file_path), "Expected file not created"
with open(expected_file_path) as f:
    content = f.read()
    assert "expected_content" in content, "File content incorrect"
```

## 🎯 LLM Auto-Generation Guidelines

When using this guide to auto-generate tests, follow these key principles:

### Required Components for Every Test:
1. **Directory Structure**: Create `Tests/TestName/` with required files
2. **Test Markers**: Always include difficulty + technology + category markers
3. **Standard Imports**: Use the exact import pattern shown
4. **Three Sections**: Setup → Run Model → Verify Outcomes
5. **Test Steps Tracking**: Define and update `test_steps` list
6. **Resource Cleanup**: Always use try/finally blocks

### Fixture Selection Decision Tree:

**Step 1: User Isolation (Required for all tests)**
- ✅ **Always use** `supabase_account_resource` with `{"useArdent": True}` to prevent config clashing

**Step 2: Will the test modify/delete data?**
- ✅ **YES** → Use **Per-Test Fixtures** (`mongo_resource`, `airflow_resource`)
- ❌ **NO** → Continue to Step 3

**Step 3: Is resource setup expensive (>10 seconds)?**
- ✅ **YES** → Use **Shared Fixtures** (`shared_resource` with descriptive ID)
- ❌ **NO** → Use **Per-Test Fixtures** for simplicity

**Step 4: Will multiple similar tests benefit from sharing?**
- ✅ **YES** → Use **Shared Fixtures** (`shared_resource` with same ID)
- ❌ **NO** → Use **Per-Test Fixtures**

### Additional Fixture Usage Patterns:

**For Per-Test (Isolated) Resources:**
```python
@pytest.mark.parametrize("mongo_resource", [{
    "resource_id": "unique_test_id",
    # ... configuration
}], indirect=True)
def test_function(request, mongo_resource):
    # Each test gets fresh, isolated resource
```

**For Shared Resources:**
```python
@pytest.mark.parametrize("shared_resource", ["meaningful_shared_id"], indirect=True)
def test_function(request, shared_resource):
    # Multiple tests with same ID share the resource
```

### Naming Conventions:

**Directory Structure:**
- Pattern: `Technology_Agent/Chat_Source_Destination_Task` or `Technology_Agent_Task`
- Examples: 
  - `PostgreSQL_Agent_Add_Record/` (simple task)
  - `MongoDB_Agent_Add_Record/` (simple task)
  - `Airflow_Agent_Simple_Pipeline/` (simple task)
  - `Airflow_Agent_Amazon_SP_API_To_PostgreSQL/` (source → destination)
  - `Airflow_Agent_PostgreSQL_To_MySQL/` (source → destination)
  - `PostgreSQL_Agent_Denormalized_Normalized_ManyToMany/` (schema transformation)
  - `Databricks_Hello_World/` (simple task)

**File Naming:**
- Main test file: `test_` + lowercase directory name with underscores
- Config file: `Test_Configs.py` (exact capitalization)
- Examples:
  - Directory: `PostgreSQL_Agent_Add_Record/` → File: `test_postgresql_agent_add_record.py`
  - Directory: `PostgreSQL_Agent_Denormalized_Normalized_ManyToMany/` → File: `test_postgresql_agent_denormalized_normalized_many_to_many.py`

**Function Naming:**
- Test function: `test_` + lowercase description with underscores
- Examples:
  - `test_postgresql_agent_add_record()`
  - `test_postgresql_agent_denormalized_normalized_many_to_many()`

**Resource and Variable Naming:**
- Resource fixture parameter: Choose based on lifecycle needs:
  - `mongo_resource` (per-test isolation)
  - `postgres_resource` (per-test isolation)
  - `airflow_resource` (per-test isolation)  
  - `shared_resource` (cross-test sharing)
- Shared resource IDs: Use descriptive names like `"read_only_mongo_setup"`, `"shared_test_database"`
- Test steps: Descriptive names that clearly indicate what's being validated

### Configuration Patterns:
- Always use `os.getenv()` for environment variables
- Structure configs under `"services"` key
- Include all necessary connection parameters
- Use meaningful default values where appropriate

### Validation Requirements:
- Implement multiple validation layers (existence, content, functionality)
- Update `test_steps` status and messages
- Use descriptive assertion messages
- Handle exceptions gracefully

### Resource Lifecycle Awareness:
- **Per-Test fixtures**: Can safely modify data, each test starts clean
- **Shared fixtures**: Should only read data or perform non-destructive operations  
- **Isolation fixtures**: Each test gets unique user + API keys to prevent config conflicts, automatic cleanup
- **Resource cleanup**: Per-test fixtures clean automatically; shared fixtures clean at session end; isolation fixtures clean users and backend configs



This guide provides the foundation for creating consistent, reliable tests in the DE-Bench framework. Follow these patterns to ensure your tests integrate seamlessly with the existing test suite and provide meaningful validation of agent capabilities. 