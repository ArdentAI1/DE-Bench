# GitHub Fixture Documentation

## Overview

The GitHub fixture provides a class-based approach to manage GitHub operations in Airflow tests. It encapsulates common GitHub operations like repository setup, branch management, PR operations, and cleanup.

## Features

- **State Archives**: Automatically unpack and push ZIP archives to initialize repository state
- **Repository Setup**: Automatically clears the dags folder and ensures .gitkeep exists
- **Branch Verification**: Check if branches exist and update test steps
- **PR Management**: Find and merge pull requests with customizable options
- **Cleanup Operations**: Reset repository state and clean up branches
- **Requirements Management**: Reset requirements.txt files
- **Error Handling**: Comprehensive error handling with detailed logging
- **Smart Path Handling**: Automatically filters `.git/` and `.github/` directories and normalizes paths for cross-platform compatibility

## Installation

The fixture requires the following environment variables:

```bash
export AIRFLOW_GITHUB_TOKEN="your_github_token"
export AIRFLOW_REPO="https://github.com/owner/repo"
```

## State Archives

### Overview

The GitHub fixture supports **state archives** - ZIP files containing a snapshot of repository contents that can be automatically unpacked and pushed to a test branch. This allows you to set up complex initial repository states for testing without manually creating files.

### What are State Archives?

State archives are ZIP files that contain the initial file structure you want in your test branch. They typically include:

- `dags/` - Directory for Airflow DAG files
- `Requirements/requirements.txt` - Python dependencies
- `requirements.txt` - Root-level requirements file
- `.gitkeep` files to preserve empty directories

**Note**: The archive can contain `.git/` and `.github/` directories, but these will be automatically filtered out during extraction since GitHub manages these internally.

### Using State Archives in Tests

State archives are specified in the fixture configuration using the `state_archive_path` parameter:

```python
from Fixtures.GitHub.github_fixture import GitHubFixture

# Define custom configuration with state archive
custom_github_config = {
    "resource_id": f"test_my_feature_{test_timestamp}_{test_uuid}",
    "state_archive_path": f"{root_dir}/Fixtures/Airflow/GitHub_States/empty-state.zip",
}

# Create fixture with state archive
github_fixture = GitHubFixture(custom_config=custom_github_config)
```

When the fixture is initialized:
1. A new test branch is created
2. The state archive is extracted locally
3. Files are pushed to the test branch (excluding `.git/` and `.github/`)
4. The local extraction directory is cleaned up
5. Your test branch now has the initial state ready

### Example: Empty State Archive

The `empty-state.zip` provides a minimal repository structure:

```
empty-state.zip
├── dags/
│   └── .gitkeep              # Preserves empty dags directory
├── Requirements/
│   └── requirements.txt      # Empty requirements file
└── requirements.txt          # Empty root requirements file
```

This "empty" state is ideal for tests where the AI agent needs to create files from scratch.

### Real-World Example

Here's how the `test_airflow_agent_hello_universe_pipeline.py` test uses state archives:

```python
import os
import time
import uuid
import importlib

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]

def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    """
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Initialize GitHub fixture with empty state archive
    custom_github_config = {
        "resource_id": f"test_airflow_hello_universe_pipeline_test_{test_timestamp}_{test_uuid}",
        "state_archive_path": f"{root_dir}/Fixtures/Airflow/GitHub_States/empty-state.zip",
    }

    github_fixture = GitHubFixture(custom_config=custom_github_config)

    return [github_fixture]
```

**What happens:**

1. **Branch Creation**: A test branch like `test_airflow_hello_universe_pipeline_test_1761285201_dc7591cc` is created
2. **State Extraction**: The `empty-state.zip` is extracted to a temporary directory
3. **File Push**: Contents are pushed to the test branch:
   - `dags/.gitkeep` → Creates empty dags directory
   - `Requirements/requirements.txt` → Creates empty requirements file
   - `requirements.txt` → Creates root requirements file
4. **Cleanup**: The temporary extraction directory is deleted
5. **Ready for Test**: The test branch now has the initial structure, and the AI agent can add DAG files

### Creating Custom State Archives

To create your own state archives:

1. **Set up a local directory** with the desired structure:
   ```bash
   mkdir my-state
   cd my-state
   mkdir -p dags Requirements
   touch dags/.gitkeep
   touch Requirements/requirements.txt
   touch requirements.txt
   # Add any other files you need...
   ```

2. **Create the ZIP archive**:
   ```bash
   zip -r my-state.zip .
   ```

3. **Place it in the GitHub_States directory**:
   ```bash
   mv my-state.zip /path/to/DE-Bench/Fixtures/Airflow/GitHub_States/
   ```

4. **Use it in your test**:
   ```python
   custom_github_config = {
       "resource_id": f"test_my_feature_{test_timestamp}_{test_uuid}",
       "state_archive_path": f"{root_dir}/Fixtures/Airflow/GitHub_States/my-state.zip",
   }
   ```

### State Archive Best Practices

1. **Keep archives minimal**: Only include necessary files for your test scenario
2. **Use .gitkeep for empty directories**: Ensures directories are preserved
3. **Don't include .git/**: These are filtered automatically but add unnecessary size
4. **Use descriptive names**: Name archives by their purpose (e.g., `empty-state.zip`, `with-existing-dags.zip`)
5. **Version control your archives**: Commit them to your repository so tests are reproducible
6. **Document contents**: Add a README or comment explaining what's in each archive

### When to Use State Archives

**Use state archives when:**
- Testing AI agents that modify existing code
- Setting up complex initial repository structures
- Ensuring consistent starting points across test runs
- Testing scenarios with specific file dependencies

**Don't use state archives when:**
- Starting from a truly empty repository (just use the fixture without `state_archive_path`)
- Testing cleanup operations (you want a dirty state, not a prepared one)
- Files can be easily created programmatically in the test

### Technical Implementation

The `unpack_and_push_state_file()` method in `GitHubManager` handles state archive processing:

```python
def unpack_and_push_state_file(self, state_archive_path: Union[str, Path]) -> None:
    """
    Unpack the state file and push it to the new branch

    :param Union[str, Path] state_archive_path: Path to the state archive file
    :rtype: None
    """
```

**Processing steps:**

1. **Validation**: Verifies the archive exists, is a file, and has `.zip` extension
2. **Extraction**: Unpacks to a temporary directory named `{branch_name}/state_file`
3. **File Iteration**: Walks through extracted files, computing relative paths
4. **GitHub API Calls**: For each file:
   - Converts local paths to GitHub-compatible paths (forward slashes)
   - Filters out `.git/` and `.github/` directories
   - Checks if file exists on branch (updates if exists, creates if not)
5. **Cleanup**: Removes temporary extraction directory

**Key features:**

- **Path normalization**: Converts OS-specific paths to forward slashes for GitHub compatibility
- **Smart filtering**: Automatically skips `.git/` and `.github/` directories that can't be created via API
- **Idempotent operations**: Handles cases where files already exist (updates instead of failing)
- **Error handling**: Provides clear error messages for missing files, invalid archives, or API failures

**Example log output:**

```
✅ Created branch: test_airflow_hello_universe_pipeline_test_1761285201_dc7591cc
Created new file: dags/.gitkeep
Created new file: Requirements/requirements.txt
Created new file: requirements.txt
Skipping .git/config (GitHub-managed directory)
Skipping .github/workflows/deploy.yml (GitHub-managed directory)
✅ State file unpacked empty-state.zip and pushed to test_airflow_hello_universe_pipeline_test_1761285201_dc7591cc branch
```

## Basic Usage

### Using the Fixture

```python
import pytest

@pytest.mark.github
def test_with_github_fixture(request, github_resource):
    # Access the GitHub manager
    github_manager = github_resource["github_manager"]

    # Repository is already set up with clean dags folder
    print(f"Repository: {github_manager.repo_name}")

    # Your test logic here...
```

### Using with Airflow Fixture

```python
@pytest.mark.airflow
@pytest.mark.github
def test_airflow_and_github(request, airflow_resource, github_resource):
    # Access both fixtures
    airflow_base_url = airflow_resource["base_url"]
    github_manager = github_resource["github_manager"]
    
    # Your test logic here...
```

## GitHubManager Class Methods

### Core Methods

#### `setup_dags_folder()`
Clears the dags folder and ensures .gitkeep exists.

```python
github_manager.setup_dags_folder()
```

#### `unpack_and_push_state_file(state_archive_path)`
Unpacks a ZIP archive and pushes its contents to the test branch. Automatically filters out `.git/` and `.github/` directories.

```python
from pathlib import Path

# Unpack and push state archive
state_path = Path("/path/to/empty-state.zip")
github_manager.unpack_and_push_state_file(state_path)
```

**Note**: This method is typically called automatically during fixture initialization if `state_archive_path` is provided in the configuration. Manual usage is rare.

#### `verify_branch_exists(branch_name, test_step)`
Verify that a branch exists and update test step.

```python
test_step = {"name": "Branch Check", "status": "did not reach", "Result_Message": ""}

branch_exists, test_step = github_manager.verify_branch_exists("feature/my-branch", test_step)
if not branch_exists:
    raise Exception(test_step["Result_Message"])
```

#### `find_and_merge_pr(pr_title, test_step, commit_title=None, merge_method="squash")`
Find a PR by title and merge it, updating test step.

```python
pr_exists, test_step = github_manager.find_and_merge_pr(
    pr_title="Add My Feature", 
    test_step=test_step, 
    commit_title="Add My Feature",
    merge_method="squash"
)
if not pr_exists:
    raise Exception(test_step["Result_Message"])
```

#### `delete_branch(branch_name)`
Delete a branch if it exists.

```python
github_manager.delete_branch("feature/my-branch")
```

#### `reset_repo_state(folder_name, keep_file_names=None)`
Reset the repository to a clean state by clearing a folder.

```python
github_manager.reset_repo_state("dags")
```

#### `check_if_action_is_complete(pr_title, wait_before_checking=60, max_retries=10, branch_name=None)`
Check if a GitHub action is complete.

```python
if not github_manager.check_if_action_is_complete(pr_title="Add My Feature"):
    raise Exception("Action is not complete")
```

#### `cleanup_requirements(requirements_path="Requirements/")`
Reset requirements.txt to blank.

```python
github_manager.cleanup_requirements()
```

#### `get_repo_info()`
Get repository information.

```python
repo_info = github_manager.get_repo_info()
print(f"Repo: {repo_info['repo_name']}")
print(f"URL: {repo_info['repo_url']}")
print(f"Default branch: {repo_info['default_branch']}")
```

## Complete Example

Here's a complete example showing how to use the GitHub fixture in a typical Airflow test:

```python
import pytest
import time

@pytest.mark.airflow
@pytest.mark.pipeline
def test_airflow_pipeline_with_github(request, airflow_resource, github_resource):
    """
    Example test showing how to use both Airflow and GitHub fixtures.
    """
    # Get fixtures
    github_manager = github_resource["github_manager"]
    dag_name = "hello_universe_dag"
    pr_title = "Add Hello Universe DAG"
    
    # Setup test steps
    test_steps = [
        {
            "name": "Checking Git Branch Existence",
            "description": "Checking if the git branch exists with the right name",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking PR Creation",
            "description": "Checking if the PR was created with the right name",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking DAG Results",
            "description": "Checking if the DAG produces the expected results",
            "status": "did not reach",
            "Result_Message": "",
        },
    ]
    
    request.node.user_properties.append(("test_steps", test_steps))
    
    try:
        # Your test setup here...
        
        # Run your model/pipeline here...
        
        # Verify branch creation
        branch_exists, test_steps[0] = github_manager.verify_branch_exists("feature/hello_universe_dag", test_steps[0])
        if not branch_exists:
            raise Exception(test_steps[0]["Result_Message"])
        
        # Verify PR creation and merge
        pr_exists, test_steps[1] = github_manager.find_and_merge_pr(
            pr_title=pr_title, 
            test_step=test_steps[1], 
            commit_title=pr_title, 
            merge_method="squash"
        )
        if not pr_exists:
            raise Exception(test_steps[1]["Result_Message"])
        
        # Check if GitHub action is complete
        if not github_manager.check_if_action_is_complete(pr_title=pr_title):
            raise Exception("Action is not complete")
        
        # Your verification logic here...
        
        print("✓ Test completed successfully!")
        
    finally:
        # Cleanup using the manager
        github_manager.delete_branch("feature/hello_universe_dag")
```

## Migration from Manual GitHub Operations

### Before (Manual Operations)

```python
# Setup GitHub repository
access_token = os.getenv("AIRFLOW_GITHUB_TOKEN")
airflow_github_repo = os.getenv("AIRFLOW_REPO")

if "github.com" in airflow_github_repo:
    parts = airflow_github_repo.split("/")
    airflow_github_repo = f"{parts[-2]}/{parts[-1]}"

g = Github(access_token)
repo = g.get_repo(airflow_github_repo)

# Clear dags folder
dags_contents = repo.get_contents("dags")
for content in dags_contents:
    if content.name != ".gitkeep":
        repo.delete_file(
            path=content.path,
            message="Clear dags folder",
            sha=content.sha,
            branch="main",
        )

# Ensure .gitkeep exists
try:
    repo.get_contents("dags/.gitkeep")
except:
    repo.create_file(
        path="dags/.gitkeep",
        message="Add .gitkeep to dags folder",
        content="",
        branch="main",
    )

# Check branch exists
try:
    branch = repo.get_branch("feature/my-branch")
    test_steps[0]["status"] = "passed"
    test_steps[0]["Result_Message"] = "Branch was created successfully"
except Exception as e:
    test_steps[0]["status"] = "failed"
    test_steps[0]["Result_Message"] = f"Branch was not created: {str(e)}"
    raise Exception(f"Branch was not created: {str(e)}")

# Find and merge PR
pulls = repo.get_pulls(state="open")
target_pr = None
for pr in pulls:
    if pr.title == "Add My Feature":
        target_pr = pr
        test_steps[1]["status"] = "passed"
        test_steps[1]["Result_Message"] = "PR was created successfully"
        break

if not target_pr:
    test_steps[1]["status"] = "failed"
    test_steps[1]["Result_Message"] = "PR not found"
    raise Exception("PR not found")

merge_result = target_pr.merge(
    commit_title="Add My Feature", merge_method="squash"
)

if not merge_result.merged:
    raise Exception(f"Failed to merge PR: {merge_result.message}")

# Cleanup
try:
    ref = repo.get_git_ref(f"heads/feature/my-branch")
    ref.delete()
except Exception as e:
    print(f"Branch might not exist or other error: {e}")

# Reset repo state
dags_contents = repo.get_contents("dags")
for content in dags_contents:
    if content.name != ".gitkeep":
        repo.delete_file(
            path=content.path,
            message="Clear dags folder",
            sha=content.sha,
            branch="main",
        )

try:
    repo.get_contents("dags/.gitkeep")
except:
    repo.create_file(
        path="dags/.gitkeep",
        message="Add .gitkeep to dags folder",
        content="",
        branch="main",
    )
```

### After (Using GitHub Fixture)

```python
# Get GitHub manager from fixture
github_manager = github_resource["github_manager"]

# Repository is already set up by fixture
print("GitHub repository setup completed by fixture")

# Verify branch and merge PR
branch_exists, test_steps[0] = github_manager.verify_branch_exists("feature/my-branch", test_steps[0])
if not branch_exists:
    raise Exception(test_steps[0]["Result_Message"])

pr_exists, test_steps[1] = github_manager.find_and_merge_pr(
    pr_title="Add My Feature", 
    test_step=test_steps[1], 
    commit_title="Add My Feature",
    merge_method="squash"
)
if not pr_exists:
    raise Exception(test_steps[1]["Result_Message"])

# Check if GitHub action is complete
if not github_manager.check_if_action_is_complete(pr_title="Add My Feature"):
    raise Exception("Action is not complete")

# Cleanup
github_manager.delete_branch("feature/my-branch")
```

## Benefits

1. **Reduced Code Duplication**: Common GitHub operations are centralized
2. **Better Error Handling**: Consistent error handling across all tests
3. **Easier Maintenance**: Changes to GitHub operations only need to be made in one place
4. **Improved Readability**: Test code is cleaner and more focused on business logic
5. **Automatic Setup**: Repository setup is handled automatically by the fixture
6. **Consistent Cleanup**: Standardized cleanup procedures

## Error Handling

The fixture provides comprehensive error handling:

- **Repository Access**: Validates repository access and provides clear error messages
- **Branch Operations**: Handles cases where branches don't exist
- **PR Operations**: Manages PR not found scenarios and merge failures
- **File Operations**: Handles file deletion and creation errors gracefully

## Logging

The fixture provides detailed logging for debugging:

- Repository setup progress
- Branch verification results
- PR search and merge status
- Cleanup operations
- Error details with context

## Best Practices

1. **Always use the fixture**: Don't manually create GitHub clients in tests
2. **Use test steps**: Pass individual test_step dictionaries to methods that update them
3. **Handle return values**: Methods return tuples with success status and updated test_step
4. **Clean up properly**: Always call cleanup methods in finally blocks
5. **Check action completion**: Use `check_if_action_is_complete()` to wait for GitHub actions
6. **Use descriptive names**: Use meaningful branch and PR names for debugging
7. **Define variables early**: Set up dag_name and pr_title variables at the start of your test
8. **Use state archives for complex setups**: Leverage state archives instead of creating files programmatically when testing with pre-existing repository structures
9. **Keep state archives in version control**: Commit state ZIP files to ensure reproducible tests across environments

## Troubleshooting

### Common Issues

1. **Repository not found**: Check `AIRFLOW_REPO` environment variable
2. **Authentication failed**: Verify `AIRFLOW_GITHUB_TOKEN` is valid
3. **Branch not found**: Ensure the branch name matches exactly
4. **PR not found**: Check the PR title matches exactly

### State Archive Issues

#### Error: "path contains a malformed path component"

**Cause**: Attempting to create files in `.git/` or `.github/` directories via GitHub API.

**Solution**: The fixture automatically filters these directories. If you see this error, ensure you're using the latest version of the fixture code that includes the filtering logic.

**Code fix** (already implemented in `github_manager.py:110-111`):
```python
# Skip .git and .github directories (GitHub manages these internally)
dirs[:] = [d for d in dirs if d not in ['.git', '.github']]
```

#### Error: "sha wasn't supplied"

**Cause**: Attempting to create a file that already exists on the branch.

**Solution**: The fixture automatically handles this by checking if files exist before creating them. If you see this error, ensure you're using the latest version that includes the update/create logic.

**Code fix** (already implemented in `github_manager.py:127-150`):
```python
# Check if file already exists on the branch
try:
    existing_file = self.repo.get_contents(github_path, ref=self.branch_name)
    # File exists, update it
    self.repo.update_file(...)
except github.GithubException as e:
    if e.status == 404:
        # File doesn't exist, create it
        self.repo.create_file(...)
```

#### Error: "State file does not exist"

**Cause**: The `state_archive_path` points to a non-existent file.

**Solutions**:
- Verify the path is correct and uses absolute paths or correct relative paths
- Check that the ZIP file exists in the `Fixtures/Airflow/GitHub_States/` directory
- Ensure the file has a `.zip` extension

**Example of correct path**:
```python
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
state_archive_path = f"{root_dir}/Fixtures/Airflow/GitHub_States/empty-state.zip"
```

#### State Archive Not Pushing Files

**Symptoms**: Branch is created but files from state archive don't appear.

**Debugging steps**:
1. Check the ZIP file contents:
   ```bash
   unzip -l /path/to/state-archive.zip
   ```

2. Look for log output indicating which files were pushed:
   ```
   Created new file: dags/.gitkeep
   Updated existing file: requirements.txt
   ```

3. Verify files aren't being filtered:
   - Files in `.git/` and `.github/` are intentionally skipped
   - Check the ZIP doesn't have an unexpected directory structure

4. Check GitHub API rate limits (rare but possible in intensive testing)

### Debug Mode

Enable debug logging by setting the log level:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

When adding new GitHub operations:

1. Add the method to the `GitHubManager` class
2. Include proper error handling
3. Add logging for debugging
4. Update this documentation
5. Add tests for the new functionality 