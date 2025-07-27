# GitHub Fixture Documentation

## Overview

The GitHub fixture provides a class-based approach to manage GitHub operations in Airflow tests. It encapsulates common GitHub operations like repository setup, branch management, PR operations, and cleanup.

## Features

- **Repository Setup**: Automatically clears the dags folder and ensures .gitkeep exists
- **Branch Verification**: Check if branches exist and update test steps
- **PR Management**: Find and merge pull requests with customizable options
- **Cleanup Operations**: Reset repository state and clean up branches
- **Requirements Management**: Reset requirements.txt files
- **Error Handling**: Comprehensive error handling with detailed logging

## Installation

The fixture requires the following environment variables:

```bash
export AIRFLOW_GITHUB_TOKEN="your_github_token"
export AIRFLOW_REPO="https://github.com/owner/repo"
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

## Troubleshooting

### Common Issues

1. **Repository not found**: Check `AIRFLOW_REPO` environment variable
2. **Authentication failed**: Verify `AIRFLOW_GITHUB_TOKEN` is valid
3. **Branch not found**: Ensure the branch name matches exactly
4. **PR not found**: Check the PR title matches exactly

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