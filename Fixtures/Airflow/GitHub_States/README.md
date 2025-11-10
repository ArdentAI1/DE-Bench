# GitHub State Archives

## What Are State Archives?

GitHub state archives are `.zip` files containing complete snapshots of a GitHub repository, including:
- Application source code and configuration
- Complete Git history (`.git/` directory)
- Directory structure with `.gitkeep` files
- Optional GitHub workflows (`.github/` directory)

These archives are used to initialize test branches with specific pre-existing content, enabling tests to start from a known state rather than an empty repository.

## Available State Archives

### 1. empty-state.zip
**Purpose:** Blank repository for testing AI agent creation from scratch

**Use Case:** Tests where the AI must create all resources (DAGs, configurations, etc.) from nothing

**Contents:**
- Basic repository structure
- Empty directories maintained with `.gitkeep`
- Minimal or empty `requirements.txt`
- Git history with initial commit

**Example Test:** `Tests/Airflow_Agent_Hello_Universe_Pipeline/`

### 2. hello-world-failure-state.zip
**Purpose:** Repository with intentionally broken code for remediation testing

**Use Case:** Tests where the AI must debug and fix failing code

**Contents:**
- `dags/hello_world.py` - DAG with intentional failure
- `Dockerfile` - Container configuration
- `airflow_init.sh` - Airflow initialization script
- Git history with the broken state

**Intentional Failure:**
```python
fail_task = BashOperator(
    task_id="intentional_failure",
    bash_command="cat /nonexistent/file.txt",  # This will fail
    dag=dag
)
```

**Example Test:** `Tests/Airflow_Agent_Failed_Hello_World/`

## How State Archives Work

When a test uses a state archive:

1. **Branch Creation** - A new test branch is created in the GitHub repository
2. **Archive Extraction** - The `.zip` file is extracted to a temporary directory
3. **File Upload** - Files are pushed to GitHub via the API
   - `.git/` and `.github/` directories are **skipped** (GitHub manages these internally)
   - All other files are created or updated on the branch
4. **Cleanup** - Temporary directory is removed

## Creating a New State Archive

### Step 1: Prepare the Repository

1. Clone or create a repository with the desired state:
   ```bash
   git clone <repo-url> my-test-state
   cd my-test-state
   ```

2. Add/modify files for your test scenario:
   ```bash
   # For empty state: keep minimal files
   # For failure state: add intentionally broken code
   ```

3. Commit all changes:
   ```bash
   git add .
   git commit -m "Create test state"
   ```

### Step 2: Create the Archive

Create a `.zip` file including the `.git` directory:

```bash
zip -r my-new-state.zip . -x "*.pyc" -x "__pycache__/*" -x ".DS_Store"
```

**Must Include:**
- All source files and directories
- Complete `.git/` directory (for git history)
- `.gitkeep` files (to preserve empty directories)
- `.github/` directory (if needed for workflows)

### Step 3: Validate the Archive

Check the contents:
```bash
unzip -l my-new-state.zip | grep -E "^(.git|dags|Requirements)"
```

Test extraction:
```bash
mkdir test-extract
cd test-extract
unzip ../my-new-state.zip
git log --oneline  # Verify git history
ls -la             # Verify files
```

### Step 4: Place in Repository

Move the archive to this directory:
```bash
mv my-new-state.zip /path/to/DE-Bench/Fixtures/Airflow/GitHub_States/
```

### Step 5: Use in Tests

Reference the archive in your test's `get_fixtures()` function:

```python
custom_github_config = {
    "resource_id": f"test_airflow_{resource_id}",
    "state_archive_path": f"{root_dir}/Fixtures/Airflow/GitHub_States/my-new-state.zip",
}

github_fixture = GitHubFixture(custom_config=custom_github_config)
```

## Best Practices

### Archive Size
- **Keep small**: Only include necessary files
- **Minimize git history**: Use `git gc --aggressive --prune=all`
- **Exclude build artifacts**: `.pyc`, `__pycache__`, `node_modules/`, etc.

### Documentation
- **Name descriptively**: e.g., `pandas-with-errors-state.zip`
- **Comment failures**: Explain why code is broken (for failure states)
- **Document intent**: Add comments in test code explaining the archive purpose

### Testing
- **Validate archives**: Test extraction and git history before using
- **Version control**: Commit archives to git for reproducibility
- **Verify structure**: Ensure `.git/` directory is complete

## Archive Structure Example

```
my-state.zip
├── .git/                      # Complete git repository (INCLUDED in zip)
│   ├── objects/               # Git objects and history
│   ├── refs/                  # Branches and tags
│   ├── logs/                  # Git logs
│   ├── config                 # Git configuration
│   └── HEAD                   # Current branch pointer
├── .github/                   # GitHub workflows (INCLUDED in zip)
│   └── workflows/
│       └── ci.yml
├── dags/                      # Application code
│   ├── .gitkeep               # Preserves empty directory
│   └── my_dag.py
├── Requirements/
│   └── requirements.txt
├── requirements.txt
├── Dockerfile
└── README.md
```

**Important:** While `.git/` and `.github/` are **included** in the zip archive, they are **automatically skipped** when pushing files to GitHub (see `unpack_and_push_state_file()` in `GitHubManager`).

## Troubleshooting

### Archive Too Large
**Problem:** Zip file is several MB or larger

**Solutions:**
- Remove unnecessary git history: `git gc --aggressive --prune=all`
- Use shallow clone: `git clone --depth 1 <repo>`
- Exclude build artifacts in zip command: `zip -r state.zip . -x "*.pyc" -x "__pycache__/*"`

### Files Not Appearing on GitHub
**Problem:** Files from archive don't show up on the test branch

**Solutions:**
- Verify archive contains files: `unzip -l state.zip`
- Check file paths (must use forward slashes)
- Ensure `.git/` and `.github/` are **not** being pushed as regular files (they're auto-skipped)
- Review GitHub API errors in test logs

### Git History Issues
**Problem:** Git history is broken or incomplete

**Solutions:**
- Ensure `.git/` directory is complete in archive
- Verify `HEAD` points to valid branch: `cat .git/HEAD`
- Check refs exist: `ls .git/refs/heads/`
- Test locally: Extract archive and run `git log`

### Empty Directories Lost
**Problem:** Empty directories disappear

**Solution:** Add `.gitkeep` files to empty directories before creating archive:
```bash
find . -type d -empty -exec touch {}/.gitkeep \;
```

## Related Documentation

- **Full Documentation:** See `Fixtures/GitHub/github_manager.py` module docstring
- **Example Tests:**
  - `Tests/Airflow_Agent_Failed_Hello_World/test_airflow_agent_failed_hello_world_pipeline.py`
  - `Tests/Airflow_Agent_Hello_Universe_Pipeline/test_airflow_agent_hello_universe_pipeline.py`
- **GitHubManager Code:** `Fixtures/GitHub/github_manager.py:84-153` (see `unpack_and_push_state_file()`)
