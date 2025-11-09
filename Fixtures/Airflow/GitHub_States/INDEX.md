# GitHub State Archives - Documentation Index

This directory contains GitHub state archives (`.zip` files) and their documentation.

## Quick Links

| Document | Purpose | When to Use |
|----------|---------|-------------|
| **[README.md](README.md)** | Main reference guide | First time learning or quick lookup |
| **[EXAMPLE_hello_world_failure.md](EXAMPLE_hello_world_failure.md)** | Detailed example with actual file contents | Creating similar archives or understanding structure |
| **[github_manager.py](../../GitHub/github_manager.py)** (lines 1-186) | Complete technical documentation | Deep understanding of implementation |

## Available Archives

| Archive | Size | Purpose | Example Test |
|---------|------|---------|--------------|
| **empty-state.zip** | 908 KB | Blank repository for creation tests | [Airflow_Agent_Hello_Universe_Pipeline](../../../Tests/Airflow_Agent_Hello_Universe_Pipeline/) |
| **hello-world-failure-state.zip** | 698 KB | Repository with broken DAG for fixing tests | [Airflow_Agent_Failed_Hello_World](../../../Tests/Airflow_Agent_Failed_Hello_World/) |

## Documentation Structure

```
GitHub_States/
├── INDEX.md                              ← You are here (navigation)
├── README.md                             ← Quick reference guide
├── EXAMPLE_hello_world_failure.md        ← Concrete example with file contents
├── empty-state.zip                       ← Empty state archive
└── hello-world-failure-state.zip         ← Failure state archive

Related Documentation:
├── ../../GitHub/github_manager.py        ← Module docstring (lines 1-186)
└── ../../../Tests/                       ← Example test implementations
    ├── Airflow_Agent_Failed_Hello_World/
    └── Airflow_Agent_Hello_Universe_Pipeline/
```

## Common Tasks

### I want to...

**...understand what state archives are**
→ Start with [README.md](README.md) - "What Are State Archives?" section

**...see what's inside an archive**
→ Read [EXAMPLE_hello_world_failure.md](EXAMPLE_hello_world_failure.md)

**...create a new state archive**
→ Follow [README.md](README.md) - "Creating a New State Archive" section
→ Use [EXAMPLE_hello_world_failure.md](EXAMPLE_hello_world_failure.md) as template

**...use an existing archive in a test**
→ Check [README.md](README.md) - "Using State Archives in Tests" section
→ See example tests in `Tests/` directory

**...understand how archives are processed**
→ Read [github_manager.py](../../GitHub/github_manager.py) docstring
→ See `unpack_and_push_state_file()` method (lines 84-153)

**...troubleshoot an archive issue**
→ Check [README.md](README.md) - "Troubleshooting" section
→ Validate archive structure with commands provided

**...know which archive to use**
→ See "Available Archives" table above
→ Read [README.md](README.md) - "Available State Archives" section

## Quick Commands

### Inspect an Archive
```bash
# List contents
unzip -l hello-world-failure-state.zip | head -20

# Extract to examine
mkdir /tmp/test && cd /tmp/test
unzip path/to/hello-world-failure-state.zip
```

### Create New Archive
```bash
# From repository root
zip -r my-state.zip . -x "*.pyc" -x "__pycache__/*" -x ".DS_Store"

# Verify
unzip -t my-state.zip
```

### Use in Test
```python
custom_github_config = {
    "resource_id": f"test_{resource_id}",
    "state_archive_path": f"{root_dir}/Fixtures/Airflow/GitHub_States/my-state.zip",
}
github_fixture = GitHubFixture(custom_config=custom_github_config)
```

## Document Summaries

### README.md (6.9 KB)
**Sections:**
- What Are State Archives? (overview)
- Available State Archives (descriptions of empty-state.zip and hello-world-failure-state.zip)
- How State Archives Work (processing steps)
- Creating a New State Archive (step-by-step guide)
- Best Practices (organized by topic)
- Troubleshooting (common issues and solutions)

**Best For:** Quick reference, step-by-step instructions

### EXAMPLE_hello_world_failure.md (8.2 KB)
**Sections:**
- Archive Purpose
- Complete File Structure (tree view)
- Key File Contents (actual code from archive)
- Git History
- Test Usage
- Expected AI Behavior
- Creating Similar Archives

**Best For:** Learning by example, template for new archives

### github_manager.py Docstring (lines 1-186)
**Sections:**
- Overview
- Purpose (empty state vs. failure remediation)
- State Archive Structure
- Creating a State Archive (3 steps)
- Using State Archives in Tests
- Available State Archives
- Best Practices
- Troubleshooting
- Example Test Usage

**Best For:** Complete technical reference, implementation details

## Key Concepts

### State Archives Are...
- `.zip` files with complete repository snapshots
- Include source code + git history (`.git/` directory)
- Used to initialize test branches with known content
- Two types: empty (creation) or failure (remediation)

### Important Notes
- ✅ `.git/` and `.github/` ARE in the zip archive
- ✅ They are EXCLUDED when pushing to GitHub (auto-skipped)
- ✅ GitHub manages these directories internally
- ✅ Archives include full git history and branches

### Common Workflow
1. Create/clone repository with desired state
2. Commit all changes (capture git history)
3. Create zip including `.git/` directory
4. Place in `GitHub_States/` directory
5. Reference in test's `custom_github_config`
6. Test framework unpacks and pushes files to new branch

## Getting Help

**General questions:** Read [README.md](README.md)

**Technical details:** Check [github_manager.py](../../GitHub/github_manager.py) docstring

**Examples needed:** See [EXAMPLE_hello_world_failure.md](EXAMPLE_hello_world_failure.md)

**Code implementation:** Review test files:
- `Tests/Airflow_Agent_Failed_Hello_World/test_airflow_agent_failed_hello_world_pipeline.py`
- `Tests/Airflow_Agent_Hello_Universe_Pipeline/test_airflow_agent_hello_universe_pipeline.py`

## Contributing

When creating new state archives:

1. ✅ Follow naming convention: `{purpose}-state.zip`
2. ✅ Document the archive in README.md
3. ✅ Include git history but keep it minimal
4. ✅ Test extraction and usage before committing
5. ✅ Add example documentation if complex
6. ✅ Keep archives small (< 1 MB if possible)

## Archive Validation Checklist

Before using a new archive:

- [ ] Archive is valid zip: `unzip -t archive.zip`
- [ ] Contains `.git/` directory
- [ ] Git history is valid: Extract and run `git log`
- [ ] Required files are present
- [ ] `.gitkeep` files in empty directories
- [ ] No build artifacts (`.pyc`, `__pycache__/`, etc.)
- [ ] Size is reasonable (< 1 MB preferred)
- [ ] Documented in README.md

---

**Last Updated:** 2025-11-07

**Related Modules:**
- `Fixtures.GitHub.github_manager` - Core implementation
- `Fixtures.GitHub.github_fixture` - Test fixture integration
- `Fixtures.Airflow.airflow_fixture` - Airflow test integration
