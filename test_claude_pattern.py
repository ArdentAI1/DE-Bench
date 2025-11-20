#!/usr/bin/env python3
"""
Test the exact pattern we use for Claude Code to isolate the hanging issue
"""
import modal
import os

APP_NAME = "test-claude-pattern"
GAR_IMAGE = "us-central1-docker.pkg.dev/ardent-de-bench/de-bench/usercontainersregistryimage:v1"


def test_exact_claude_pattern():
    """Test the exact same pattern we use for Claude Code"""
    print("=" * 60)
    print("TEST: Exact Claude Code pattern")
    print("=" * 60)
    
    # Build env vars exactly like we do for Claude
    env_vars = {
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID_CLAUDE", "fake_key"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY_CLAUDE", "fake_secret"),
        "AWS_REGION": os.environ.get("AWS_REGION_CLAUDE", "us-east-1"),
        "CLAUDE_CODE_USE_BEDROCK": "1",
        "IS_SANDBOX": "1",
    }
    
    # Build script exactly like we do - WITH THE ACTUAL CLAUDE COMMAND
    test_task = "Insert a record into the users table in Snowflake with name='test' and email='test@example.com'"
    task_script = f"""
set -e
echo "Starting Claude Code task..."
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y --no-install-recommends nodejs
npm install -g @anthropic-ai/claude-code
claude --version
echo "Running actual claude command..."
claude -p "{test_task}" --allowedTools all --dangerously-skip-permissions
echo "Task complete!"
"""
    
    # Create image
    image = modal.Image.from_gcp_artifact_registry(
        GAR_IMAGE,
        secret=modal.Secret.from_name("gcp-registry-secret"),
    )
    
    # Lookup or create app
    app = modal.App.lookup(APP_NAME, create_if_missing=True)
    
    # Create sandbox
    print("Creating sandbox...")
    sandbox = modal.Sandbox.create(
        image=image,
        app=app,
        timeout=600,  # 10 minutes
        cpu=1.0,
        memory=2048,
    )
    
    try:
        # Build full script with env vars
        env_exports = " ".join([f'export {k}="{v}";' for k, v in env_vars.items()])
        full_script = env_exports + task_script
        
        print("Executing script...")
        print("Script length:", len(full_script))
        
        # Execute exactly like we do in modal_runner.py
        process = sandbox.exec("bash", "-c", full_script, timeout=600)
        print("Exec called, waiting for completion...")
        
        # Wait for completion
        process.wait()
        print(f"Process completed! Return code: {process.returncode}")
        
        # Read output
        stdout_content = process.stdout.read()
        stderr_content = process.stderr.read()
        
        print("\n" + "=" * 60)
        print("STDOUT:")
        print("=" * 60)
        print(stdout_content[:1000] if len(stdout_content) > 1000 else stdout_content)
        
        if stderr_content:
            print("\n" + "=" * 60)
            print("STDERR:")
            print("=" * 60)
            print(stderr_content[:500] if len(stderr_content) > 500 else stderr_content)
        
        return process.returncode == 0
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        print("Terminating sandbox...")
        sandbox.terminate()
        print("Done!")


if __name__ == "__main__":
    print("Testing exact Claude Code pattern...\n")
    success = test_exact_claude_pattern()
    
    print("\n" + "=" * 60)
    if success:
        print("✅ Test passed!")
    else:
        print("❌ Test failed!")

