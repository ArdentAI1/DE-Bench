#!/usr/bin/env python3
"""
Test if we need to read output while the process is running
"""
import modal
import os
import time

APP_NAME = "test-streaming"
GAR_IMAGE = "us-central1-docker.pkg.dev/ardent-de-bench/de-bench/usercontainersregistryimage:v1"


def test_streaming_approach():
    """Test reading output while process runs"""
    print("=" * 60)
    print("TEST: Streaming output approach")
    print("=" * 60)
    
    # Simple script that produces output over time
    task_script = """
set -e
echo "Starting..."
sleep 2
echo "Step 1 complete"
sleep 2
echo "Step 2 complete"
sleep 2
echo "Done!"
"""
    
    image = modal.Image.from_gcp_artifact_registry(
        GAR_IMAGE,
        secret=modal.Secret.from_name("gcp-registry-secret"),
    )
    
    app = modal.App.lookup(APP_NAME, create_if_missing=True)
    
    print("Creating sandbox...")
    sandbox = modal.Sandbox.create(
        image=image,
        app=app,
        timeout=60,
        cpu=1.0,
        memory=2048,
    )
    
    try:
        print("Executing script...")
        process = sandbox.exec("bash", "-c", task_script, timeout=60)
        
        print("Reading output in real-time...")
        # Try reading output line by line while it runs
        for line in process.stdout:
            print(f"STDOUT: {line}")
        
        print("Waiting for completion...")
        process.wait()
        print(f"Process completed! Return code: {process.returncode}")
        
        # Read any remaining output
        remaining = process.stdout.read()
        if remaining:
            print(f"Remaining output: {remaining}")
        
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
    print("Testing streaming output...\n")
    success = test_streaming_approach()
    
    print("\n" + "=" * 60)
    if success:
        print("✅ Test passed!")
    else:
        print("❌ Test failed!")

