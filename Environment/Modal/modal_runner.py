import os
import subprocess
from typing import Any, Dict
import modal
import uuid

from dotenv import load_dotenv
load_dotenv()


GAR_IMAGE = "us-central1-docker.pkg.dev/ardent-de-bench/de-bench/agent-sqltools-py311:agent-local"


def run_modal_task(command: str, mode: str):
    """Run an agent task on Modal with dynamic mode-based configuration."""
    
    if mode == "Claude_Code":
        settings_object = {
            "secrets": {
                "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID_CLAUDE"),
                "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY_CLAUDE"),
                "AWS_REGION": os.getenv("AWS_REGION_CLAUDE"),
                "CLAUDE_CODE_USE_BEDROCK": "1",
                "IS_SANDBOX": "1",
            },
            "task_script": f"""
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y --no-install-recommends nodejs
npm install -g @anthropic-ai/claude-code
claude --version
claude -p "{command}" --allowedTools all --dangerously-skip-permissions
"""
        }
    elif mode == "OpenAI_Codex":
        settings_object = {
            "secrets": {
                "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
                "IS_SANDBOX": "1",
            },
            "task_script": f"""
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y --no-install-recommends nodejs
npm install -g @openai/codex@0.29.0
codex --help >/dev/null
codex exec --dangerously-bypass-approvals-and-sandbox --skip-git-repo-check "{command}"
"""
        }
    else:
        raise ValueError(f"Unknown mode: {mode}")

    # Create unique app name for this run
    app_name = f"de-bench-agent-runner-{uuid.uuid4().hex[:8]}"

    # Set up Modal image
    image = modal.Image.from_gcp_artifact_registry(
        GAR_IMAGE,
        secret=modal.Secret.from_name("gcp-registry-secret"),
    )

    # Create Modal app
    app = modal.App(app_name)
    
    @app.function(
        image=image,
        cpu=1.0,
        memory="2Gi",
        timeout=3600,
        secrets=[modal.Secret.from_dict(settings_object["secrets"])],
        serialized=True,
        name="run_task",
    )
    def run_task():
        """Execute the task script inside the Modal container."""
        env = os.environ.copy()
        proc = subprocess.run(
            settings_object["task_script"],
            shell=True,
            capture_output=True,
            text=True,
            env=env,
            check=False
        )
        return {"status": "pass" if proc.returncode == 0 else "failed"}
    
    # Deploy and run the task
    with app.run():
        result = run_task.remote()
    
    return {
        "status": result["status"],
        "app_name": app_name
    }

