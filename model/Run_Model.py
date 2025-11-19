# configure this file to run your model
import os
import sys
from dotenv import load_dotenv
import uuid
import braintrust
from braintrust import current_span

load_dotenv()


from ardent import ArdentClient, ArdentError
from Environment.Kubernetes.Kubernetes import Kubernetes
from Environment.File_Share.File_Share import create_file_share

# import your AI model into this file


@braintrust.traced
def run_model(container, task, configs, extra_information={}):
    # A Wrapper for your model to do things.

    result = None

    mode = extra_information.get("mode", "Ardent")

    print(f"{mode=}")
    print(f"{container=}")
    print(f"{task=}")
    print(f"{configs=}")
    print(f"{extra_information=}")

    # create the ardent client with the specific creds then we go!
    if mode == "Ardent":
        Ardent_Client = ArdentClient(
            api_key=extra_information["api_key"],
            base_url=os.getenv("ARDENT_BASE_URL"),
        )

        result = Ardent_Client.create_and_execute_job(
            message=task,
            header_overrides={
                "X-Braintrust-Exported-Parent-Span": current_span().export(),
            },
        )

    if mode == "Claude_Code":
        # Claude Code via Kubernetes (synchronous)
        print("Using Claude Code")

        # Prepare identifiers and resources (fully local, no backend IDs)

        job_k8s = extra_information.get("kubernetes_object")
        pod_name = extra_information.get("pod_name")

        # Third command: Run Claude Code with actual task and configs
        # Escape quotes in task and configs for shell command
        escaped_task = task.replace('"', '\\"').replace("'", "\\'")
        escaped_configs = str(configs).replace('"', '\\"').replace("'", "\\'")

        claude_prompt = f"Task: {escaped_task}\\n\\nAvailable configurations: {escaped_configs}\\n\\nPlease complete this task using the provided configurations."
        claude_command = f'claude -p "{claude_prompt}" --allowedTools all --dangerously-skip-permissions'
        print("This is the calude command")
        print(claude_command)
        claude_output = job_k8s.run_terminal_command_in_pod(pod_name, claude_command)

        result = {
            "status": "pass",
            "pod_name": pod_name,
            "claude_output": claude_output,
        }
        print(result)

    if mode == "OpenAI_Codex":
        # OpenAI Codex via Kubernetes (synchronous)
        print("Using OpenAI Codex")

        # Prepare identifiers and resources (fully local, no backend IDs)

        job_k8s = extra_information.get("kubernetes_object")
        pod_name = extra_information.get("pod_name")

        # Run OpenAI Codex with actual task and configs
        # Escape quotes in task and configs for shell command
        escaped_task = task.replace('"', '\\"').replace("'", "\\'")
        escaped_configs = str(configs).replace('"', '\\"').replace("'", "\\'")

        codex_prompt = f"Task: {escaped_task}\\n\\nAvailable configurations: {escaped_configs}\\n\\nPlease complete this task using the provided configurations."
        codex_command = f'codex exec --dangerously-bypass-approvals-and-sandbox --skip-git-repo-check "{codex_prompt}"'
        print("This is the codex command")
        print(codex_command)
        codex_output = job_k8s.run_terminal_command_in_pod(pod_name, codex_command)

        result = {
            "status": "pass",
            "pod_name": pod_name,
            "codex_output": codex_output,
        }
        print(result)

    return result
