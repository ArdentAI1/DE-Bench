# configure this file to run your model
import os
import sys
from dotenv import load_dotenv
import braintrust
from braintrust import current_span

load_dotenv()



from ardent import ArdentClient
from Environment.Modal.modal_runner import run_modal_task

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
            public_key=extra_information["publicKey"],
            secret_key=extra_information["secretKey"],
            base_url=os.getenv("ARDENT_BASE_URL"),
        )

        result = Ardent_Client.create_and_execute_job(
            org_id=extra_information.get("org_id"),
            message=task,
            header_overrides={
                "X-Braintrust-Exported-Parent-Span": current_span().export(),
            },
        )

    if mode == "Claude_Code":
        print("Using Claude Code via Modal")

        prompt = (
            f"Task: {task}\n\nAvailable configurations: {configs}\n\n"
            "Please complete this task using the provided configurations."
        )

        modal_result = run_modal_task(command=prompt, mode="Claude_Code")

        result = {
            "status": modal_result["status"]
        }
        print(result)

    if mode == "OpenAI_Codex":
        print("Using OpenAI Codex via Modal")

        prompt = (
            f"Task: {task}\n\nAvailable configurations: {configs}\n\n"
            "Please complete this task using the provided configurations."
        )

        modal_result = run_modal_task(command=prompt, mode="OpenAI_Codex")

        result = {
            "status": modal_result["status"]
        }
        print(result)

    return result
