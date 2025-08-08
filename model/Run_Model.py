# configure this file to run your model
import os
import sys
from dotenv import load_dotenv

load_dotenv()


from ardent import ArdentClient, ArdentError
# import your AI model into this file


def run_model(container, task, configs, extra_information = {}):
    # A Wrapper for your model to do things.

    #create the ardent client with the specific creds then we go!
    if extra_information.get("useArdent", False) == True:
        Ardent_Client = ArdentClient(
            public_key=extra_information["publicKey"],
            secret_key=extra_information["secretKey"],
            base_url=os.getenv("ARDENT_BASE_URL"),
        )

    result = Ardent_Client.create_and_execute_job(
        message=task,
    )

    return result
