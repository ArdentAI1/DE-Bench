from Configs.ArdentConfig import Ardent_Client
import os
from dotenv import load_dotenv

load_dotenv()


def set_up_model_configs(Configs, custom_info=None):

    results = {}

    if "services" in Configs:

        for service in Configs["services"]:
            service_config = Configs["services"][service]

            # ensure all required fields are present

            airflow_result = Ardent_Client.set_config(
                config_type="airflow",
                github_token=service_config["github_token"],
                repo=service_config["repo"],
                dag_path=service_config["dag_path"],
                host=service_config["host"],
                username=service_config["username"],
                password=service_config["password"],
            )

            if not results:
                results = {service: airflow_result}
            else:
                results[service] = airflow_result

    return results


def remove_model_configs(Configs, custom_info=None):
    # this is a place where we can remove the model configs

    for service in Configs["services"]:
        id = custom_info[service]["specific_config"]["id"]

        Ardent_Client.delete_config(config_id=id)
