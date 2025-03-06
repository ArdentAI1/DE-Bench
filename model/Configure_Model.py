from Configs.ArdentConfig import Ardent_Client
import os
from dotenv import load_dotenv

load_dotenv()


def set_up_model_configs(Configs, custom_info=None):

    results = {}

    if "services" in Configs:

        for service in Configs["services"]:
            service_config = Configs["services"][service]

            # Handle different service types
            if service == "airflow":
                # ensure all required fields are present
                service_result = Ardent_Client.set_config(
                    config_type="airflow",
                    github_token=service_config["github_token"],
                    repo=service_config["repo"],
                    dag_path=service_config["dag_path"],
                    host=service_config["host"],
                    username=service_config["username"],
                    password=service_config["password"],
                )

            elif service == "mongodb":
                service_result = Ardent_Client.set_config(
                    config_type="mongodb",
                    connection_string=service_config["connection_string"],
                    databases=service_config["databases"],
                )

            elif service == "postgreSQL":
                service_result = Ardent_Client.set_config(
                    config_type="postgreSQL",
                    Hostname=service_config["hostname"],
                    Port=service_config["port"],
                    username=service_config["username"],
                    password=service_config["password"],
                    databases=service_config["databases"],
                )

            elif service == "mysql":
                service_result = Ardent_Client.set_config(
                    config_type="mysql",
                    host=service_config["host"],
                    port=service_config["port"],
                    username=service_config["username"],
                    password=service_config["password"],
                    databases=service_config["databases"],
                )
                
            elif service == "tigerbeetle":
                service_result = Ardent_Client.set_config(
                    config_type="tigerbeetle",
                    cluster_id=service_config["cluster_id"],
                    replica_addresses=service_config["replica_addresses"],
                )

            # Add the result to our results dictionary
            if not results:
                results = {service: service_result}
            else:
                results[service] = service_result

    return results


def remove_model_configs(Configs, custom_info=None):
    # This is a place where we can remove the model configs
    if custom_info and "services" in Configs:
        for service in Configs["services"]:
            if service in custom_info:
                id = custom_info[service]["specific_config"]["id"]
                Ardent_Client.delete_config(config_id=id)
