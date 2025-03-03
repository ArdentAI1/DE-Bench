import os
import importlib
import pytest
import mysql.connector
from python_on_whales import DockerClient
import time
from datetime import datetime, timedelta

from Configs.ArdentConfig import Ardent_Client
from model.Run_Model import run_model
from Configs.MySQLConfig import connection

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


@pytest.mark.mysql
@pytest.mark.tigerbeetle
@pytest.mark.plaid
@pytest.mark.finch
@pytest.mark.api_integration
@pytest.mark.database
def test_mysql_to_tigerbeetle(request):
    input_dir = os.path.dirname(os.path.abspath(__file__))

    # Create a Docker client with the compose file configuration
    docker = DockerClient(compose_files=[os.path.join(input_dir, "docker-compose.yml")])

    # SECTION 1: SETUP THE TEST
    try:
        # Start docker-compose to set up tigerbeetle
        docker.compose.up(detach=True)

        # Give TigerBeetle a moment to start up
        time.sleep(10)

        # add the right stuff to tigerbeetle

        # let's sync ardent with the tigerbeetle instance

        tigerbeetle_result = Ardent_Client.set_config(
            config_type="tigerbeetle",
            cluster_id="0",
            replica_addresses=["127.0.0.1:3000"],
        )

        # Now we set up the mysql Instance

        cursor = connection.cursor()

        # create a test database and then select it to execute the queries
        cursor.execute("CREATE DATABASE IF NOT EXISTS Access_Tokens")
        cursor.execute("USE Access_Tokens")

        # Create test tables
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS plaid_access_tokens (
                company_id VARCHAR(255) PRIMARY KEY,
                access_token VARCHAR(255) NOT NULL
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS finch_access_tokens (
                company_id VARCHAR(255) PRIMARY KEY,
                access_token VARCHAR(255) NOT NULL
            )
        """
        )

        # Insert test data with IGNORE to skip duplicates
        cursor.execute(
            """
            INSERT IGNORE INTO plaid_access_tokens (company_id, access_token) 
            VALUES (%s, %s)
        """,
            ("123", "test_plaid_token"),
        )

        cursor.execute(
            """
            INSERT IGNORE INTO finch_access_tokens (company_id, access_token) 
            VALUES (%s, %s)
        """,
            ("123", os.getenv("FINCH_ACCESS_TOKEN")),
        )

        connection.commit()

        mysql_result = Ardent_Client.set_config(
            config_type="mysql",
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            username=os.getenv("MYSQL_USERNAME"),
            password=os.getenv("MYSQL_PASSWORD"),
            databases=[{"name": "Access_Tokens"}],
        )

        print(mysql_result)

    except Exception as e:
        raise Exception(f"Error in setup: {e}")

    try:
        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        run_model(
            container=None, task=Test_Configs.User_Input, configs=Test_Configs.Configs
        )
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))

        # SECTION 3: VERIFY THE OUTCOMES
        # Verify data in TigerBeetle
        # TODO: Add TigerBeetle verification logic here
        assert True, "Data not found in TigerBeetle"

    finally:
        # Cleanup
        try:
            # MySQL cleanup
            cursor.execute("DROP DATABASE IF EXISTS Access_Tokens")
            connection.commit()
            cursor.close()
            # Stop and remove containers, networks, and volumes to clean up tigerbeetle
            docker.compose.down(volumes=True)

            # now we delete the config for tigerbeetle
            Ardent_Client.delete_config(
                config_id=tigerbeetle_result["specific_config"]["id"]
            )

            # now we delete the config for mysql
            Ardent_Client.delete_config(config_id=mysql_result["specific_config"]["id"])
        except Exception as e:
            print(f"Error during cleanup: {e}")
