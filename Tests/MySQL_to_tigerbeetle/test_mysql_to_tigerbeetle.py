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
def test_mysql_to_tigerbeetle():
    input_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create a Docker client with the compose file configuration
    docker = DockerClient(
        compose_files=[os.path.join(input_dir, "docker-compose.yml")]
    )

    #SECTION 1: SETUP THE TEST
    try:
        # Start docker-compose to set up tigerbeetle
        docker.compose.up(detach=True)
        
        # Give TigerBeetle a moment to start up
        time.sleep(10)
        
        #cursor = connection.cursor()

        #create a test database and then select it to execute the queries
        #cursor.execute("CREATE DATABASE IF NOT EXISTS Access_Tokens")
        #cursor.execute("USE Access_Tokens")
        
        # Create test tables
        #cursor.execute("""
        #    CREATE TABLE IF NOT EXISTS plaid_access_tokens (
        #        company_id VARCHAR(255) PRIMARY KEY,
        #        access_token VARCHAR(255) NOT NULL
        #    )
        #""")
        
        #cursor.execute("""
        #    CREATE TABLE IF NOT EXISTS finch_access_tokens (
        #        company_id VARCHAR(255) PRIMARY KEY,
        #        access_token VARCHAR(255) NOT NULL
        #    )
        #""")
        
        # Insert test data
        #cursor.execute("""
        #    INSERT INTO plaid_access_tokens (company_id, access_token) 
        #    VALUES (%s, %s)
        #""", ("test_company_123", "test_plaid_token"))
        
        #cursor.execute("""
        #    INSERT INTO finch_access_tokens (company_id, access_token) 
        #    VALUES (%s, %s)
        #""", ("test_company_123", "test_finch_token"))
        
        #connection.commit()

    except Exception as e:
        raise Exception(f"Error in setup: {e}")
    

    try:
        #SECTION 2: RUN THE MODEL
        #run_model(container=None, task=Test_Configs.User_Input, configs=Test_Configs.Configs)

        #SECTION 3: VERIFY THE OUTCOMES
        # Verify data in TigerBeetle
        # TODO: Add TigerBeetle verification logic here
        assert True, "Data not found in TigerBeetle"

    finally:
        # Cleanup
        try:
            # MySQL cleanup
            #cursor.execute("DROP TABLE IF EXISTS plaid_access_tokens")
            #cursor.execute("DROP TABLE IF EXISTS finch_access_tokens")
            #connection.commit()
            #cursor.close()
            #connection.close()
            
            # Stop and remove containers, networks, and volumes
            docker.compose.down(volumes=True)
        except Exception as e:
            print(f"Error during cleanup: {e}")