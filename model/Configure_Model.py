import os
from ardent import ArdentClient

from dotenv import load_dotenv
from braintrust import current_span
from braintrust import traced

load_dotenv()


@traced(name="set_up_model_configs")
def set_up_model_configs(Configs, custom_info=None):
    mode = (custom_info or {}).get("mode", "Ardent")

    results = {}

    # For non-Ardent modes, no remote config setup is required
    if mode == "Ardent":

        @traced(name="setup_ardent_connector")
        def setup_ardent_connector(
            service_name,
            connection_details,
            name,
            selected_paths,
            header_overrides=None,
        ):
            return Ardent_Client.setup_connector(
                service_name=service_name,
                connection_details=connection_details,
                name=name,
                selected_paths=selected_paths,
                header_overrides={
                    **(header_overrides or {}),
                    "X-Braintrust-Exported-Parent-Span": current_span().export(),
                },
            )

        Ardent_Client = ArdentClient(
            public_key=custom_info["publicKey"],
            secret_key=custom_info["secretKey"],
            base_url=os.getenv("ARDENT_BASE_URL"),
        )

        if "services" in Configs:
            for service in Configs["services"]:
                service_config = Configs["services"][service]

                print(f"🔍 SERVICE CONFIG: {service_config}")
                print(f"🔍 SERVICE: {service}")

                # Handle different service types
                if service == "mongodb":
                    print(f"🔧 Setting up MongoDB config:")
                    print(
                        f"   Connection string: {service_config.get('connection_string', 'MISSING')}"
                    )
                    print(f"   Databases: {service_config.get('databases', 'MISSING')}")

                    try:
                        service_result = setup_ardent_connector(
                            service_name="mongodb",
                            connection_details={
                                "connection_string": service_config[
                                    "connection_string"
                                ],
                                "databases": service_config["databases"],
                            },
                            name="MongoDB Connection",
                            selected_paths=[
                                db["name"] for db in service_config["databases"]
                            ],
                        )
                        print(f"✅ MongoDB config set successfully")
                    except Exception as e:
                        print(f"❌ MongoDB config failed:")
                        print(f"   Error: {str(e)}")
                        print(f"   Config data being sent:")
                        print(f"     - config_type: 'mongodb'")
                        print(
                            f"     - connection_string: {service_config.get('connection_string')}"
                        )
                        print(f"     - databases: {service_config.get('databases')}")
                        raise

                elif service == "postgreSQL":
                    print(f"🔧 Setting up PostgreSQL config:")
                    print(f"   Hostname: {service_config['hostname']}")
                    print(f"   Port: {service_config['port']}")
                    print(f"   Username: {service_config['username']}")
                    print(f"   Password: {service_config['password']}")
                    print(f"   Databases: {service_config['databases']}")

                    try:
                        service_result = setup_ardent_connector(
                            service_name="postgresql",  # V2 API uses lowercase
                            connection_details={
                                "host": service_config[
                                    "hostname"
                                ],  # V2 API uses "host" not "hostname"
                                "port": service_config["port"],
                                "username": service_config["username"],
                                "password": service_config["password"],
                            },
                            name="PostgreSQL Connection",
                            selected_paths=[
                                db["name"] for db in service_config["databases"]
                            ],
                        )
                        print(f"✅ PostgreSQL config set successfully")
                    except Exception as e:
                        print("EXCEPTION", str(e))
                        raise

                elif service == "mysql":
                    print(f"🔧 Setting up MySQL config:")
                    print(f"   Host: {service_config['host']}")
                    print(f"   Port: {service_config['port']}")
                    print(f"   Username: {service_config['username']}")
                    print(f"   Databases: {service_config['databases']}")

                    try:
                        service_result = setup_ardent_connector(
                            service_name="mysql",  # V2 API uses lowercase
                            connection_details={
                                "host": service_config["host"],
                                "port": service_config["port"],
                                "username": service_config["username"],
                                "password": service_config["password"],
                            },
                            name="MySQL Connection",
                            selected_paths=[
                                db["name"] for db in service_config["databases"]
                            ],
                        )
                        print(f"✅ MySQL config set successfully")
                    except Exception as e:
                        print("EXCEPTION", str(e))
                        raise

                elif service == "tigerbeetle":
                    service_result = setup_ardent_connector(
                        config_type="tigerbeetle",
                        cluster_id=service_config["cluster_id"],
                        replica_addresses=service_config["replica_addresses"],
                        header_overrides={
                            "X-Braintrust-Exported-Parent-Span": current_span().export(),
                        },
                    )

                elif service == "databricks":
                    service_result = setup_ardent_connector(
                        config_type="databricks",
                        server_hostname=service_config["host"],
                        access_token=service_config["token"],
                        http_path=service_config["http_path"],
                        cluster_id=service_config.get("cluster_id"),
                        catalogs=[
                            {
                                "name": service_config["catalog"],
                                "databases": [
                                    {"name": service_config["schema"], "tables": []}
                                ],
                            }
                        ],
                    )

                elif service == "snowflake":
                    print(f"🔧 Setting up Snowflake config:")
                    print(f"   Account: {service_config['account']}")
                    print(f"   User: {service_config['user']}")
                    print(f"   Warehouse: {service_config['warehouse']}")
                    print(f"   Role: {service_config.get('role', 'SYSADMIN')}")

                    # Build selected_paths from created_resources (V2 API format)
                    created_resources = service_config.get("created_resources", [])
                    selected_paths = []

                    for resource in created_resources:
                        if resource["type"] == "database":
                            db_name = resource["name"]
                            schema_name = resource.get("schema")
                            tables = resource.get("tables", [])

                            if tables:
                                # If tables exist, select each table: database.schema.table
                                for table in tables:
                                    selected_paths.append(
                                        f"{db_name}.{schema_name}.{table}"
                                    )
                            else:
                                # Otherwise select the whole schema: database.schema
                                selected_paths.append(f"{db_name}.{schema_name}")

                    print(f"   Selected paths: {selected_paths}")

                    try:
                        service_result = setup_ardent_connector(
                            service_name="snowflake",  # V2 API uses lowercase
                            connection_details={
                                "account": service_config["account"],
                                "user": service_config["user"],
                                "password": service_config["password"],
                                "warehouse": service_config["warehouse"],
                                "role": service_config.get("role", "SYSADMIN"),
                            },
                            name="Snowflake Connection",
                            selected_paths=selected_paths,
                        )
                        print(f"✅ Snowflake config set successfully")
                    except Exception as e:
                        print("EXCEPTION", str(e))
                        raise

                elif service == "airflow":
                    print(f"🔧 Setting up Airflow config:")
                    print(f"   Webserver URL: {service_config.get('host')}")
                    print(f"   Username: {service_config.get('username')}")
                    print(
                        f"   GitHub Token: {'***' if service_config.get('github_token') else 'NOT SET'}"
                    )
                    print(f"   Repository: {service_config.get('repo')}")
                    print(f"   DAG Path: {service_config.get('dag_path')}")
                    print(
                        f"   Requirements Path: {service_config.get('requirements_path')}"
                    )

                    try:
                        connection_details = {
                            "webserver_url": service_config.get(
                                "host"
                            ),  # Fixture provides "host"
                            "github_token": service_config.get("github_token"),
                            "repo": service_config.get("repo"),
                            "dag_path": service_config.get("dag_path", "dags/"),
                            "requirements_path": service_config.get(
                                "requirements_path", "requirements.txt"
                            ),
                        }

                        # Add authentication - either username/password or api_token
                        if service_config.get("api_token"):
                            connection_details["api_token"] = service_config.get(
                                "api_token"
                            )
                        else:
                            connection_details["username"] = service_config.get(
                                "username"
                            )
                            connection_details["password"] = service_config.get(
                                "password"
                            )

                        service_result = setup_ardent_connector(
                            service_name="airflow",  # V2 API uses lowercase
                            connection_details=connection_details,
                            name="Airflow Connection",
                            selected_paths=[
                                "*"
                            ],  # Select all DAGs, should be fine for most tests.
                        )
                        print(f"✅ Airflow config set successfully")
                    except Exception as e:
                        print("EXCEPTION", str(e))
                        raise

                elif service == "github":
                    # Handle GitHub service - skip Ardent config as it's handled locally
                    print(
                        f"🐙 GitHub service detected - handling locally (no Ardent config needed)"
                    )
                    service_result = {"status": "handled_locally", "service": service}
                else:
                    # Handle unknown service types
                    print(f"⚠️ Unknown service type: {service}")
                    service_result = None

                # Add the result to our results dictionary
                if service_result is not None:
                    if not results:
                        results = {service: service_result}
                    else:
                        results[service] = service_result
    elif mode == "Claude_Code":
        print("Configuring Modal runner for Claude Code")
        results = {"mode": mode}
    elif mode == "OpenAI_Codex":
        print("Configuring Modal runner for OpenAI Codex")
        results = {"mode": mode}
    return results


def cleanup_model_artifacts(Configs, custom_info=None):
    # This is a place where we can remove the model configs

    mode = custom_info.get("mode", "Ardent")

    print("Cleaning up model artifacts")
    print(f"--mode: {mode}")
    # Note: Skipping custom_info JSON dump to avoid serialization issues with Kubernetes objects

    if mode == "Ardent":
        Ardent_Client = ArdentClient(
            public_key=custom_info["publicKey"],
            secret_key=custom_info["secretKey"],
            base_url=os.getenv("ARDENT_BASE_URL"),
        )

        if "services" in Configs:
            for service in Configs["services"]:
                if service in custom_info:
                    # New V2 API: connector ID is returned directly
                    connector_id = custom_info[service].get("id")
                    if connector_id:
                        Ardent_Client.delete_connector(connector_id=connector_id)

        if "job_id" in custom_info:
            Ardent_Client.delete_job(job_id=custom_info["job_id"])

    elif mode in {"Claude_Code", "OpenAI_Codex"}:
        # No cleanup required when running via Modal
        print(f"No cleanup required for {mode} resources")


# Create an alias for backwards compatibility
remove_model_configs = cleanup_model_artifacts
