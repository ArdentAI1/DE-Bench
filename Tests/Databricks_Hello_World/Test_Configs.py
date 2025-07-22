import os
import uuid
import time
from dotenv import load_dotenv

load_dotenv()

# Generate unique identifiers for this test run to avoid conflicts
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]
test_id = f"{test_timestamp}_{test_uuid}"
unique_message = f"HELLO_WORLD_SUCCESS_{test_id}"
table_suffix = test_id.replace('-', '_')  # SQL-safe table name

# Required environment variables for Databricks testing
REQUIRED_ENV_VARS = {
    "DATABRICKS_HOST": "Databricks workspace hostname (e.g., dbc-abc123-def.cloud.databricks.com)",
    "DATABRICKS_TOKEN": "Databricks personal access token"
}

OPTIONAL_ENV_VARS = {
    "DATABRICKS_CLUSTER_ID": "Existing cluster ID (if not provided, will create one)",
    "DATABRICKS_HTTP_PATH": "SQL warehouse HTTP path (defaults to /sql/1.0/warehouses/default)"
}

# Validate required environment variables
missing_vars = []
for var, description in REQUIRED_ENV_VARS.items():
    if not os.getenv(var):
        missing_vars.append(f"  {var}: {description}")

if missing_vars:
    raise EnvironmentError(
        f"Missing required environment variables for Databricks testing:\n" + 
        "\n".join(missing_vars) +
        f"\n\nOptional variables:\n" + 
        "\n".join([f"  {var}: {desc}" for var, desc in OPTIONAL_ENV_VARS.items()])
    )

# Test configuration for simple Spark job validation
User_Input = f"""
Create a Databricks job that implements a "Hello World" data pipeline. Follow these explicit steps:

STEP 1: Create a PySpark script that does the following:
- Import required libraries: `from pyspark.sql import SparkSession` and `from pyspark.sql.functions import current_timestamp, lit`
- Create a SparkSession: `spark = SparkSession.builder.appName("HelloWorld").getOrCreate()`
- Create a DataFrame with exactly these columns and values:
  * message: "{unique_message}"
  * timestamp: current_timestamp()
  * test_type: "hello_world"
- Use this exact code pattern:
  ```python
  df = spark.createDataFrame([("{unique_message}",)], ["message"])
  df = df.withColumn("timestamp", current_timestamp())
  df = df.withColumn("test_type", lit("hello_world"))
  ```

STEP 2: Write the data to Delta format:
- Write to this exact path: `dbfs:/tmp/hello_world_test_{test_id}`
- Use Delta format: `df.write.format("delta").mode("overwrite").save("dbfs:/tmp/hello_world_test_{test_id}")`

STEP 3: Register the table in the catalog:
- Register as: `hive_metastore.default.hello_world_test_{table_suffix}`
- Use this exact code: `df.write.format("delta").mode("overwrite").option("path", "dbfs:/tmp/hello_world_test_{test_id}").saveAsTable("hive_metastore.default.hello_world_test_{table_suffix}")`

STEP 4: Display verification:
- Show the DataFrame contents: `df.show()`
- Print success message: `print("✅ Hello World job completed successfully!")`
- Print the unique message: `print(f"Message: {unique_message}")`

STEP 5: Create and submit the job:
- Upload the PySpark script to DBFS (path: `/tmp/hello_world_script.py`)
- Create a Databricks job using the Jobs API with these specifications:
  * Job name: "Hello World Test Job"
  * Task type: "spark_python_task"
  * Python file: "dbfs:/tmp/hello_world_script.py"
  * Use the existing cluster
- Submit the job and wait for completion
- Verify the job succeeds (state: "TERMINATED" with result_state: "SUCCESS")

The final result should be:
- A Delta table at `dbfs:/tmp/hello_world_test_{test_id}` containing exactly one row
- A registered table `hive_metastore.default.hello_world_test_{table_suffix}` 
- The row should contain the message "{unique_message}", a timestamp, and test_type "hello_world"

Use the Databricks REST API for job creation and submission. The script must be completely self-contained and runnable.
"""

# Test configuration parameters
Configs = {
    "services": {
        "databricks": {
            # Workspace configuration
            "host": os.getenv("DATABRICKS_HOST"),
            "token": os.getenv("DATABRICKS_TOKEN"),
            "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID"),
            "http_path": os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/default"),
            
            # Unity Catalog configuration
            "catalog": "hive_metastore",
            "schema": "default",
            "table": f"hello_world_test_{table_suffix}",
            
            # Test parameters
            "unique_message": unique_message,
            "test_id": test_id,
            
            # Paths
            "notebook_path": f"/Shared/de_bench/hello_world_test_{test_id}",
            "delta_table_path": f"dbfs:/tmp/hello_world_test_{test_id}"
        }
    }
}