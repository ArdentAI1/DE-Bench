import time
import requests
from typing import Dict, Any
from databricks_api import DatabricksAPI

def extract_warehouse_id_from_http_path(http_path: str) -> str:
    """Extract warehouse ID from HTTP path like /sql/1.0/warehouses/abc123"""
    if "/warehouses/" in http_path:
        return http_path.split("/warehouses/")[-1]
    return None

def execute_sql_query(host: str, token: str, warehouse_id: str, sql_query: str, 
                     catalog: str = "hive_metastore", schema: str = "default", timeout: int = 60) -> Dict[str, Any]:
    """Execute SQL query using Databricks SQL Statement Execution API"""
    
    # Ensure host has proper format
    if not host.startswith("https://"):
        host = f"https://{host}"
    
    # Prepare request payload for SQL execution
    payload = {
        "warehouse_id": warehouse_id,
        "catalog": catalog,
        "schema": schema,
        "statement": sql_query,
        "wait_timeout": f"{timeout}s",
        "format": "JSON_ARRAY",
        "disposition": "INLINE"
    }
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Execute SQL statement
        response = requests.post(
            f"{host}/api/2.0/sql/statements/",
            headers=headers,
            json=payload,
            timeout=timeout + 10  # Add buffer for HTTP timeout
        )
        
        if response.status_code != 200:
            return {
                "success": False,
                "error": f"HTTP {response.status_code}: {response.text}",
                "status_code": response.status_code
            }
        
        result = response.json()
        
        # Check if statement executed successfully
        if result.get("status", {}).get("state") == "SUCCEEDED":
            return {
                "success": True,
                "data": result.get("result", {}).get("data_array", []),
                "schema": result.get("manifest", {}).get("schema", {}),
                "row_count": result.get("manifest", {}).get("total_row_count", 0)
            }
        elif result.get("status", {}).get("state") == "PENDING":
            return {
                "success": False,
                "error": f"Query timed out after {timeout} seconds",
                "state": "PENDING"
            }
        else:
            return {
                "success": False,
                "error": f"Query failed with state: {result.get('status', {}).get('state')}",
                "details": result
            }
            
    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "error": f"Request failed: {str(e)}"
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}"
        }

 