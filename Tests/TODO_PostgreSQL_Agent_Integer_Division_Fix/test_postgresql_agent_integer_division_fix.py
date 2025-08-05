# Import from the Model directory
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, remove_model_configs
import os
import importlib
import pytest
import time
import psycopg2
import uuid

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel test execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


@pytest.mark.postgresql
@pytest.mark.database
@pytest.mark.three
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"integer_division_fix_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"purchases_test_db_{test_timestamp}_{test_uuid}",
            "tables": [
                {
                    "name": "purchases_bad",
                    "columns": [
                        {"name": "user_id", "type": "INT", "primary_key": True},
                        {"name": "total_items", "type": "INT", "not_null": True},
                        {"name": "total_orders", "type": "INT", "not_null": True}
                    ],
                    "data": [
                        {"user_id": 1, "total_items": 5, "total_orders": 10},   # Should be 0.5
                        {"user_id": 2, "total_items": 3, "total_orders": 7},    # Should be ~0.4286
                        {"user_id": 4, "total_items": 3, "total_orders": 4},    # Should be 0.75 (but INT/INT = 0)
                    ]
                }
            ]
        }
    ]
}], indirect=True)
def test_postgresql_agent_integer_division_fix(request, postgres_resource, supabase_account_resource):
    """Test that validates AI agent can identify and fix integer division truncation issues in PostgreSQL calculations."""
    
    # Set up test tracking
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    
    test_steps = [
        {
            "name": "Integer Division Problem Demonstration",
            "description": "Verify the current schema demonstrates integer division truncation (results = 0)",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Agent Analysis and Fix",
            "description": "AI Agent analyzes the mathematical issue and implements proper structural fix by changing column data types",
            "status": "did not reach", 
            "Result_Message": "",
        },
        {
            "name": "Structural Fix Validation",
            "description": "Verify the agent fixed the original table structure (changed columns to NUMERIC), not just created workarounds",
            "status": "did not reach",
            "Result_Message": "",
        },
        
        {
            "name": "Data Preservation Validation",
            "description": "Verify all original data was preserved during the fix",
            "status": "did not reach",
            "Result_Message": "",
        }
    ]
    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    config_results = None
    created_db_name = postgres_resource["created_resources"][0]["name"]
    print(f"PostgreSQL Agent Integer Division Fix test using database: {created_db_name}")
    
    try:
        # Set up model configurations with actual database name and test-specific credentials
        test_configs = Test_Configs.Configs.copy()
        test_configs["services"]["postgreSQL"]["databases"] = [{"name": created_db_name}]
        config_results = set_up_model_configs(
            Configs=test_configs,
            custom_info={
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )

        # DEMONSTRATE THE INTEGER DIVISION PROBLEM FIRST
        db_connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require",
        )
        db_cursor = db_connection.cursor()

        print("\n=== BEFORE MODEL RUN - DATABASE STATE ===")
        
        # Show current table structure
        db_cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'purchases_bad'
            ORDER BY ordinal_position
        """)
        initial_schema = db_cursor.fetchall()
        print(f"INITIAL SCHEMA: {initial_schema}")
        
        # Show current data
        db_cursor.execute("SELECT * FROM purchases_bad ORDER BY user_id")
        initial_data = db_cursor.fetchall()
        print(f"INITIAL DATA: {initial_data}")

        # Demonstrate integer division truncation problem
        db_cursor.execute("SELECT user_id, total_items, total_orders, total_items/total_orders AS avg_items FROM purchases_bad WHERE total_orders > 0")
        problem_results = db_cursor.fetchall()
        print(f"INTEGER DIVISION RESULTS: {problem_results}")

        # Check if all results are 0 (integer division truncation)
        all_zero = all(result[3] == 0 for result in problem_results)
        print(f"ALL RESULTS ARE ZERO: {all_zero}")

        if all_zero and len(problem_results) >= 3:
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = f"Integer division problem confirmed: All averages truncated to 0: {problem_results}"
        else:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = f"Integer division problem not demonstrated as expected: {problem_results}"
            raise AssertionError("Initial integer division problem setup validation failed")

        db_cursor.close()
        db_connection.close()
        
        print(f"\n=== ABOUT TO RUN MODEL ===")
        print(f"Database: {created_db_name}")

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        model_result = run_model(
            container=None,
            task=Test_Configs.User_Input,
            configs=test_configs,
            extra_information={
                "useArdent": True,
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))

        test_steps[1]["status"] = "passed"
        test_steps[1]["Result_Message"] = "AI Agent completed integer division analysis and fix"
        
        print(f"\n=== MODEL RUN COMPLETED ===")
        print(f"Runtime: {end_time - start_time:.2f} seconds")

        # SECTION 3: VERIFY THE OUTCOMES

        # Reconnect to verify the agent's solution
        db_connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require",
        )
        db_cursor = db_connection.cursor()

        print("\n=== AFTER MODEL RUN - DATABASE STATE ===")

        try:
            # Show all tables in the database
            db_cursor.execute("""
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema, table_name
            """)
            all_tables = db_cursor.fetchall()
            print(f"ALL TABLES: {all_tables}")

            # Show all views in the database
            db_cursor.execute("""
                SELECT table_schema, table_name
                FROM information_schema.views
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            """)
            all_views = db_cursor.fetchall()
            print(f"ALL VIEWS: {all_views}")

            # Show all functions
            db_cursor.execute("""
                SELECT routine_schema, routine_name, routine_type
                FROM information_schema.routines
                WHERE routine_schema NOT IN ('information_schema', 'pg_catalog')
            """)
            all_functions = db_cursor.fetchall()
            print(f"ALL FUNCTIONS: {all_functions}")

            # Check current schema of purchases_bad table
            db_cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = 'purchases_bad'
                ORDER BY ordinal_position
            """)
            current_schema = db_cursor.fetchall()
            print(f"CURRENT PURCHASES_BAD SCHEMA: {current_schema}")

            # Check current data in purchases_bad
            db_cursor.execute("SELECT * FROM purchases_bad ORDER BY user_id")
            current_data = db_cursor.fetchall()
            print(f"CURRENT PURCHASES_BAD DATA: {current_data}")

            # Test current division behavior
            db_cursor.execute("SELECT user_id, total_items, total_orders, total_items/total_orders AS division_result FROM purchases_bad WHERE total_orders > 0")
            current_division = db_cursor.fetchall()
            print(f"CURRENT DIVISION RESULTS: {current_division}")

            # Look for evidence of the agent's fix - check if proper decimal calculations now exist
            # The agent might have created new columns, views, functions, or modified existing data types

            # First, check if original data is preserved
            db_cursor.execute("SELECT COUNT(*) FROM purchases_bad")
            row_count = db_cursor.fetchone()[0]
            print(f"ROW COUNT: {row_count}")

            if row_count == 3:
                test_steps[3]["status"] = "passed"
                test_steps[3]["Result_Message"] = "Original data preserved (3 rows maintained)"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3]["Result_Message"] = f"Data preservation failed: Expected 3 rows, found {row_count}"
                raise AssertionError("Original data not preserved during fix")
            
            # Look for the agent's solution - could be new columns, views, or altered table structure
            # Check for any new columns that might contain proper averages
            db_cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'purchases_bad'
                AND column_name LIKE '%avg%'
            """)
            avg_columns = db_cursor.fetchall()
            print(f"AVG COLUMNS FOUND: {avg_columns}")

            # Check for any new views that might handle the calculation
            db_cursor.execute("""
                SELECT table_name
                FROM information_schema.views
                WHERE table_schema = 'public'
            """)
            views = db_cursor.fetchall()
            print(f"VIEWS FOUND: {views}")

            # Check for any new functions that might handle the calculation
            db_cursor.execute("""
                SELECT routine_name
                FROM information_schema.routines
                WHERE routine_type = 'FUNCTION'
                AND specific_schema = 'public'
            """)
            functions = db_cursor.fetchall()
            print(f"FUNCTIONS FOUND: {functions}")

            # Try different approaches the agent might have used
            decimal_results_found = False
            solution_method = ""
            
            print(f"\n=== TESTING DIFFERENT SOLUTION APPROACHES ===")

            # Method 1: Check if agent created a computed column
            if avg_columns:
                print(f"Testing Method 1: Computed column approach with {avg_columns}")
                try:
                    db_cursor.execute("SELECT user_id, avg_items_per_order FROM purchases_bad ORDER BY user_id")
                    computed_results = db_cursor.fetchall()
                    print(f"COMPUTED COLUMN RESULTS: {computed_results}")

                    # Validate the results
                    expected_results = [(1, 0.5), (2, 0.4286), (3, 0.75)]

                    for i, (user_id, avg_val) in enumerate(computed_results[:3]):
                        expected_avg = expected_results[i][1]
                        if abs(float(avg_val) - expected_avg) < 0.01:  # Allow small floating point differences
                            decimal_results_found = True

                    solution_method = f"Generated column: {avg_columns[0][0]}"
                    print(f"Method 1 Results: decimal_results_found={decimal_results_found}")
                except Exception as e:
                    print(f"Method 1 Failed: {e}")
                    pass
            else:
                print("Method 1: No avg columns found, skipping")

            # Method 2: Check if agent created a view
            if views and not decimal_results_found:
                print(f"Testing Method 2: View approach with {views}")
                for view in views:
                    try:
                        view_name = view[0]
                        db_cursor.execute(f"SELECT user_id, * FROM {view_name} ORDER BY user_id")
                        view_results = db_cursor.fetchall()
                        print(f"VIEW {view_name} RESULTS: {view_results}")

                        # Look for decimal values in any column
                        for row in view_results:
                            for val in row[1:]:  # Skip user_id
                                if val is not None and isinstance(val, (float, int)) and 0 < val < 1:
                                    decimal_results_found = True
                                    solution_method = f"View: {view_name}"
                                    break
                            if decimal_results_found:
                                break

                        print(f"Method 2 Results for {view_name}: decimal_results_found={decimal_results_found}")

                    except Exception as e:
                        print(f"Method 2 Failed for {view_name}: {e}")
                        continue
            else:
                print("Method 2: No views found or decimal results already found, skipping")

            # Method 3: Check if agent created a function
            if functions and not decimal_results_found:
                print(f"Testing Method 3: Function approach with {functions}")
                for func in functions:
                    try:
                        func_name = func[0]
                        # Try to call the function
                        db_cursor.execute(f"SELECT user_id, {func_name}(total_items, total_orders) FROM purchases_bad ORDER BY user_id")
                        func_results = db_cursor.fetchall()
                        print(f"FUNCTION {func_name} RESULTS: {func_results}")

                        # Check for decimal results
                        for user_id, avg_val in func_results:
                            if avg_val is not None and isinstance(avg_val, (float, int)) and 0 < avg_val < 1:
                                decimal_results_found = True
                                solution_method = f"Function: {func_name}"
                                break

                        print(f"Method 3 Results for {func_name}: decimal_results_found={decimal_results_found}")

                    except Exception as e:
                        print(f"Method 3 Failed for {func_name}: {e}")
                        continue
            else:
                print("Method 3: No functions found or decimal results already found, skipping")
            
            # Method 4: Check if agent properly fixed the underlying data types (REQUIRED)
            # This is now the primary validation - we require structural fixes, not workarounds
            print(f"\n=== TESTING METHOD 4: DATA TYPE FIX ===")
            db_cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'purchases_bad'
                AND column_name IN ('total_items', 'total_orders')
                ORDER BY column_name
            """)
            current_types = db_cursor.fetchall()
            print(f"DEBUG: Raw column types from DB: {current_types}")

            # Check if BOTH columns were properly changed to numeric types
            items_type = next((col[1] for col in current_types if col[0] == 'total_items'), None)
            orders_type = next((col[1] for col in current_types if col[0] == 'total_orders'), None)

            print(f"DEBUG: Column types detected - total_items: {items_type}, total_orders: {orders_type}")

            # Check for various numeric data types PostgreSQL might return
            numeric_types = ['numeric', 'decimal', 'real', 'double precision', 'float']
            proper_types_fixed = (items_type and any(nt in items_type.lower() for nt in numeric_types) and
                                orders_type and any(nt in orders_type.lower() for nt in numeric_types))
            
            print(f"DEBUG: proper_types_fixed = {proper_types_fixed}")
            print(f"DEBUG: numeric_types checked: {numeric_types}")

            if proper_types_fixed:
                print("DEBUG: Data types were properly fixed, testing calculations...")
                # Test the calculation with the fixed types
                try:
                    db_cursor.execute("""
                        SELECT user_id,
                               CASE WHEN total_orders > 0
                                    THEN total_items / total_orders
                                    ELSE NULL
                               END AS avg_items
                        FROM purchases_bad
                        ORDER BY user_id
                    """)
                    type_results = db_cursor.fetchall()
                    print(f"DEBUG: Type-fixed calculation results: {type_results}")

                    # Check for decimal results
                    decimal_found = any(result[1] is not None and 0 < result[1] < 1 for result in type_results)
                    
                    print(f"DEBUG: decimal_found={decimal_found}")
                    
                    if decimal_found:
                        decimal_results_found = True
                        solution_method = "Proper data type fix - columns changed to NUMERIC"
                        print(f"DEBUG: Method 4 SUCCESS - decimal calculations working")

                except Exception as e:
                    print(f"DEBUG: Method 4 calculation test failed: {e}")
                    # Even with proper types, calculation failed
                    pass
            else:
                print("DEBUG: Data types were NOT properly fixed - still using integer types")

            print(f"\n=== FINAL VALIDATION DECISIONS ===")
            print(f"proper_types_fixed: {proper_types_fixed}")
            print(f"decimal_results_found: {decimal_results_found}")
            print(f"solution_method: '{solution_method}'")
            print(f"avg_columns: {len(avg_columns) if avg_columns else 0}")
            print(f"views: {len(views) if views else 0}")
            print(f"functions: {len(functions) if functions else 0}")

            # If proper structural fix wasn't implemented, this is a failure
            if not proper_types_fixed and (avg_columns or views or functions):
                workaround_type = "view" if views else "function" if functions else "computed column"
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = f"INCORRECT SOLUTION: Agent created workaround ({workaround_type}) instead of fixing the underlying data structure. Original columns still have wrong data types: total_items={items_type}, total_orders={orders_type}"
                print(f"DEBUG: Test failing at condition 1 - proper_types_fixed={proper_types_fixed}, workarounds found: avg_columns={len(avg_columns) if avg_columns else 0}, views={len(views) if views else 0}, functions={len(functions) if functions else 0}")
                raise AssertionError("Agent used workaround instead of proper structural fix - original table columns must be changed to NUMERIC types")
            
            # Debug output before final validation
            print(f"DEBUG: Final validation state - proper_types_fixed={proper_types_fixed}, decimal_results_found={decimal_results_found}, solution_method='{solution_method}'")
            
            # Validate structural fix and decimal results
            if proper_types_fixed and decimal_results_found:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"SUCCESS: Proper structural fix implemented! {solution_method} - Integer division truncation fixed at the source"
                print("DEBUG: TEST PASSED - Proper structural fix with working calculations!")
            elif decimal_results_found and not proper_types_fixed:
                workaround_type = "view" if views else "function" if functions else "computed column" if avg_columns else "unknown workaround"
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = f"INCORRECT APPROACH: Agent created workaround instead of fixing underlying structure. Found: {workaround_type}"
                print(f"DEBUG: Test failing at condition 2 - decimal_results_found={decimal_results_found}, proper_types_fixed={proper_types_fixed}")
                raise AssertionError("Agent used workaround (view/function/computed column) instead of proper structural fix")
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = "No proper structural fix found - original table columns still have wrong data types"
                print("DEBUG: Test failing at condition 3 - No acceptable solution found")
                raise AssertionError("Agent did not properly fix the underlying data type issue")
            

            
            # Final success
            print("PostgreSQL Agent Integer Division Fix test completed successfully!")
            print(f"Database: {created_db_name}")
            print(f"Solution method: {solution_method}")
            print("✅ Integer division truncation issue resolved with proper structural fix!")
            print(f"✅ Original table columns changed to proper data types: total_items={items_type}, total_orders={orders_type}")
            assert True, "Integer division fix successful - proper structural fix implemented (data types changed)"
        
        finally:
            db_cursor.close()
            db_connection.close()

    except Exception as e:
        # Update any remaining test steps that didn't reach
        for step in test_steps:
            if step["status"] == "did not reach":
                step["status"] = "failed"
                step["Result_Message"] = f"Test failed before reaching this step: {str(e)}"
        raise
    
    finally:
        # CLEANUP
        if config_results:
            remove_model_configs(
                Configs=test_configs, 
                custom_info={
                    **config_results,
                    "publicKey": supabase_account_resource["publicKey"],
                    "secretKey": supabase_account_resource["secretKey"],
                }
            ) 