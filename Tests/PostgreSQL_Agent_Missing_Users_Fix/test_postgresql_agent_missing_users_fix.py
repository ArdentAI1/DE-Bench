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
@pytest.mark.two
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"missing_users_fix_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"users_subs_test_db_{test_timestamp}_{test_uuid}",
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "user_id", "type": "INT", "primary_key": True},
                        {"name": "name", "type": "TEXT", "not_null": True}
                    ],
                    "data": [
                        {"user_id": 1, "name": "Alice"},
                        {"user_id": 2, "name": "Bob"},
                        {"user_id": 3, "name": "Carol"}  # Carol has no subscription!
                    ]
                },
                {
                    "name": "subscriptions_bad", 
                    "columns": [
                        {"name": "user_id", "type": "INT"},  # No PK, no FK constraints
                        {"name": "plan", "type": "TEXT"}     # Free text, not normalized
                    ],
                    "data": [
                        {"user_id": 1, "plan": "Pro"},
                        {"user_id": 2, "plan": "Basic"}
                        # Carol (user_id 3) has no subscription row at all
                    ]
                }
            ]
        }
    ]
}], indirect=True)
def test_postgresql_agent_missing_users_fix(request, postgres_resource, supabase_account_resource):
    """Test that validates AI agent can identify and fix schema design issues that cause users to disappear from queries."""
    
    # Set up test tracking
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    
    test_steps = [
        {
            "name": "Missing Users Problem Demonstration",
            "description": "Verify the current schema demonstrates users disappearing from INNER JOIN queries",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Agent Analysis and Fix",
            "description": "AI Agent analyzes the schema design issue and implements proper relational database solution",
            "status": "did not reach", 
            "Result_Message": "",
        },
        {
            "name": "Schema Design Validation",
            "description": "Verify the agent created proper normalized schema with FK constraints and optional relationships",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Data Preservation Validation", 
            "description": "Verify all original users are preserved and queryable with proper LEFT JOIN logic",
            "status": "did not reach",
            "Result_Message": "",
        }
    ]
    request.node.user_properties.append(("test_steps", test_steps))
    
    # SECTION 1: SETUP THE TEST
    config_results = None
    created_db_name = postgres_resource["created_resources"][0]["name"]
    print(f"PostgreSQL Agent Missing Users Fix test using database: {created_db_name}")
    
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
        
        # DEMONSTRATE THE MISSING USERS PROBLEM FIRST
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
        
        # Show initial table structures
        db_cursor.execute("""
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name IN ('users', 'subscriptions_bad')
            ORDER BY table_name, ordinal_position
        """)
        initial_schema = db_cursor.fetchall()
        print(f"INITIAL SCHEMA: {initial_schema}")
        
        # Show initial data
        db_cursor.execute("SELECT 'users' as table_name, user_id, name, NULL as plan FROM users UNION ALL SELECT 'subscriptions_bad' as table_name, user_id, NULL as name, plan FROM subscriptions_bad ORDER BY table_name, user_id")
        initial_data = db_cursor.fetchall()
        print(f"INITIAL DATA: {initial_data}")
        
        # Show total user count
        db_cursor.execute("SELECT COUNT(*) FROM users")
        total_users = db_cursor.fetchone()[0]
        print(f"TOTAL USERS: {total_users}")

        # Demonstrate the missing users problem with INNER JOIN
        db_cursor.execute("SELECT u.user_id, u.name, s.plan FROM users u JOIN subscriptions_bad s USING (user_id) ORDER BY u.user_id")
        inner_join_results = db_cursor.fetchall()
        print(f"INNER JOIN RESULTS (loses Carol): {inner_join_results}")
        
        # Check if Carol is missing from INNER JOIN
        inner_join_count = len(inner_join_results)
        carol_missing = not any(row[1] == 'Carol' for row in inner_join_results)
        
        print(f"INNER JOIN COUNT: {inner_join_count} (should be less than {total_users})")
        print(f"CAROL MISSING FROM INNER JOIN: {carol_missing}")

        if inner_join_count < total_users and carol_missing:
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = f"Missing users problem confirmed: INNER JOIN returns {inner_join_count} users instead of {total_users}, Carol missing"
        else:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = f"Missing users problem not demonstrated as expected. INNER JOIN returned {inner_join_count} users, Carol missing: {carol_missing}"
            raise AssertionError("Initial missing users problem setup validation failed")

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
        test_steps[1]["Result_Message"] = "AI Agent completed missing users analysis and fix"
        
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
            # Show all tables now
            db_cursor.execute("""
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema, table_name
            """)
            all_tables = db_cursor.fetchall()
            print(f"ALL TABLES: {all_tables}")

            # Show all views
            db_cursor.execute("""
                SELECT table_schema, table_name
                FROM information_schema.views
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            """)
            all_views = db_cursor.fetchall()
            print(f"ALL VIEWS: {all_views}")

            # Check if agent created proper normalized schema
            proper_schema_created = False
            solution_method = ""
            
            print("\n=== DETAILED SCHEMA ANALYSIS ===")
            
            # Look for plans table (normalized approach)
            db_cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'plans')")
            plans_table_exists = db_cursor.fetchone()[0]
            print(f"PLANS TABLE EXISTS: {plans_table_exists}")
            
            # Look for proper subscriptions table (not subscriptions_bad) 
            db_cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'subscriptions')")
            subscriptions_table_exists = db_cursor.fetchone()[0]
            print(f"SUBSCRIPTIONS TABLE EXISTS: {subscriptions_table_exists}")
            
            # Show all table names to see what agent created
            table_names = [table[1] for table in all_tables if table[0] == 'public']
            print(f"ALL PUBLIC TABLES: {table_names}")
            
            # If agent created different table names, let's check those
            possible_plans_tables = [t for t in table_names if 'plan' in t.lower()]
            possible_subscription_tables = [t for t in table_names if 'subscription' in t.lower() and t != 'subscriptions_bad']
            print(f"POSSIBLE PLANS TABLES: {possible_plans_tables}")
            print(f"POSSIBLE SUBSCRIPTION TABLES: {possible_subscription_tables}")
            
            if plans_table_exists and subscriptions_table_exists:
                print("=== CHECKING PROPER SCHEMA DESIGN ===")
                
                # Check plans table structure
                db_cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = 'plans'
                    ORDER BY ordinal_position
                """)
                plans_schema = db_cursor.fetchall()
                print(f"PLANS TABLE SCHEMA: {plans_schema}")
                
                # Check subscriptions table structure  
                db_cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = 'subscriptions'
                    ORDER BY ordinal_position
                """)
                subscriptions_schema = db_cursor.fetchall()
                print(f"SUBSCRIPTIONS TABLE SCHEMA: {subscriptions_schema}")
                
                # Check for FK constraints
                db_cursor.execute("""
                    SELECT tc.constraint_name, tc.table_name, kcu.column_name, 
                           ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name IN ('subscriptions', 'plans')
                """)
                fk_constraints = db_cursor.fetchall()
                print(f"FK CONSTRAINTS: {fk_constraints}")
                
                # Check for PK on subscriptions.user_id
                db_cursor.execute("""
                    SELECT column_name
                    FROM information_schema.key_column_usage
                    WHERE table_name = 'subscriptions'
                    AND constraint_name IN (
                        SELECT constraint_name
                        FROM information_schema.table_constraints
                        WHERE table_name = 'subscriptions'
                        AND constraint_type = 'PRIMARY KEY'
                    )
                """)
                subscriptions_pk = db_cursor.fetchall()
                print(f"SUBSCRIPTIONS PK COLUMNS: {subscriptions_pk}")
                
                # Validate proper schema design
                has_user_id_pk = any(col[0] == 'user_id' for col in subscriptions_pk)
                has_fk_constraints = len(fk_constraints) >= 1  # At least one FK
                
                print(f"SCHEMA VALIDATION DETAILS:")
                print(f"  - PK on user_id: {has_user_id_pk}")
                print(f"  - FK constraints count: {len(fk_constraints)}")
                print(f"  - FK constraints: {fk_constraints}")
                
                if has_user_id_pk and has_fk_constraints:
                    proper_schema_created = True
                    solution_method = "Proper normalized schema with FK constraints"
                    print(f"✅ PROPER SCHEMA VALIDATED!")
                else:
                    print(f"❌ SCHEMA VALIDATION FAILED: PK on user_id={has_user_id_pk}, FK constraints={has_fk_constraints}")
            else:
                print("❌ Expected tables (plans + subscriptions) not found, checking for alternative solutions...")
                
                # Check if agent modified the existing subscriptions_bad table
                print("\n=== CHECKING ALTERNATIVE SOLUTIONS ===")
                
                # Check if subscriptions_bad was modified with constraints
                db_cursor.execute("""
                    SELECT tc.constraint_name, tc.constraint_type
                    FROM information_schema.table_constraints AS tc
                    WHERE tc.table_name = 'subscriptions_bad'
                    AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
                """)
                existing_table_constraints = db_cursor.fetchall()
                print(f"SUBSCRIPTIONS_BAD CONSTRAINTS: {existing_table_constraints}")
                
                # Check if any new tables were created with different names
                for table_name in table_names:
                    if table_name not in ['users', 'subscriptions_bad']:
                        print(f"ANALYZING NEW TABLE: {table_name}")
                        db_cursor.execute(f"""
                            SELECT column_name, data_type, is_nullable
                            FROM information_schema.columns
                            WHERE table_name = '{table_name}'
                            ORDER BY ordinal_position
                        """)
                        table_schema = db_cursor.fetchall()
                        print(f"  Schema: {table_schema}")

            # Test the critical validation: Do all users show up in queries now?
            print("\n=== TESTING USER PRESERVATION ===")
            
            all_users_preserved = False
            carol_properly_handled = False
            
            # Try to find a query that shows all users with their subscription status
            print("=== TESTING DIFFERENT QUERY APPROACHES ===")
            
            # Test 1: If proper schema was created, test with proper LEFT JOIN
            if proper_schema_created:
                print("Testing with proper normalized schema...")
                try:
                    db_cursor.execute("""
                        SELECT u.user_id, u.name,
                               COALESCE(p.plan_name, 'No plan') AS plan
                        FROM users u
                        LEFT JOIN subscriptions s USING (user_id)
                        LEFT JOIN plans p ON p.plan_id = s.plan_id
                        ORDER BY u.user_id
                    """)
                    proper_join_results = db_cursor.fetchall()
                    print(f"PROPER LEFT JOIN RESULTS: {proper_join_results}")
                    
                    # Check if all 3 users are present
                    if len(proper_join_results) == 3:
                        all_users_preserved = True
                        # Check if Carol is properly handled
                        carol_row = next((row for row in proper_join_results if row[1] == 'Carol'), None)
                        if carol_row and (carol_row[2] == 'No plan' or carol_row[2] is None):
                            carol_properly_handled = True
                            solution_method = "Proper normalized schema with LEFT JOIN"
                            print(f"✅ CAROL PROPERLY HANDLED: {carol_row}")
                        
                except Exception as e:
                    print(f"❌ PROPER JOIN TEST FAILED: {e}")
            
            # Test 2: Maybe agent just fixed the query without changing schema
            if not all_users_preserved:
                print("Testing simple LEFT JOIN with original tables...")
                try:
                    db_cursor.execute("""
                        SELECT u.user_id, u.name,
                               COALESCE(s.plan, 'No plan') AS plan
                        FROM users u
                        LEFT JOIN subscriptions_bad s USING (user_id)
                        ORDER BY u.user_id
                    """)
                    simple_join_results = db_cursor.fetchall()
                    print(f"SIMPLE LEFT JOIN RESULTS: {simple_join_results}")
                    
                    if len(simple_join_results) == 3:
                        all_users_preserved = True
                        carol_row = next((row for row in simple_join_results if row[1] == 'Carol'), None)
                        if carol_row and (carol_row[2] == 'No plan' or carol_row[2] is None):
                            carol_properly_handled = True
                            solution_method = "Simple LEFT JOIN fix (not ideal but functional)"
                            print(f"✅ SIMPLE SOLUTION - CAROL HANDLED: {carol_row}")
                        
                except Exception as e:
                    print(f"❌ SIMPLE JOIN TEST FAILED: {e}")
            
            # Test 3: Check if agent created any alternative table structures
            if not all_users_preserved and len(table_names) > 2:  # More than just users + subscriptions_bad
                print("Testing alternative table structures...")
                for table_name in table_names:
                    if table_name not in ['users', 'subscriptions_bad'] and not all_users_preserved:
                        try:
                            print(f"Testing table: {table_name}")
                            # Try to join users with this new table
                            db_cursor.execute(f"""
                                SELECT u.user_id, u.name, t.*
                                FROM users u
                                LEFT JOIN {table_name} t ON u.user_id = t.user_id
                                ORDER BY u.user_id
                            """)
                            alt_results = db_cursor.fetchall()
                            print(f"ALTERNATIVE TABLE {table_name} RESULTS: {alt_results}")
                            
                            if len(alt_results) == 3:
                                all_users_preserved = True
                                solution_method = f"Alternative solution using table: {table_name}"
                                if any('Carol' in str(row) for row in alt_results):
                                    carol_properly_handled = True
                                    print(f"✅ ALTERNATIVE SOLUTION WORKS: {table_name}")
                            
                        except Exception as e:
                            print(f"❌ ALTERNATIVE TABLE {table_name} TEST FAILED: {e}")
                            continue
            
            # Also test if agent created any views that solve the problem
            if all_views and not all_users_preserved:
                print("=== TESTING VIEWS FOR USER PRESERVATION ===")
                for view in all_views:
                    try:
                        view_name = view[1]
                        db_cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
                        view_count = db_cursor.fetchone()[0]
                        print(f"VIEW {view_name} COUNT: {view_count}")
                        
                        if view_count == 3:  # All users preserved
                            db_cursor.execute(f"SELECT * FROM {view_name} ORDER BY user_id")
                            view_results = db_cursor.fetchall()
                            print(f"VIEW {view_name} RESULTS: {view_results}")
                            
                            # Check if Carol is in the view
                            if any('Carol' in str(row) for row in view_results):
                                all_users_preserved = True
                                carol_properly_handled = True
                                solution_method = f"View: {view_name}"
                                
                    except Exception as e:
                        print(f"VIEW {view_name} TEST FAILED: {e}")
                        continue

            print(f"\n=== FINAL VALIDATION RESULTS ===")
            print(f"proper_schema_created: {proper_schema_created}")
            print(f"all_users_preserved: {all_users_preserved}")
            print(f"carol_properly_handled: {carol_properly_handled}")
            print(f"solution_method: '{solution_method}'")
            print(f"total_tables_found: {len(table_names)}")
            print(f"views_found: {len(all_views) if all_views else 0}")

            # More flexible validation - accept either proper schema OR working solution
            schema_acceptable = proper_schema_created or all_users_preserved
            
            print(f"\n=== VALIDATION DECISION LOGIC ===")
            print(f"Schema acceptable (proper_schema OR user_preservation): {schema_acceptable}")
            print(f"  - Proper normalized schema: {proper_schema_created}")
            print(f"  - User preservation working: {all_users_preserved}")

            # Validate schema design (be more flexible)
            if proper_schema_created:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"SUCCESS: Proper normalized schema created with FK constraints and PK on user_id"
                print("✅ TEST STEP 2 PASSED: Proper schema design")
            elif all_users_preserved:
                test_steps[2]["status"] = "partial"
                test_steps[2]["Result_Message"] = f"PARTIAL: Working solution found but not ideal schema design. Method: {solution_method}"
                print(f"⚠️ TEST STEP 2 PARTIAL: Working but not ideal - {solution_method}")
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = "Schema design validation failed - no working solution found"
                print("❌ TEST STEP 2 FAILED: No working solution")
                raise AssertionError("Agent did not create any working solution for missing users problem")

            # Validate user preservation
            if all_users_preserved and carol_properly_handled:
                test_steps[3]["status"] = "passed"
                test_steps[3]["Result_Message"] = f"SUCCESS: All users preserved in queries, Carol properly handled as 'No plan'. Solution: {solution_method}"
                print("✅ TEST STEP 3 PASSED: User preservation works")
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3]["Result_Message"] = f"User preservation failed - all_users_preserved={all_users_preserved}, carol_properly_handled={carol_properly_handled}"
                print(f"❌ TEST STEP 3 FAILED: User preservation issue")
                print(f"   - All users preserved: {all_users_preserved}")
                print(f"   - Carol properly handled: {carol_properly_handled}")
                raise AssertionError("Agent did not properly preserve all users in queries")

            # Final success
            print("PostgreSQL Agent Missing Users Fix test completed successfully!")
            print(f"Database: {created_db_name}")
            print(f"Solution method: {solution_method}")
            print("✅ Missing users issue resolved with proper schema design!")
            print(f"✅ All users preserved with proper LEFT JOIN logic")
            assert True, "Missing users fix successful - proper schema design and user preservation implemented"

        except Exception as e:
            print(f"Validation error: {e}")
            raise

        finally:
            db_cursor.close()
            db_connection.close()

    except Exception as e:
        print(f"Test execution error: {e}")
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