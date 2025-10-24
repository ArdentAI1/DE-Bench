import os

# AI Agent task for PostgreSQL idempotent upsert pipeline with conflict handling
User_Input = """
You need to implement a crash-resistant, idempotent data pipeline for a customer dimension table that can safely rerun after failures without creating duplicates.

Create a customer dimension table and an ETL pipeline that is crash-resistant, idempotent, and can safely rerun after failures without creating duplicates. It should handle the following scenarios:
1. New customers being added
2. Existing customer updates (email changes, subscription tiers, etc.)
3. Pipeline crashes and reruns without creating duplicates
4. Partial batch failures requiring replay of specific records
5. Rerunning the same batch (should be idempotent - no duplicates)
6. Handling conflicts gracefully when source systems disagree

YOU MUST EXECUTE THESE OPERATIONS TO DEMONSTRATE YOUR PIPELINE:
1. Update Alice's record: set email to 'alice.johnson@newdomain.com' and subscription_tier to 'Enterprise'
2. Rerun the same update for Alice (should be idempotent - no duplicates created)
3. Load new customer Dave with details: customer_id='DAVE_001', first_name='Dave', last_name='Wilson', email='dave@example.com', subscription_tier='Free'
4. Run the pipeline again to verify complete idempotency (no duplicate records should be created)
"""

# Configuration will be generated dynamically by create_config function
