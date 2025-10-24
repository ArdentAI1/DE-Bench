import os

# AI Agent task for MySQL Horizontal Sharding implementation
User_Input = """
Our multi-tenant SaaS application has grown to over 5,000 customers and our single MySQL database is becoming a bottleneck. We're seeing performance degradation, and we need to implement horizontal sharding to scale.

Current database structure:
- tenants table (20 tenants in test environment)
- users table (100 users across tenants)
- projects table (50 projects)
- tasks table (150 tasks)

Each tenant's data is completely isolated - they never need to query across tenants.

We need you to implement a 4-shard architecture:

1. Create a central routing database with:
   - A shard_map table that tracks which tenant is on which shard
   - A shard_registry table with connection info for each shard

2. Distribute the tenants across 4 shards using a hash function on tenant_id
   - Try to balance the distribution evenly
   - All of a tenant's data (users, projects, tasks) should be on the same shard

3. Set up the shard databases (can be named shard_01, shard_02, shard_03, shard_04)
   - Each shard should have the same schema structure

4. Create a routing function or view that can look up which shard a tenant is on

5. Optional: If possible, create a procedure to move a tenant from one shard to another (for rebalancing)

6. Create monitoring views to see how tenants are distributed across shards

Make sure each tenant's data is completely isolated to their assigned shard.
"""

# Configuration will be generated dynamically by create_config function
