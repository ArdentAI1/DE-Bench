import os
from dotenv import load_dotenv

load_dotenv()

"""
Configuration for PostgreSQL Database-Side Deduplication Test

This test verifies that AI agents can create database-side deduplication logic
using SQL stored procedures instead of processing data in Airflow containers.
The computation happens entirely within the PostgreSQL database for optimal performance.
"""

# Task description for the AI agent
User_Input = """
Create an Airflow DAG that:
1. Creates a stored procedure called 'deduplicate_users' that deduplicates users into a single user table called 'deduplicated_users'
2. Runs the stored procedure daily at midnight
3. Has a single task named 'deduplicate_users'
4. Name the DAG 'user_deduplication_dag'
5. Create a new feature branch called 'feature/BRANCH_NAME'
6. Create a pull request named 'PR_NAME'
"""

# Configuration will be generated dynamically by create_model_inputs function
