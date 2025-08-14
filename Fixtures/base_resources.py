"""
Central import hub for all fixtures.
Import all fixtures here to make them available to tests.
"""

# Test fixtures
from Fixtures.Test.shared_resources import *
from Fixtures.MongoDB.mongo_resources import *
from Fixtures.Databricks.databricks_resources import *
from Fixtures.PostgreSQL.postgres_resources import *
from Fixtures.PostgreSQL.postgres_sql_resource import *

from Fixtures.MySQL.mysql_resources import *
from Fixtures.Airflow.airflow_resources import *
from Fixtures.GitHub.github_resources import *
from Fixtures.Supabase_Account.supabase_account_resource import *

# Future imports for other categories:
# from .Databricks.clusters import databricks_cluster, databricks_client
# from .AWS.s3 import s3_bucket
# from .shared.databases import test_database 