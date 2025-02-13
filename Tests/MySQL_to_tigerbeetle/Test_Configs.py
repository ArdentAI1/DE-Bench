User_Input = """
Access the MySQL database to retrieve API keys for Plaid and Finch. Use these credentials to fetch transaction data, 
then transform and store this data in the TigerBeetle database.

The MySQL database contains the following tables:
- plaid_access_tokens (company_id, access_token)
- finch_access_tokens (company_id, access_token)

The data should be transformed and stored in TigerBeetle with appropriate transaction records.
"""

Configs = """
MySQL Configuration:
host: localhost
port: 3306
database: test_db
user: test_user
password: test_password

TigerBeetle Configuration:
cluster_id: 0
replica_addresses: ["localhost:3000"]

Plaid API Configuration:
environment: sandbox
client_id: test_client_id
secret: test_secret

Finch API Configuration:
environment: sandbox
client_id: test_client_id
secret: test_secret
""" 