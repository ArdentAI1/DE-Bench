User_Input = """
Access the MySQL database to retrieve access tokens for Plaid and Finch. Use these access tokens to fetch transaction data, 
then transform and store this data in the TigerBeetle database as transactions, correctly writing the data to tigerbeetle.
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