User_Input = """
Objective:
Set up a streaming solution to process real-time data from the given Kafka topic and write the processed data to a PostgreSQL database. Assume the PostgreSQL instance is not set up yet.

Parameters to be Provided:

Kafka Topic:

    Topic Name: Provided by the user (e.g., sensor_data)
    Message Format: JSON (schema to be defined based on user input)
    Example Message: Provided by the user
    PostgreSQL Configuration:

Connection details (host, port, database name, user, password)
Database table schema: Provided by the user

Tasks:

Set up a PostgreSQL instance.
Create a database and the required table.
Consume messages from the specified Kafka topic.
Filter out data based on a specified condition.
Compute an aggregate metric over a defined time window.
Write the processed data to the specified PostgreSQL table.
Steps:

PostgreSQL Setup:

Install PostgreSQL and create a new instance.
Create a new database.
Create the table with the provided schema.
Kafka Consumer Configuration:

Set up the configuration to consume data from the specified Kafka topic.

Data Processing Logic:

Parse the JSON messages.
Apply windowing and compute the specified aggregate metric.

PostgreSQL Sink Configuration:

Configure the sink to write the aggregated results into the specified PostgreSQL table.
Output:

Files to be Created:
streaming_solution.py: Python script containing the streaming solution code.
setup_postgresql.sql: SQL script to set up the PostgreSQL database and table.
Any configuration files necessary for Kafka and PostgreSQL connectivity.
Ensure all files are placed in the Test_Environment directory.


Please fill in solution code with comments and create files as you see fit. Output files in Test_Environment directory.
"""

Model_Configs = ""
Output_File_Location = """"""