User_Input = """
Objective:
To measure and report the costs of queries executed on BigQuery for a specific project in an on-demand pricing model, where costs are based on the number of bytes processed.

Parameters to be Provided:
Project ID: The Google Cloud project ID for which the cost analysis will be performed.
Path to Service Account Json File: Ensure the BigQuery client can authenticate and connect to the Google Cloud project.

Tasks:

Calculate Total Cost in USD for the Past Month:

Compute the total cost of all queries executed within the past month.

Weekly and Daily Cost Analysis:
Break down the total cost by week within the past month.
Break down the total cost by day within the past month.
Generate a line chart plot for:
Total cost by week (x-axis: week, y-axis: USD).
Total cost by day (x-axis: day, y-axis: USD).

Top 10 Most Expensive Queries Analysis:
Identify the top 10 most expensive queries based on the number of bytes processed.
Find common tables used in these top 10 queries.
Get the sizes of these common tables in terms of logical bytes.
Output a CSV file with a list of these common tables and their corresponding sizes.

Steps:

Set Up BigQuery Client:
Ensure the Google Cloud Python client library is installed.
Authenticate using a service account key.

Total Monthly Cost Calculation:
Query BigQuery to sum the total bytes processed for all queries in the past month.
Convert the bytes processed to USD using the on-demand pricing rate.

Weekly and Daily Cost Breakdown:
Aggregate the total bytes processed by week and day.
Convert these aggregates to USD.
Create line charts for both weekly and daily costs.

Top 10 Queries Analysis:
Retrieve and rank the top 10 queries by bytes processed.
Identify common tables used in these queries.
Calculate the size of these tables in logical bytes.
Save the table names and their sizes to a CSV file.

Output:
Total cost in USD for the past month.
Line chart of total cost by week.
Line chart of total cost by day.
CSV file with common tables among the top 10 most expensive queries and their sizes.

Please fill in the code from the starter main.py and create python files as you see fit. Output files in Test_Environment directory.
"""

Model_Configs = ""
Output_File_Location = """"""