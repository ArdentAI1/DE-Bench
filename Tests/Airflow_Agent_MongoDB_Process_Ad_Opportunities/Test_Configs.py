import os

User_Input = """Write me an Airflow pipeline that processes the user agent and client IP fields in each record in the ad_opportunities collection in MongoDB wherever it's not known. 

The goal for each record is to run these fields through placeholder functions. For client IP, output: state, country, city, longitude, and latitude. For user agent, output: browser, OS, and device. 

The results should be placed into a new collection 'ad_opportunities_processed' with these fields pre-processed. 

Then write a function to flatten the records so they can be entered into ClickHouse."""

# Configuration will be generated dynamically by create_config function

