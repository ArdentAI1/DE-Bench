import os

# Read the CSV data file
current_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(current_dir, "products_bulk_data.csv")

with open(csv_file_path, "r", encoding="utf-8") as f:
    CSV_DATA = f.read()

# AI Agent task for MySQL bulk data ingestion with LOAD DATA INFILE
User_Input = f"""
Perform a bulk data ingestion using LOAD DATA INFILE with data from a CSV file, do the following when ingesting the data:

1. Handle duplicate records appropriately
2. Handle the character encoding properly (UTF-8)
3. Set appropriate field and line terminators
4. Validate that all data was imported correctly

**CSV data:**
{CSV_DATA}
"""

# Configuration will be generated dynamically by create_config function
