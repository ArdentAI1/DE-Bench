#!/usr/bin/env python3
"""
Script to create MongoDB BSON seeds from JSON data.
This script:
1. Imports JSON data to MongoDB (makes it realistic with _id fields)
2. Dumps it as compressed BSON
3. Cleans up temporary database

Usage:
    python scripts/create_mongo_bson_seed.py <json_file> <output_name>
    
Example:
    python scripts/create_mongo_bson_seed.py test_data.json mongodb_add_record_test
"""

import os
import sys
import json
import subprocess
from pymongo import MongoClient
from bson import json_util
from dotenv import load_dotenv

load_dotenv()


def create_bson_seed(json_file: str, output_name: str):
    """Create BSON seed from JSON file by pushing to MongoDB and dumping."""
    
    # Get MongoDB URI
    mongodb_uri = os.getenv("MONGODB_URI")
    if not mongodb_uri:
        print("❌ Error: MONGODB_URI environment variable not set")
        print("   Please set it in your .env file")
        sys.exit(1)
    
    # Check if JSON file exists
    if not os.path.exists(json_file):
        print(f"❌ Error: JSON file not found: {json_file}")
        sys.exit(1)
    
    # Read JSON data (handles both standard JSON and MongoDB Extended JSON)
    print(f"📖 Reading JSON file: {json_file}")
    with open(json_file, 'r') as f:
        json_content = f.read()
        # Use json_util to handle MongoDB Extended JSON ($oid, $date, etc.)
        data = json_util.loads(json_content)
    
    if not isinstance(data, list):
        print("❌ Error: JSON file must contain an array of documents")
        sys.exit(1)
    
    print(f"   Found {len(data)} documents")
    
    # Temp database and collection names
    # Extract collection name from output_name if it contains it, otherwise default to last part
    temp_db = f"temp_seed_{output_name}"
    # For ad_opportunities, use "ad_opportunities"; for others, derive from name
    if "ad_opportunities" in output_name:
        temp_collection = "ad_opportunities"
    elif "user" in output_name.lower():
        temp_collection = "users"
    else:
        # Use the last meaningful part of the name
        temp_collection = output_name.split('_')[-1] if '_' in output_name else "data"
    output_file = f"{output_name}.bson.gz"
    
    print(f"\n🍃 Creating MongoDB BSON seed...")
    print(f"   Output: {output_file}")
    
    # Step 1: Import to MongoDB
    print(f"\n📥 Step 1: Importing to MongoDB...")
    print(f"   Database: {temp_db}")
    print(f"   Collection: {temp_collection}")
    
    client = MongoClient(mongodb_uri)
    db = client[temp_db]
    collection = db[temp_collection]
    
    # Drop existing collection
    collection.drop()
    
    # Insert data
    result = collection.insert_many(data)
    doc_count = len(result.inserted_ids)
    
    print(f"✅ Imported {doc_count} documents to MongoDB")
    
    # Step 2: Dump as BSON
    print(f"\n📦 Step 2: Creating BSON dump...")
    
    cmd = [
        "mongodump",
        f"--uri={mongodb_uri}",
        f"--db={temp_db}",
        f"--collection={temp_collection}",
        f"--archive={output_file}",
        "--gzip"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Error creating BSON dump: {result.stderr}")
        client.drop_database(temp_db)
        sys.exit(1)
    
    # Get file size
    file_size = os.path.getsize(output_file)
    file_size_mb = file_size / (1024 * 1024)
    
    if file_size_mb < 1:
        size_str = f"{file_size / 1024:.2f}KB"
    else:
        size_str = f"{file_size_mb:.2f}MB"
    
    print(f"✅ Created BSON dump: {output_file} ({size_str})")
    
    # Step 3: Cleanup
    print(f"\n🧹 Step 3: Cleaning up temporary database...")
    client.drop_database(temp_db)
    print(f"✅ Cleaned up temporary database: {temp_db}")
    
    client.close()
    
    print(f"\n🎉 BSON seed created successfully!")
    print(f"   File: {output_file}")
    print(f"   Ready to upload to S3")
    
    return output_file


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python scripts/create_mongo_bson_seed.py <json_file> <output_name>")
        print("Example: python scripts/create_mongo_bson_seed.py test_data.json mongodb_add_record_test")
        sys.exit(1)
    
    json_file = sys.argv[1]
    output_name = sys.argv[2]
    
    create_bson_seed(json_file, output_name)

