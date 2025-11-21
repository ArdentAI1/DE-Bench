#!/usr/bin/env python3
"""
Script to upload MongoDB BSON seeds to S3.

Usage:
    python scripts/upload_mongo_seed_to_s3.py <bson_file>
    
Example:
    python scripts/upload_mongo_seed_to_s3.py mongodb_add_record_test.bson.gz
"""

import os
import sys
import boto3
from botocore.exceptions import ClientError


def upload_to_s3(bson_file: str):
    """Upload BSON file to S3."""
    
    # S3 configuration
    s3_bucket = "de-bench"
    s3_path = "Mongodb_Seeds"
    
    # Check if BSON file exists
    if not os.path.exists(bson_file):
        print(f"❌ Error: BSON file not found: {bson_file}")
        sys.exit(1)
    
    # Get file info
    file_name = os.path.basename(bson_file)
    file_size = os.path.getsize(bson_file)
    file_size_mb = file_size / (1024 * 1024)
    
    if file_size_mb < 1:
        size_str = f"{file_size / 1024:.2f}KB"
    else:
        size_str = f"{file_size_mb:.2f}MB"
    
    s3_key = f"{s3_path}/{file_name}"
    s3_uri = f"s3://{s3_bucket}/{s3_key}"
    
    print(f"☁️  Uploading MongoDB BSON seed to S3...")
    print(f"   File: {bson_file} ({size_str})")
    print(f"   Destination: {s3_uri}")
    print()
    
    # Upload to S3
    try:
        s3_client = boto3.client('s3')
        
        s3_client.upload_file(
            bson_file,
            s3_bucket,
            s3_key,
            ExtraArgs={'ContentType': 'application/gzip'}
        )
        
        print("✅ Upload complete!")
        
        # Get S3 object info
        response = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        s3_size = response['ContentLength']
        s3_size_mb = s3_size / (1024 * 1024)
        
        if s3_size_mb < 1:
            s3_size_str = f"{s3_size / 1024:.2f}KB"
        else:
            s3_size_str = f"{s3_size_mb:.2f}MB"
        
        print()
        print("📋 S3 Details:")
        print(f"   Location: {s3_uri}")
        print(f"   Size: {s3_size_str}")
        print(f"   Last Modified: {response['LastModified']}")
        
        print()
        print("🎉 BSON seed is now available at:")
        print(f"   {s3_uri}")
        
        print()
        print("💡 Use in test config:")
        print('   "s3_config": {')
        print('       "bucket_url": "s3://de-bench/",')
        print(f'       "s3_key": "{s3_key}",')
        print('       "aws_key_id": "env:AWS_ACCESS_KEY",')
        print('       "aws_secret_key": "env:AWS_SECRET_KEY"')
        print('   }')
        
    except ClientError as e:
        print(f"❌ Error uploading to S3: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/upload_mongo_seed_to_s3.py <bson_file>")
        print("Example: python scripts/upload_mongo_seed_to_s3.py mongodb_add_record_test.bson.gz")
        sys.exit(1)
    
    bson_file = sys.argv[1]
    upload_to_s3(bson_file)

