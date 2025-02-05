import boto3
from botocore.exceptions import ClientError
import os

# Initialize the S3 client (assuming you have set up s3_client in AWS_Client)
from Functions.AWS.AWS_Client import s3_client

def cleanup_aws():
    bucket_name = "de-bench-testing"
    
    try:
        # List all objects in the bucket
        objects = s3_client.list_objects_v2(Bucket=bucket_name).get('Contents', [])
        
        # Delete all objects
        if objects:
            print(f"Deleting objects from bucket '{bucket_name}'...")
            delete_params = {'Bucket': bucket_name, 'Delete': {'Objects': [{'Key': obj['Key']} for obj in objects]}}
            s3_client.delete_objects(**delete_params)
            print(f"Objects deleted from bucket '{bucket_name}'.")

        # Delete the bucket
        print(f"Deleting bucket '{bucket_name}'...")
        s3_client.delete_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' deleted successfully.")
    
    except ClientError as e:
        print(f"Error during cleanup: {e}")
        raise
