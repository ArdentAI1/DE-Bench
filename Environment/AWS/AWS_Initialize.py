import sys
import os
from dotenv import load_dotenv

load_dotenv()

import boto3
from botocore.exceptions import ClientError

from Functions.AWS.AWS_Client import s3_client


def initialize_aws():
    # set up the testing bucket
    try:
        # Set the bucket creation parameters
        bucket_params = {"Bucket": "de-bench-testing"}

        # Specify the region if provided

        bucket_params["CreateBucketConfiguration"] = {
            "LocationConstraint": os.getenv("REGION_AWS")
        }

        # Create the S3 bucket
        response = s3_client.create_bucket(**bucket_params)

        print(f"Bucket {'de-bench-testing'} created successfully.")
        return response

    except ClientError as e:
        print(f"Error creating bucket: {e}")
        raise
