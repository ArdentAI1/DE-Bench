
import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError,ClientError
from dotenv import load_dotenv

load_dotenv()



def Create_AWS_Client(service_name):




    aws_access_key_id = os.getenv('ACCESS_KEY_ID_AWS')
    aws_secret_access_key = os.getenv('SECRET_ACCESS_KEY_AWS')
    aws_region = os.getenv('REGION_AWS')
    

    # Create and return the appropriate client based on the service name
    try:
        client = boto3.client(
            service_name,
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )


        return client
    except (NoCredentialsError, PartialCredentialsError) as e:
        raise EnvironmentError("Invalid AWS credentials.") from e
    except ClientError as e:
        raise RuntimeError(f"AWS client error: {e}") from e
    except Exception as e:
        raise RuntimeError(f"An error occurred: {e}") from e
    


s3_client = Create_AWS_Client('s3')
