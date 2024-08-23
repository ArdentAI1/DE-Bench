import os
from dotenv import load_dotenv


User_Input = "Take the file test.pdf and upload it to the de-bench-testing s3 bucket"

Configs = f"""

Here are the configs for AWS: 

Access Key: {os.getenv("ACCESS_KEY_ID_AWS")}

Secret Key:{os.getenv("SECRET_ACCESS_KEY_AWS")}

Region: {os.getenv("REGION_AWS")}

"""