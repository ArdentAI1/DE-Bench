import os
from ardent import ArdentClient, ArdentError
from dotenv import load_dotenv

load_dotenv()


Ardent_Client = ArdentClient(
        public_key=os.getenv("ARDENT_PUBLIC_KEY"), 
        secret_key=os.getenv("ARDENT_SECRET_KEY"),
        base_url=os.getenv("ARDENT_BASE_URL"),
        )


