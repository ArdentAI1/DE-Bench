import os
from ardent import ArdentClient, ArdentError
from dotenv import load_dotenv

load_dotenv()



Ardent_Client = ArdentClient(
    public_key=os.getenv("ARDENT_PUBLIC_KEY"),
    secret_key=os.getenv("ARDENT_SECRET_KEY"),
    base_url=os.getenv("ARDENT_BASE_URL"),
)


def test_ardent_connection():
    print("ARDENT_PUBLIC_KEY: ", os.getenv("ARDENT_PUBLIC_KEY"))
    print("ARDENT_SECRET_KEY: ", os.getenv("ARDENT_SECRET_KEY"))
    print("ARDENT_BASE_URL: ", os.getenv("ARDENT_BASE_URL"))
    print("MONGODB_URI: ", os.getenv("MONGODB_URI"))
    try:
        response = Ardent_Client.set_config(
            config_type="mongodb",
            connection_string=os.getenv("MONGODB_URI"),
            databases=[
                {"name": "test_database", "collections": [{"name": "test_collection"}]}
            ],
        )
        print("Ardent connection successful")
    except Exception as e:
        print("Ardent connection failed")
        print(e)
        raise e


if __name__ == "__main__":
    test_ardent_connection()
