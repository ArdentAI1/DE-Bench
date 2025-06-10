from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
from dotenv import load_dotenv
import os

load_dotenv()

uri = os.getenv("MONGODB_URI")

print("MongoDB URI: ", uri)

# Create a new client and connect to the server
syncMongoClient = MongoClient(uri, server_api=ServerApi("1"))
asyncMongoClient = AsyncIOMotorClient(uri, server_api=ServerApi("1"))


# Define an async function to send a ping
async def confirmMongoConnection():
    try:
        # Synchronous ping
        result = syncMongoClient.admin.command("ping")
        print("Ping result (sync): ", result)
        print(
            "Pinged your deployment. You successfully connected to MongoDB Synchronously!"
        )

        # Asynchronous ping
        result = await asyncMongoClient.admin.command("ping")
        print("Ping result (async): ", result)
        print(
            "Pinged your deployment asynchronously. You successfully connected to MongoDb Async"
        )
    except Exception as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(confirmMongoConnection())
