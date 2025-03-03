from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
from dotenv import load_dotenv
import os

load_dotenv()

uri = os.getenv("MONGODB_URI")

# Create a new client and connect to the server
syncMongoClient = MongoClient(uri, server_api=ServerApi("1"))
asyncMongoClient = AsyncIOMotorClient(uri, server_api=ServerApi("1"))


# Define an async function to send a ping
async def confirmMongoConnection():
    try:
        # Synchronous ping
        syncMongoClient.admin.command("ping")
        # print("Pinged your deployment. You successfully connected to MongoDB Synchronously!")

        # Asynchronous ping
        await asyncMongoClient.admin.command("ping")
        # print("Pinged your deployment. You successfully connected to MongoDb Async")
    except Exception as e:
        print(f"Error occurred: {e}")
