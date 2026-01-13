import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

client = None

async def get_db():
    global client
    if client is None:
        mongodb_url = os.getenv("MONGODB_URL")
        if not mongodb_url:
            raise ValueError("MONGODB_URL environment variable not set")
        client = AsyncIOMotorClient(mongodb_url)
    return client.get_default_database()  # Or specify db name: client["your_db_name"]
