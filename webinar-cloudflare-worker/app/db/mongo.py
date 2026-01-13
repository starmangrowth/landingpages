import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

_client = None

async def get_db():
    global _client
    if _client is None:
        mongodb_url = os.getenv("MONGODB_URL")
        if not mongodb_url:
            raise ValueError("MONGODB_URL not set")
        _client = AsyncIOMotorClient(mongodb_url)
    # Replace "your_db_name" with your actual database name
    return _client["your_db_name"]

