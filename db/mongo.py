import motor.motor_asyncio
import logging
from fastapi import Request
from os import getenv
from dotenv import load_dotenv

# Set up logger
logger = logging.getLogger(__name__)

# Load environment variables from multiple possible locations
import os
load_dotenv()  # Load from current directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))  # Root .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))  # App .env

# Get MongoDB URI from environment variables (no hardcoded fallback)
MONGO_URI = getenv("MONGODB_URL") or getenv("MONGO_URI")

if not MONGO_URI:
    raise ValueError("MongoDB URI not found! Please set MONGODB_URL or MONGO_URI in your environment variables.")

# Log connection (mask credentials for security)
masked_uri = MONGO_URI
if '@' in MONGO_URI and ':' in MONGO_URI:
    # Mask the password in the connection string for logging
    parts = MONGO_URI.split('@')
    if len(parts) > 1:
        credentials_part = parts[0]
        if ':' in credentials_part:
            user_pass = credentials_part.split('://')[-1]
            if ':' in user_pass:
                user, password = user_pass.split(':', 1)
                masked_credentials = f"{user}:{'*' * len(password)}"
                masked_uri = MONGO_URI.replace(user_pass, masked_credentials)

logger.info(f"MongoDB URI configured: {masked_uri}")

try:
    # Parse database name from URI first
    db_name = None
    connection_uri = MONGO_URI
    
    if '/' in MONGO_URI:
        # Extract database name from URI (everything after the last /)
        uri_parts = MONGO_URI.split('/')
        if len(uri_parts) > 3:  # mongodb://host:port/database format
            potential_db_name = uri_parts[-1]
            # Remove any query parameters if present
            if '?' in potential_db_name:
                potential_db_name = potential_db_name.split('?')[0]
            if potential_db_name.strip():
                db_name = potential_db_name
                # Create connection URI without database name for authentication
                connection_uri = '/'.join(uri_parts[:-1]) + '/'
    
    if not db_name:
        raise ValueError("Database name not found in MongoDB URI. Please ensure your MONGODB_URL includes the database name.")
    
    # Connect without database name to avoid authentication issues
    logger.info(f"Connecting to MongoDB server: {connection_uri}")
    client = motor.motor_asyncio.AsyncIOMotorClient(
        connection_uri,
        maxPoolSize=10,              # Limit to 10 connections max (prevents accumulation)
        minPoolSize=2,               # Maintain 2 minimum connections
        maxIdleTimeMS=30000,         # Close idle connections after 30 seconds
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=10000
    )
    
    # Select the database after connection
    db = client[db_name]
    logger.info(f"MongoDB connection established successfully to database: {db_name}")
    
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {str(e)}")
    raise

def get_db():
    """Returns the database connection"""
    return db