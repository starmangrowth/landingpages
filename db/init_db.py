#!/usr/bin/env python3
"""
Database initialization module for Growth Club backend.
This module ensures that the database and all necessary collections are created
when the backend starts, if they don't already exist.

Created: September 25, 2025
"""

import logging
from datetime import datetime, timezone
from pymongo.errors import PyMongoError
from app.db.mongo import get_db

# Set up logger
logger = logging.getLogger(__name__)

# Define all collections needed by the application
REQUIRED_COLLECTIONS = [
    # NEW: Clients collection for multi-tenant support
    {
        "name": "clients",
        "description": "Stores client configurations for multi-tenant support (API keys, webhooks, settings)",
        "indexes": [
            {"keys": [("client_id", 1)], "unique": True},  # Unique client identifier (slug)
            {"keys": [("status", 1)], "unique": False},
            {"keys": [("created_at", 1)], "unique": False}
        ]
    },
    {
        "name": "webinar_registrants",
        "description": "Stores webinar registration data",
        "indexes": [
            {"keys": [("client_id", 1)], "unique": False},  # Multi-tenant index
            {"keys": [("email", 1)], "unique": False},
            {"keys": [("broadcastId", 1)], "unique": False},
            {"keys": [("client_id", 1), ("email", 1), ("broadcastId", 1)], "unique": True},  # Prevent duplicate registrations per client
            {"keys": [("client_id", 1), ("email", 1)], "unique": False},  # Client-specific email lookups
            {"keys": [("createdAt", 1)], "unique": False},
            {"keys": [("status.webinarGeekSent", 1)], "unique": False},
            {"keys": [("status.ghlSent", 1)], "unique": False},
            {"keys": [("status.googleSheetsSent", 1)], "unique": False}
        ]
    },
    {
        "name": "broadcasts",
        "description": "Stores broadcast data from WebinarGeek API",
        "indexes": [
            {"keys": [("client_id", 1)], "unique": False},  # Multi-tenant index
            {"keys": [("client_id", 1), ("broadcast_id", 1)], "unique": True},  # Unique broadcast per client
            {"keys": [("client_id", 1), ("date", -1)], "unique": False},  # Client broadcasts by date
            {"keys": [("date", 1)], "unique": False},
            {"keys": [("has_ended", 1)], "unique": False},
            {"keys": [("cancelled", 1)], "unique": False},
            {"keys": [("last_synced", 1)], "unique": False}
        ]
    },
    {
        "name": "upcoming-broadcast",
        "description": "Stores the current upcoming broadcast per client (one document per client)",
        "indexes": [
            {"keys": [("client_id", 1)], "unique": True},  # One upcoming broadcast per client
            {"keys": [("broadcast_id", 1)], "unique": False},
            {"keys": [("last_synced", 1)], "unique": False}
        ]
    },
    {
        "name": "broadcast_sync_info",
        "description": "Stores synchronization metadata for broadcast syncing per client",
        "indexes": [
            {"keys": [("client_id", 1)], "unique": True},  # One sync info per client
            {"keys": [("timestamp", 1)], "unique": False}
        ]
    },
    {
        "name": "display_counters",
        "description": "Stores local registration counters for display on frontend (per client per broadcast)",
        "indexes": [
            {"keys": [("client_id", 1), ("broadcast_id", 1)], "unique": True},  # Unique per client per broadcast
            {"keys": [("client_id", 1)], "unique": False},
            {"keys": [("last_updated", 1)], "unique": False}
        ]
    }
]


async def collection_exists(db, collection_name):
    """
    Check if a collection exists in the database.
    
    Args:
        db: MongoDB database connection
        collection_name (str): Name of the collection to check
        
    Returns:
        bool: True if collection exists, False otherwise
    """
    try:
        collections = await db.list_collection_names()
        return collection_name in collections
    except Exception as e:
        logger.error(f"Error checking if collection '{collection_name}' exists: {str(e)}")
        return False

async def create_collection_with_indexes(db, collection_config):
    """
    Create a collection with its required indexes if it doesn't exist.
    
    Args:
        db: MongoDB database connection
        collection_config (dict): Collection configuration with name, description, and indexes
        
    Returns:
        bool: True if successful, False otherwise
    """
    collection_name = collection_config["name"]
    description = collection_config.get("description", "")
    indexes = collection_config.get("indexes", [])
    
    try:
        # Check if collection already exists
        if await collection_exists(db, collection_name):
            logger.info(f"✅ Collection '{collection_name}' already exists")
            
            # Still try to create indexes in case they're missing
            collection = db[collection_name]
            for index_config in indexes:
                try:
                    keys = index_config["keys"]
                    options = {k: v for k, v in index_config.items() if k != "keys"}
                    await collection.create_index(keys, **options)
                    logger.debug(f"✅ Index {keys} ensured for '{collection_name}'")
                except Exception as e:
                    # Index might already exist, this is not critical
                    logger.debug(f"Index {keys} for '{collection_name}': {str(e)}")
            
            return True
        
        # Create the collection
        logger.info(f"🔄 Creating collection '{collection_name}': {description}")
        collection = db[collection_name]
        
        # Insert a temporary document to actually create the collection
        temp_doc = {
            "_temp_init": True,
            "created_at": datetime.now(timezone.utc),
            "purpose": description
        }
        await collection.insert_one(temp_doc)
        
        # Remove the temporary document
        await collection.delete_one({"_temp_init": True})
        
        logger.info(f"✅ Collection '{collection_name}' created successfully")
        
        # Create indexes
        for index_config in indexes:
            try:
                keys = index_config["keys"]
                options = {k: v for k, v in index_config.items() if k != "keys"}
                await collection.create_index(keys, **options)
                logger.info(f"✅ Index {keys} created for '{collection_name}'")
            except Exception as e:
                logger.warning(f"Failed to create index {keys} for '{collection_name}': {str(e)}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create collection '{collection_name}': {str(e)}")
        return False

async def initialize_database():
    """
    Initialize the database by creating all required collections and indexes.
    This function is safe to call multiple times - it only creates what's missing.
    
    Returns:
        bool: True if initialization completed successfully, False otherwise
    """
    start_time = datetime.now(timezone.utc)
    logger.info("🚀 Starting database initialization...")
    
    try:
        # Get database connection
        db = get_db()
        
        # Get current database name
        db_name = db.name
        logger.info(f"📊 Initializing database: {db_name}")
        
        # Track initialization results
        success_count = 0
        error_count = 0
        
        # Create each required collection
        for collection_config in REQUIRED_COLLECTIONS:
            try:
                if await create_collection_with_indexes(db, collection_config):
                    success_count += 1
                else:
                    error_count += 1
            except Exception as e:
                logger.error(f"❌ Error initializing collection '{collection_config['name']}': {str(e)}")
                error_count += 1
        
        # MULTI-TENANT NOTE:
        # Do NOT create a global/legacy broadcast_sync_info document (e.g. _id=last_sync).
        # Sync info is stored per-client with { client_id: <id>, ... } and will be created
        # by the sync job on first run for each client.
        
        # Log results
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        if error_count == 0:
            logger.info(f"🎉 Database initialization completed successfully!")
            logger.info(f"📊 Results: {success_count} collections initialized in {duration:.2f}s")
            return True
        else:
            logger.warning(f"⚠️ Database initialization completed with errors!")
            logger.warning(f"📊 Results: {success_count} successful, {error_count} errors in {duration:.2f}s")
            return False
        
    except PyMongoError as e:
        logger.error(f"❌ MongoDB error during database initialization: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error during database initialization: {str(e)}")
        return False

async def verify_database_setup():
    """
    Verify that all required collections exist and have basic functionality.
    
    Returns:
        dict: Verification results with details about each collection
    """
    logger.info("🔍 Verifying database setup...")
    
    try:
        db = get_db()
        verification_results = {
            "database_name": db.name,
            "collections": {},
            "overall_status": "unknown"
        }
        
        all_good = True
        
        for collection_config in REQUIRED_COLLECTIONS:
            collection_name = collection_config["name"]
            
            try:
                # Check if collection exists
                exists = await collection_exists(db, collection_name)
                
                if exists:
                    # Test basic operations
                    collection = db[collection_name]
                    
                    # Count documents
                    doc_count = await collection.count_documents({})
                    
                    # List indexes
                    indexes = await collection.list_indexes().to_list(None)
                    index_names = [idx.get("name", "unknown") for idx in indexes]
                    
                    verification_results["collections"][collection_name] = {
                        "exists": True,
                        "document_count": doc_count,
                        "indexes": index_names,
                        "status": "✅ OK"
                    }
                    
                    logger.info(f"✅ {collection_name}: {doc_count} documents, {len(index_names)} indexes")
                    
                else:
                    verification_results["collections"][collection_name] = {
                        "exists": False,
                        "status": "❌ MISSING"
                    }
                    logger.error(f"❌ {collection_name}: Collection does not exist")
                    all_good = False
                    
            except Exception as e:
                verification_results["collections"][collection_name] = {
                    "exists": "unknown",
                    "error": str(e),
                    "status": "⚠️ ERROR"
                }
                logger.error(f"⚠️ {collection_name}: Error during verification - {str(e)}")
                all_good = False
        
        verification_results["overall_status"] = "✅ PASS" if all_good else "❌ FAIL"
        
        if all_good:
            logger.info("🎉 Database verification completed successfully!")
        else:
            logger.warning("⚠️ Database verification found issues!")
        
        return verification_results
        
    except Exception as e:
        logger.error(f"❌ Error during database verification: {str(e)}")
        return {
            "database_name": "unknown",
            "collections": {},
            "overall_status": "❌ ERROR",
            "error": str(e)
        }

if __name__ == "__main__":
    """
    Run database initialization when script is executed directly.
    """
    import asyncio
    
    async def main():
        # Set up logging for direct execution
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Initialize database
        init_success = await initialize_database()
        
        # Verify setup
        verification_results = await verify_database_setup()
        
        print("\n" + "="*60)
        print("DATABASE INITIALIZATION SUMMARY")
        print("="*60)
        print(f"Initialization: {'SUCCESS' if init_success else 'FAILED'}")
        print(f"Verification: {verification_results.get('overall_status', 'UNKNOWN')}")
        print(f"Database: {verification_results.get('database_name', 'UNKNOWN')}")
        print("\nCollection Status:")
        for name, info in verification_results.get('collections', {}).items():
            print(f"  {name}: {info.get('status', 'UNKNOWN')}")
        print("="*60)
        
        return init_success and verification_results.get('overall_status') == '✅ PASS'
    
    # Run the main function
    success = asyncio.run(main())
    exit(0 if success else 1)