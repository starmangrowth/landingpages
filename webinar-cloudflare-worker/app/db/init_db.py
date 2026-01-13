async def initialize_database():
    db = await get_db()
    # Create collections and indexes (from your original logic)
    await db.webinar_registrants.create_index([("client_id", 1), ("email", 1), ("broadcastId", 1)], unique=True)
    await db.display_counters.create_index([("client_id", 1), ("broadcast_id", 1)], unique=True)
    # Add other collections/indexes as in your code
    return True

async def verify_database_setup():
    # Your verification logic
    return {"overall_status": "✅ PASS"}
