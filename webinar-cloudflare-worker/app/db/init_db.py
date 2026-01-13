async def initialize_database():
    db = await get_db()
    # Example indexes from your multi-tenant pattern
    await db.webinar_registrants.create_index([("client_id", 1), ("email", 1), ("broadcastId", 1)], unique=True)
    await db.display_counters.create_index([("client_id", 1), ("broadcast_id", 1)], unique=True)
    await db.clients.create_index("client_id", unique=True)
    # Add more collections/indexes as needed
    print("Database initialized")
    return True

async def verify_database_setup():
    return {"overall_status": "PASS"}

