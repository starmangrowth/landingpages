async def get_client_config(client_id: str, db=None):
    if db is None:
        db = await get_db()
    return await db.clients.find_one({"client_id": client_id, "active": True})

async def validate_client_id(client_id: str, db=None):
    config = await get_client_config(client_id, db)
    return bool(config)

