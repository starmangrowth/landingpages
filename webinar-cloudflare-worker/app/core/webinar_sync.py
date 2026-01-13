async def sync_webinars():
    from app.db.mongo import get_db
    db = await get_db()
    clients = await db.clients.find({"active": True}).to_list(None)
    for client in clients:
        api_key = client.get("webinar_geek_api_key")
        if api_key:
            # Your sync logic here: fetch broadcasts, update upcoming-broadcast collection, etc.
            print(f"Synced broadcasts for client {client['client_id']}")

