async def sync_webinars():
    db = await get_db()
    clients = await db.clients.find({"active": True}).to_list(None)
    for client in clients:
        api_key = client.get("webinar_geek_api_key")
        if api_key:
            # Your sync logic: fetch broadcasts, update upcoming-broadcast, etc.
            print(f"Synced for client {client['client_id']}")
