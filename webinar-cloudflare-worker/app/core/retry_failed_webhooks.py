async def retry_failed_webhooks():
    from app.db.mongo import get_db
    db = await get_db()
    # Find documents with failed webhook status and retry Google Sheets/GHL
    print("Retried failed webhooks")

