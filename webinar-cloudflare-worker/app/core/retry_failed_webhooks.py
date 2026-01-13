async def retry_failed_webhooks():
    db = await get_db()
    # Your retry logic: find failed status, resend Google Sheets/GHL webhooks
    print("Retried failed webhooks")
