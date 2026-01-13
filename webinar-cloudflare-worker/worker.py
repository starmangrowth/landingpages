import os
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.api_router import api_router

load_dotenv()

app = FastAPI(title="GC Website Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change to your domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

@app.get("/api/health")
async def health_check():
    return {
        "status": "ok",
        "env_vars": {
            "mongodb_url": bool(os.getenv("MONGODB_URL")),
        },
        "multi_tenant": {
            "client_config_source": "mongodb.clients",
            "global_integration_env_vars_expected": False,
        },
    }

# Scheduled jobs handler (replaces APScheduler)
async def scheduled(event):
    from app.db.init_db import initialize_database
    from app.core.webinar_sync import sync_webinars
    from app.core.retry_failed_webhooks import retry_failed_webhooks

    print("Running scheduled jobs...")
    await initialize_database()  # Ensure collections/indexes
    await sync_webinars()
    await retry_failed_webhooks()
    print("Scheduled jobs completed.")

# Cloudflare exports
export = app  # For HTTP requests
export.scheduled = scheduled  # For cron triggers
