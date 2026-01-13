import os
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from app.api.api_router import router as api_router
from workers import WorkerEntrypoint  # Official import
import asgi  # ASGI adapter

load_dotenv()

app = FastAPI(title="GC Website Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

@app.get("/api/health")
async def health_check():
    return {
        "status": "ok",
        "env_vars": {"mongodb_url": bool(os.getenv("MONGODB_URL"))},
        "multi_tenant": {"client_config_source": "mongodb.clients"},
    }

class MyWorker(WorkerEntrypoint):
    async def fetch(self, request):
        return await asgi.fetch(app, request, self.env)

    async def scheduled(self, event):
        from app.db.init_db import initialize_database
        from app.core.webinar_sync import sync_webinars
        from app.core.retry_failed_webhooks import retry_failed_webhooks

        print("Starting scheduled jobs...")
        await initialize_database()
        await sync_webinars()
        await retry_failed_webhooks()
        print("Scheduled jobs completed.")

export = MyWorker()
