#run it with uvicorn app.main:app --reload  
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from app.api.api_router import api_router
import os
from dotenv import load_dotenv
from app.core.scheduler import init_scheduler, shutdown
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Load environment variables from .env file
load_dotenv()

app = FastAPI(title="GC Website Backend", version="1.0.0")

# CORS setup (adjust origins as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change to your frontend domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

@app.get("/api/health")
def health_check():
    """
    Health check endpoint.

    MULTI-TENANT NOTE:
    We intentionally do NOT report global integration env vars (WEBINAR_GEEK_API_KEY, etc)
    because integrations are configured per-client in the `clients` collection.
    """
    return {
        "status": "ok",
        "env_vars": {
            "mongodb_url": bool(os.environ.get("MONGODB_URL")),
        },
        "multi_tenant": {
            "client_config_source": "mongodb.clients",
            "global_integration_env_vars_expected": False,
        },
    }

async def startup_event():
    """Initialize components on application startup"""
    logger = logging.getLogger(__name__)
    
    # Initialize database and collections first
    logger.info("🚀 Starting database initialization...")
    try:
        from app.db.init_db import initialize_database, verify_database_setup
        
        # Initialize database collections and indexes
        init_success = await initialize_database()
        if init_success:
            logger.info("✅ Database initialization completed successfully")
        else:
            logger.warning("⚠️ Database initialization completed with warnings")
        
        # Verify database setup
        verification = await verify_database_setup()
        if verification.get('overall_status') == '✅ PASS':
            logger.info("✅ Database verification passed")
        else:
            logger.warning(f"⚠️ Database verification status: {verification.get('overall_status')}")
            
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {str(e)}")
        # Continue startup even if DB init fails (for development)
    
    # Initialize the scheduler
    logger.info("🔧 Initializing scheduler...")
    init_scheduler()

    # Automatically register webinar sync job if not already registered
    from app.core.scheduler import scheduler, add_job
    from app.core.webinar_sync import sync_webinars
    from app.core.retry_failed_webhooks import retry_failed_webhooks

    WEBINAR_SYNC_JOB_ID = "webinar_sync_job"
    RETRY_WEBHOOKS_JOB_ID = "retry_webhooks_job"
    
    # Auto-register webinar sync job
    if not scheduler.get_job(WEBINAR_SYNC_JOB_ID):
        added = add_job(
            job_id=WEBINAR_SYNC_JOB_ID,
            func=sync_webinars,
            trigger="interval",
            minutes=40 # every 40 minutes
        )
        if added:
            logging.getLogger(__name__).info("Webinar sync job registered automatically at startup (every 40 minutes)")
            # Run sync immediately on startup to populate broadcasts
            import asyncio
            asyncio.create_task(sync_webinars())
            logging.getLogger(__name__).info("🚀 Running initial webinar sync on startup...")
        else:
            logging.getLogger(__name__).error("Failed to auto-register webinar sync job at startup")
    else:
        logging.getLogger(__name__).info("Webinar sync job already registered")
    
    # Auto-register retry webhooks job
    if not scheduler.get_job(RETRY_WEBHOOKS_JOB_ID):
        added = add_job(
            job_id=RETRY_WEBHOOKS_JOB_ID,
            func=retry_failed_webhooks,
            trigger="interval",
            minutes=2  # every 5 minutes
        )
        if added:
            logging.getLogger(__name__).info("Retry webhooks job registered automatically at startup (every 2 minutes)")
        else:
            logging.getLogger(__name__).error("Failed to auto-register retry webhooks job at startup")
    else:
        logging.getLogger(__name__).info("Retry webhooks job already registered")
    
async def shutdown_event():
    """Clean up resources on application shutdown"""
    from app.db.mongo import client
    
    # Shutdown the scheduler gracefully
    shutdown()
    
    # Close MongoDB connections
    try:
        client.close()
        logger.info("MongoDB connections closed successfully")
    except Exception as e:
        logger.error(f"Error closing MongoDB connections: {str(e)}")

app.add_event_handler("startup", startup_event)
app.add_event_handler("shutdown", shutdown_event)


#run it with uvicorn app.main:app --reload   