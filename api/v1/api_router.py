from fastapi import APIRouter
from app.api.v1.endpoints import posts, pages, contact, settings
from app.api.v1.endpoints.webinar import routes as webinar_routes
from app.api.v1.endpoints.webinar import db_routes as webinar_db_routes
from app.api.v1.endpoints.scheduler import routes as scheduler_routes
from app.api.v1.endpoints import broadcast
from app.api.v1.endpoints import admin  # NEW: Client management endpoints

api_router = APIRouter(prefix="/v1")

api_router.include_router(posts.router)
api_router.include_router(pages.router)
api_router.include_router(contact.router)
api_router.include_router(settings.router)
api_router.include_router(webinar_routes.router, prefix="/webinar", tags=["Webinar"])
api_router.include_router(webinar_db_routes.router, prefix="/webinar", tags=["Webinar DB"])
api_router.include_router(scheduler_routes.router, prefix="/scheduler", tags=["Scheduler"])
api_router.include_router(broadcast.router, prefix="/broadcast", tags=["Broadcast"])
api_router.include_router(admin.router, prefix="/admin", tags=["Admin"])  # NEW: Client management