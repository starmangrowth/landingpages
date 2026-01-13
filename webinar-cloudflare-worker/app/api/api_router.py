from fastapi import APIRouter, HTTPException, Request
from typing import Dict, Any, List, Optional
import httpx
from datetime import datetime, timedelta
import json
import logging
from asyncio import create_task
from app.models.webinar import WebinarRegistration, WebinarDetails, LeadSubmission
from app.db.mongo import get_db
from app.core.client_config import get_client_config, validate_client_id
from urllib.parse import urlparse, parse_qs

router = APIRouter()
logger = logging.getLogger(__name__)

# === ALL HELPERS AND ENDPOINTS FROM YOUR PASTED CODE ===

def serialize_datetime_objects(data):
    if isinstance(data, dict):
        return {key: serialize_datetime_objects(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [serialize_datetime_objects(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    else:
        return data

async def increment_display_counter(db, client_id: str, broadcast_id: str):
    # Your full code here (from paste)

async def get_display_counter(db, client_id: str, broadcast_id: str) -> int:
    # Your full code here

async def fetch_existing_broadcast_subscription(broadcast_id: str, email: str, api_key: str) -> Optional[Dict[str, Any]]:
    # Your full code here

# All @router endpoints - paste your exact implementations
@router.get("/subscriber-count/{client_id}")
async def get_subscriber_count(client_id: str):
    # Paste your full function body

@router.get("/future-broadcasts/{client_id}")
async def get_future_broadcasts(client_id: str):
    # Paste your full function body

@router.get("/upcoming/{client_id}")
async def get_upcoming_webinars(client_id: str):
    # Paste your full function body

@router.post("/register")
async def register_webinar(registration: WebinarRegistration):
    # Paste your ENTIRE long /register function body here (the big one with Google Sheets immediate fire, DB ops, WebinarGeek call, GHL background, counter increment, etc.)

@router.get("/webinars/{client_id}")
async def get_webinars(client_id: str):
    # Paste your full function body

@router.get("/webinar-details/{client_id}/{webinar_id}")
async def get_webinar_details(client_id: str, webinar_id: str):
    # Paste your full function body

@router.post("/submit-lead")
async def submit_lead(lead: LeadSubmission):
    # Paste your full function body

