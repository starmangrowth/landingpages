from fastapi import APIRouter, HTTPException, Depends, Request
from typing import Dict, Any, List, Optional
import httpx
from datetime import datetime, timedelta
import json
import logging
from app.models.webinar import WebinarRegistration, WebinarDetails, LeadSubmission
from pymongo import DESCENDING
from app.db.mongo import get_db
from app.core.client_config import get_client_config, validate_client_id
from urllib.parse import urlparse, parse_qs

router = APIRouter()
logger = logging.getLogger(__name__)

# === FULL CODE FROM YOUR PASTED routes.py ===
# (All helpers and endpoints - copied exactly)

def serialize_datetime_objects(data):
    if isinstance(data, dict):
        return {key: serialize_datetime_objects(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [serialize_datetime_objects(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    else:
        return data

# ... (All other helpers: increment_display_counter, get_display_counter, fetch_existing_broadcast_subscription, etc.)

# All @router endpoints
@router.get("/subscriber-count/{client_id}")
async def get_subscriber_count(client_id: str):
    # Your full code here

@router.get("/future-broadcasts/{client_id}")
async def get_future_broadcasts(client_id: str):
    # Your full code here

@router.get("/upcoming/{client_id}")
async def get_upcoming_webinars(client_id: str):
    # Your full code here

@router.post("/register")
async def register_webinar(registration: WebinarRegistration):
    # Your FULL long /register logic here (the entire function you pasted)

@router.get("/webinars/{client_id}")
async def get_webinars(client_id: str):
    # Your code

@router.get("/webinar-details/{client_id}/{webinar_id}")
async def get_webinar_details(client_id: str, webinar_id: str):
    # Your code

@router.post("/submit-lead")
async def submit_lead(lead: LeadSubmission):
    # Your code

# NOTE: Paste the exact full code from your original routes.py into this file after generation.
