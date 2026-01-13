"""
Webinar registration and management routes.

MULTI-TENANT: All endpoints require client_id for proper data isolation.
Each client has their own WebinarGeek API key, webhook URLs, and display settings
stored in the clients collection.

Updated: January 2026 - Full multi-tenant support
"""

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


def serialize_datetime_objects(data):
    """Convert datetime objects to ISO strings for JSON serialization"""
    if isinstance(data, dict):
        return {key: serialize_datetime_objects(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [serialize_datetime_objects(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    else:
        return data


async def increment_display_counter(db, client_id: str, broadcast_id: str):
    """
    Increment the display counter for a specific client's broadcast.
    
    Args:
        db: MongoDB database connection
        client_id: Client identifier for multi-tenant isolation
        broadcast_id: ID of the broadcast to increment counter for
    """
    if not client_id:
        logger.warning("Cannot increment display counter: missing client_id")
        return
    
    if not broadcast_id or str(broadcast_id) in ["None", "not_available", ""]:
        logger.warning("Cannot increment display counter: invalid broadcast_id")
        return
        
    try:
        result = await db.display_counters.update_one(
            {
                "client_id": client_id,
                "broadcast_id": str(broadcast_id)
            },
            {
                "$inc": {"registration_count": 1},
                "$set": {"last_updated": datetime.utcnow()},
                "$setOnInsert": {
                    "created_at": datetime.utcnow(),
                    "client_id": client_id,
                    "broadcast_id": str(broadcast_id)
                }
            },
            upsert=True
        )
        
        if result.upserted_id:
            logger.info(f"Created new display counter for client {client_id} / broadcast {broadcast_id}")
        else:
            logger.info(f"Incremented display counter for client {client_id} / broadcast {broadcast_id}")
            
    except Exception as e:
        logger.error(f"Error incrementing display counter for client {client_id} / broadcast {broadcast_id}: {str(e)}")
        raise


async def get_display_counter(db, client_id: str, broadcast_id: str) -> int:
    """
    Get the current display counter for a specific client's broadcast.
    
    Args:
        db: MongoDB database connection
        client_id: Client identifier for multi-tenant isolation
        broadcast_id: ID of the broadcast
        
    Returns:
        int: Current display counter value
    """
    if not client_id:
        return 0
    
    if not broadcast_id or str(broadcast_id) in ["None", "not_available", ""]:
        return 0
        
    try:
        counter_doc = await db.display_counters.find_one({
            "client_id": client_id,
            "broadcast_id": str(broadcast_id)
        })
        return counter_doc.get("registration_count", 0) if counter_doc else 0
    except Exception as e:
        logger.error(f"Error getting display counter for client {client_id} / broadcast {broadcast_id}: {str(e)}")
        return 0


async def fetch_existing_broadcast_subscription(
    broadcast_id: str, 
    email: str, 
    api_key: str
) -> Optional[Dict[str, Any]]:
    """
    Fetch existing subscription for a specific broadcast using client's API key.
    
    Args:
        broadcast_id: The broadcast ID
        email: User's email
        api_key: Client-specific WebinarGeek API key
        
    Returns:
        dict: Subscription data or None
    """
    headers = {
        "Api-Token": api_key,
        "Accept": "application/json",
    }
    
    try:
        async with httpx.AsyncClient() as client:
            # Try to get all broadcast subscriptions and filter
            url = f"https://app.webinargeek.com/api/v2/broadcasts/{broadcast_id}/subscriptions"
            
            response = await client.get(url, headers=headers, timeout=15.0)
            
            if response.is_success:
                data = response.json()
                
                all_subs = []
                if isinstance(data, list):
                    all_subs = data
                elif isinstance(data, dict):
                    all_subs = data.get("subscriptions") or data.get("data") or []
                
                # Find matching email
                for sub in all_subs:
                    sub_email = sub.get("email", "")
                    if sub_email.lower() == email.lower():
                        logger.info(f"Found existing subscription for {email}")
                        return {"subscriptions": [sub]}
                
    except Exception as e:
        logger.error(f"Error fetching broadcast subscription: {str(e)}")
    
    return None


@router.get("/subscriber-count/{client_id}")
async def get_subscriber_count(client_id: str):
    """
    Get the current subscriber count for a client's upcoming broadcast.
    Returns base count + actual subscriptions_count from WebinarGeek + display counter.
    
    Args:
        client_id: Client identifier for multi-tenant isolation
        
    Returns:
        dict: Current subscriber count with details
    """
    try:
        db = get_db()
        
        # Get client config for base count
        client_config = await get_client_config(client_id, db)
        if not client_config:
            raise HTTPException(status_code=404, detail=f"Client '{client_id}' not found or inactive")
        
        base_count = client_config.get("base_subscriber_count", 0)
        
        # Fetch the upcoming broadcast for THIS client
        upcoming_broadcast = await db["upcoming-broadcast"].find_one({"client_id": client_id})
        
        if not upcoming_broadcast or not upcoming_broadcast.get("broadcast_id"):
            return {
                "client_id": client_id,
                "total_subscribers": base_count,
                "base_count": base_count,
                "webinar_geek_count": 0,
                "display_counter": 0,
                "broadcast_id": None,
                "message": "No upcoming broadcast found"
            }
        
        # Get counts
        webinar_geek_count = upcoming_broadcast.get("subscriptions_count", 0)
        broadcast_id = upcoming_broadcast.get("broadcast_id")
        display_counter = await get_display_counter(db, client_id, broadcast_id)
        
        # Calculate total
        total_count = base_count + webinar_geek_count + display_counter
        
        return {
            "client_id": client_id,
            "total_subscribers": total_count,
            "base_count": base_count,
            "webinar_geek_count": webinar_geek_count,
            "display_counter": display_counter,
            "broadcast_id": broadcast_id,
            "last_updated": upcoming_broadcast.get("last_synced"),
            "broadcast_date": upcoming_broadcast.get("readable_date")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching subscriber count for client {client_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch subscriber count: {str(e)}")


@router.get("/future-broadcasts/{client_id}")
async def get_future_broadcasts(client_id: str):
    """
    Fetch all future broadcasts for a specific client.
    Returns all broadcasts where has_ended=False and cancelled=False, sorted by date.
    
    Args:
        client_id: Client identifier for multi-tenant isolation
    """
    try:
        db = get_db()
        
        # Validate client
        if not await validate_client_id(client_id, db):
            raise HTTPException(status_code=404, detail=f"Client '{client_id}' not found or inactive")
        
        # Get client config for base count
        client_config = await get_client_config(client_id, db)
        base_count = client_config.get("base_subscriber_count", 0) if client_config else 0
        
        # Get current timestamp
        current_timestamp = int(datetime.now().timestamp())
        
        # Query broadcasts for THIS client
        query = {
            "client_id": client_id,
            "has_ended": False,
            "cancelled": False,
            "date": {"$gt": current_timestamp}
        }
        
        broadcasts_cursor = db["broadcasts"].find(query).sort("date", 1)
        broadcasts = await broadcasts_cursor.to_list(length=100)
        
        logger.info(f"Found {len(broadcasts)} future broadcasts for client {client_id}")
        
        if not broadcasts:
            return {
                "client_id": client_id,
                "webinars": [],
                "total_count": 0,
                "message": "No future broadcasts found",
                "success": True
            }
        
        # Transform broadcasts to webinar format
        webinars = []
        for broadcast in broadcasts:
            broadcast.pop("_id", None)
            
            # Serialize datetime objects
            if "last_synced" in broadcast:
                broadcast["last_synced"] = broadcast["last_synced"].isoformat() if hasattr(broadcast["last_synced"], 'isoformat') else str(broadcast["last_synced"])
            
            # Get display counter
            broadcast_id = broadcast.get("broadcast_id")
            display_counter = await get_display_counter(db, client_id, broadcast_id)
            
            # Calculate subscriber count
            webinar_geek_count = broadcast.get("subscriptions_count", 0)
            total_subscribers = base_count + webinar_geek_count + display_counter
            
            webinar = {
                "webinar_id": broadcast_id,
                "title": f"Broadcast on {broadcast.get('readable_date', 'TBD')}",
                "current_subscribers": total_subscribers,
                "next_broadcast": {
                    "id": broadcast_id,
                    "timestamp": broadcast.get("date"),
                    "date": broadcast.get("readable_date"),
                    "has_ended": broadcast.get("has_ended", False),
                    "cancelled": broadcast.get("cancelled", False),
                    "subscriptions_count": broadcast.get("subscriptions_count", 0),
                    "viewers_count": broadcast.get("viewers_count", 0),
                    "live_viewers_count": broadcast.get("live_viewers_count", 0),
                    "replay_link": broadcast.get("replay_link")
                }
            }
            
            webinars.append(webinar)
        
        return {
            "client_id": client_id,
            "webinars": webinars,
            "total_count": len(webinars),
            "success": True,
            "source": "broadcasts_collection"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching future broadcasts for client {client_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch future broadcasts: {str(e)}")


@router.get("/upcoming/{client_id}")
async def get_upcoming_webinars(client_id: str):
    """
    Fetch upcoming broadcast data for a specific client.
    Returns the latest upcoming broadcast with countdown information.
    
    Args:
        client_id: Client identifier for multi-tenant isolation
    """
    try:
        db = get_db()
        
        # Validate and get client config
        client_config = await get_client_config(client_id, db)
        if not client_config:
            raise HTTPException(status_code=404, detail=f"Client '{client_id}' not found or inactive")
        
        base_count = client_config.get("base_subscriber_count", 0)

        # Fetch the upcoming broadcast for THIS client
        upcoming_broadcast = await db["upcoming-broadcast"].find_one({"client_id": client_id})

        if not upcoming_broadcast or not upcoming_broadcast.get("broadcast_id"):
            return {
                "client_id": client_id,
                "broadcast_id": None,
                "message": "No upcoming broadcasts found",
                "countdown_data": None,
                "webinars": []
            }

        # Remove MongoDB _id and handle datetime serialization
        upcoming_broadcast.pop("_id", None)
        if "last_synced" in upcoming_broadcast:
            upcoming_broadcast["last_synced"] = upcoming_broadcast["last_synced"].isoformat() if hasattr(upcoming_broadcast["last_synced"], 'isoformat') else str(upcoming_broadcast["last_synced"])

        # Calculate countdown data
        countdown_data = None
        broadcast_id = upcoming_broadcast.get("broadcast_id")
        if broadcast_id:
            broadcast_date = upcoming_broadcast.get("date")
            if broadcast_date:
                current_time = datetime.now().timestamp()
                time_remaining = broadcast_date - current_time

                if time_remaining > 0:
                    days = int(time_remaining // (24 * 3600))
                    hours = int((time_remaining % (24 * 3600)) // 3600)
                    minutes = int((time_remaining % 3600) // 60)
                    seconds = int(time_remaining % 60)

                    countdown_data = {
                        "days": days,
                        "hours": hours,
                        "minutes": minutes,
                        "seconds": seconds,
                        "total_seconds": time_remaining,
                        "formatted_time": upcoming_broadcast.get("readable_date")
                    }

        # Calculate subscriber count
        webinar_geek_count = upcoming_broadcast.get("subscriptions_count", 0)
        display_counter = await get_display_counter(db, client_id, broadcast_id)
        total_subscriber_count = base_count + webinar_geek_count + display_counter
        
        # Format response
        webinars = []
        if broadcast_id:
            webinars.append({
                "webinar_id": broadcast_id,
                "title": "Upcoming Broadcast",
                "current_subscribers": total_subscriber_count,
                "next_broadcast": {
                    "id": broadcast_id,
                    "timestamp": upcoming_broadcast.get("date"),
                    "date": upcoming_broadcast.get("readable_date"),
                    "has_ended": upcoming_broadcast.get("has_ended", False),
                    "cancelled": upcoming_broadcast.get("cancelled", False),
                    "subscriptions_count": upcoming_broadcast.get("subscriptions_count", 0),
                    "viewers_count": upcoming_broadcast.get("viewers_count", 0),
                    "live_viewers_count": upcoming_broadcast.get("live_viewers_count", 0),
                    "replay_link": upcoming_broadcast.get("replay_link")
                }
            })

        return {
            "client_id": client_id,
            "broadcast_id": broadcast_id,
            "broadcast_data": upcoming_broadcast,
            "countdown_data": countdown_data,
            "webinars": webinars,
            "subscriber_count": {
                "total_subscribers": total_subscriber_count,
                "base_count": base_count,
                "webinar_geek_count": webinar_geek_count,
                "display_counter": display_counter
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching upcoming broadcast for client {client_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch upcoming broadcast: {str(e)}")


@router.post("/register")
async def register_webinar(registration: WebinarRegistration):
    """
    Register a user for a broadcast with resilient multi-tenant flow:
    
    MULTI-TENANT: Uses client_id from registration to fetch client-specific:
    - WebinarGeek API key
    - Google Sheets webhook URL
    - GHL webhook URL
    - Base subscriber count
    
    Flow:
    1. Validate client_id and fetch client configuration
    2. Fire Google Sheets webhook IMMEDIATELY (before any DB operations)
    3. Attempt DB save (with fallback if DB is down)
    4. Attempt WebinarGeek broadcast registration using client's API key
    5. Return success even if DB/WebinarGeek fails (Google Sheets always gets data)
    """
    import asyncio
    
    try:
        db = get_db()
        client_id = registration.client_id
        
        # STEP 0: Validate client and get configuration
        client_config = await get_client_config(client_id, db)
        if not client_config:
            raise HTTPException(
                status_code=400, 
                detail=f"Client '{client_id}' not found or inactive"
            )
        
        # Extract client-specific credentials
        webinar_geek_api_key = client_config.get("webinar_geek_api_key")
        google_sheet_webhook_url = client_config.get("google_sheet_url")
        ghl_webhook_url = client_config.get("ghl_url")
        base_count = client_config.get("base_subscriber_count", 0)
        
        logger.info(f"Processing registration for client '{client_id}' - {registration.email}")

        # Handle name field: use full name as firstName for WebinarGeek
        if registration.name and not registration.firstName:
            registration.firstName = registration.name.strip()
            name_parts = registration.name.strip().split(maxsplit=1)
            registration.lastName = name_parts[1] if len(name_parts) > 1 else ""

        # Normalize submittedAt
        if not registration.submittedAt:
            registration.submittedAt = datetime.now()

        registration_data = registration.dict()
        registration_data["client_id"] = client_id  # Ensure client_id is in data

        # Get broadcast ID
        broadcast_id = registration_data.get("broadcastId") or registration_data.get("webinarId")
        broadcast_id_str = str(broadcast_id) if broadcast_id is not None else None
        
        # Store valid broadcast ID
        if broadcast_id and str(broadcast_id) not in ["None", "not_available", ""]:
            registration_data["broadcastId"] = str(broadcast_id)
        else:
            registration_data.pop("broadcastId", None)

        # Initialize status flags
        registration_data["status"] = {
            "webinarGeekSent": False,
            "ghlSent": False,
            "googleSheetsSent": False,
            "lastUpdated": datetime.now()
        }

        # ------------------------------------------------------------------
        # IMMEDIATE: Fire Google Sheets webhook FIRST
        # ------------------------------------------------------------------
        
        async def send_google_sheets_webhook_immediate(reg_data: dict, doc_id_ref: list):
            """Send Google Sheets webhook immediately - works independently of DB"""
            if not google_sheet_webhook_url:
                logger.warning(f"⏭️ Google Sheets webhook skipped for client {client_id} - not configured")
                return
            
            try:
                sheet_payload = serialize_datetime_objects(reg_data.copy())
                if "companyName" not in sheet_payload:
                    sheet_payload["companyName"] = None
                sheet_payload.update({
                    "timestamp": datetime.now().isoformat(),
                    "submitted_at": int(datetime.now().timestamp())
                })
                
                # Add channel ID from URL params if present
                channel_id = reg_data.get("id") or reg_data.get("ID")
                if channel_id:
                    sheet_payload["ID"] = channel_id
                
                logger.info(f"📊 Google Sheets webhook sending for {sheet_payload.get('email', 'N/A')} (client: {client_id})")
                
                async with httpx.AsyncClient(follow_redirects=True) as client:
                    sheets_response = await client.post(google_sheet_webhook_url, json=sheet_payload, timeout=120.0)
                    
                    if sheets_response.is_success:
                        sheet_success = False
                        try:
                            response_json = sheets_response.json()
                            
                            if response_json.get('ok') is True:
                                sheet_success = True
                            elif response_json.get('ok') is False and response_json.get('skipped') is True:
                                sheet_success = True
                                logger.info(f"✅ Google Sheets DUPLICATE - {response_json.get('reason', 'Already processed')}")
                            else:
                                sheet_success = True
                                
                        except json.JSONDecodeError:
                            sheet_success = True
                        except Exception as parse_error:
                            logger.error(f"❌ Parse error: {str(parse_error)}")
                            sheet_success = False
                        
                        if sheet_success:
                            logger.info(f"✅ Google Sheets webhook SUCCESS for {reg_data.get('email', 'N/A')} (client: {client_id})")
                            if doc_id_ref[0]:
                                try:
                                    await db.webinar_registrants.update_one(
                                        {"_id": doc_id_ref[0]},
                                        {"$set": {
                                            "status.googleSheetsSent": True,
                                            "status.googleSheetsInProgress": False,
                                            "status.lastUpdated": datetime.now()
                                        }}
                                    )
                                except Exception as db_update_error:
                                    logger.warning(f"⚠️ Google Sheets succeeded but couldn't update DB status: {str(db_update_error)}")
                    else:
                        logger.error(f"❌ Google Sheets webhook failed ({sheets_response.status_code}) for client {client_id}")
                                
            except Exception as e:
                logger.error(f"❌ Google Sheets webhook exception for client {client_id}: {str(e)}")
        
        # Reference to doc_id
        doc_id_ref = [None]
        
        # 🚀 Fire Google Sheets webhook IMMEDIATELY
        google_sheets_task = asyncio.create_task(send_google_sheets_webhook_immediate(registration_data, doc_id_ref))
        logger.info(f"🚀 Google Sheets webhook fired IMMEDIATELY for {registration.email} (client: {client_id})")

        # ------------------------------------------------------------------
        # DB OPERATIONS: Check existing user and save
        # ------------------------------------------------------------------
        existing_doc = None
        db_available = True
        insert_result = None
        
        try:
            # Check for existing registration for THIS client + broadcast
            if broadcast_id and broadcast_id_str not in ["None", "not_available", ""]:
                existing_doc = await db.webinar_registrants.find_one({
                    "client_id": client_id,
                    "email": registration.email,
                    "broadcastId": broadcast_id_str
                })
                
                if existing_doc:
                    logger.info(f"Existing registration found for {registration.email} on broadcast {broadcast_id_str} (client: {client_id})")
            else:
                # No real broadcast ID - check for fallback registration
                existing_doc = await db.webinar_registrants.find_one({
                    "client_id": client_id,
                    "email": registration.email,
                    "$or": [
                        {"broadcastId": {"$exists": False}},
                        {"broadcastId": {"$in": [None, "None", "not_available", ""]}},
                    ],
                })
            
            # Preserve existing WebinarGeek data
            if existing_doc:
                if existing_doc.get("watchLink"):
                    registration_data["watchLink"] = existing_doc.get("watchLink")
                if existing_doc.get("confirmationLink"):
                    registration_data["confirmationLink"] = existing_doc.get("confirmationLink")
                if existing_doc.get("webinarGeekResponse"):
                    registration_data["webinarGeekResponse"] = existing_doc.get("webinarGeekResponse")
                if existing_doc.get("webinarGeekId"):
                    registration_data["webinarGeekId"] = existing_doc.get("webinarGeekId")
                registration_data["alreadyRegistered"] = True
                
        except Exception as db_query_error:
            logger.error(f"⚠️ DB query failed (continuing without DB): {str(db_query_error)}")
            db_available = False
            existing_doc = None

        # Perform DB write
        try:
            if db_available:
                if existing_doc:
                    logger.info(f"Updating existing record for {registration.email} (client: {client_id})")
                    
                    await db.webinar_registrants.update_one(
                        {"_id": existing_doc["_id"]},
                        {"$set": registration_data}
                    )
                    insert_result = type('obj', (object,), {'inserted_id': existing_doc["_id"]})
                    doc_id_ref[0] = existing_doc["_id"]
                else:
                    insert_result = await db.webinar_registrants.insert_one(registration_data)
                    doc_id_ref[0] = insert_result.inserted_id
                    logger.info(f"Inserted new registration with ID: {insert_result.inserted_id} (client: {client_id})")
        except Exception as db_error:
            logger.error(f"⚠️ Failed DB write operation: {str(db_error)}")
            db_available = False

        # ------------------------------------------------------------------
        # WebinarGeek broadcast registration
        # ------------------------------------------------------------------
        wg_response_data = None
        wg_success = False
        wg_timeout = 30.0
        
        if webinar_geek_api_key and broadcast_id and str(broadcast_id) not in ["None", "not_available", ""]:
            webinar_geek_url = f"https://app.webinargeek.com/api/v2/broadcasts/{broadcast_id}/subscriptions"
            logger.info(f"Attempting WebinarGeek registration for broadcast {broadcast_id} (client: {client_id})")
            
            # Build extra fields using client's field mappings
            extra_fields = {}
            
            utm_source_field = client_config.get("field_utm_source", "extra_field_101")
            utm_medium_field = client_config.get("field_utm_medium", "extra_field_102")
            utm_campaign_field = client_config.get("field_utm_campaign", "extra_field_103")
            submitted_from_url_field = client_config.get("field_submitted_from_url", "extra_field_1527745")
            
            if hasattr(registration, 'utm_source') and registration.utm_source:
                extra_fields[utm_source_field] = registration.utm_source
            if hasattr(registration, 'utm_medium') and registration.utm_medium:
                extra_fields[utm_medium_field] = registration.utm_medium
            if hasattr(registration, 'utm_campaign') and registration.utm_campaign:
                extra_fields[utm_campaign_field] = registration.utm_campaign
            if registration.submittedFromUrl:
                extra_fields[submitted_from_url_field] = registration.submittedFromUrl
            
            # Build consent fields
            consent_fields = {}
            if registration.terms:
                consent_fields["Privacy policy"] = "I consent."
            
            # Build WebinarGeek payload
            payload = {
                "firstname": registration.firstName,
                "surname": registration.lastName or "",
                "email": registration.email,
                "company": registration.companyName if registration.companyName else None,
                "phone": registration.phone if registration.phone else None,
                "country": registration.countryCode.upper() if registration.countryCode else None,
                "custom_field": registration.utm_campaign if hasattr(registration, 'utm_campaign') else None,
                "external_id": registration_data.get("id") if "id" in registration_data else None,
                "skip_confirmation_mail": False,
            }
            
            if extra_fields:
                payload["extra_fields"] = extra_fields
            if consent_fields:
                payload["consent_fields"] = consent_fields
            
            # Remove None values
            payload = {k: v for k, v in payload.items() if v not in (None, {}, "")}
            if "company" not in payload:
                payload["company"] = None
            
            logger.info(f"WebinarGeek registration attempt for {registration.email} on broadcast {broadcast_id} (client: {client_id})")
            
            async with httpx.AsyncClient() as client_http:
                try:
                    response = await client_http.post(
                        webinar_geek_url,
                        json=payload,
                        headers={
                            "Api-Token": webinar_geek_api_key,
                            "Content-Type": "application/json",
                            "Accept": "application/json",
                        },
                        timeout=wg_timeout,
                    )
                    
                    if response.status_code == 201:
                        wg_response_data = response.json()
                        wg_success = True
                        logger.info(f"✅ WebinarGeek registration SUCCESSFUL for {registration.email} (client: {client_id})")
                        
                        # Extract fields from response
                        registration_data.update({
                            "webinarGeekId": wg_response_data.get("id"),
                            "confirmationLink": wg_response_data.get("confirmation_link"),
                            "watchLink": wg_response_data.get("watch_link"),
                            "emailVerified": wg_response_data.get("email_verified", False),
                            "timeZone": wg_response_data.get("time_zone"),
                            "createdAt": wg_response_data.get("created_at"),
                            "registrationSource": wg_response_data.get("registration_source"),
                            "eligibleToWatch": wg_response_data.get("eligible_to_watch", True),
                        })
                        
                        # Extract broadcast info
                        if "broadcast" in wg_response_data:
                            broadcast_info = wg_response_data["broadcast"]
                            registration_data.update({
                                "broadcastId": broadcast_info.get("id"),
                                "broadcastDate": broadcast_info.get("date"),
                                "broadcastHasEnded": broadcast_info.get("has_ended", False),
                                "broadcastCancelled": broadcast_info.get("cancelled", False),
                                "replayAvailable": broadcast_info.get("replay_available", False),
                                "publicReplayLink": broadcast_info.get("public_replay_link"),
                            })
                        
                        # Store full response
                        registration_data["webinarGeekResponse"] = wg_response_data
                        
                    elif response.status_code == 422:
                        # Already registered on WebinarGeek
                        logger.info(f"⚠️ WebinarGeek 422 - User already registered: {registration.email} (client: {client_id})")
                        
                        error_data = {}
                        try:
                            error_data = response.json()
                        except:
                            error_data = {"message": response.text}
                        
                        error_msg = str(error_data).lower()
                        
                        already_registered = any(phrase in error_msg for phrase in [
                            "already registered", "already signed up", "duplicate",
                            "already exists", "email has already been taken"
                        ])
                        
                        if already_registered:
                            registration_data["alreadyRegistered"] = True
                            wg_success = True
                            
                            # Try to fetch existing subscription
                            try:
                                existing_sub_data = await fetch_existing_broadcast_subscription(
                                    str(broadcast_id), registration.email, webinar_geek_api_key
                                )
                                
                                if existing_sub_data and existing_sub_data.get("subscriptions"):
                                    sub = existing_sub_data["subscriptions"][0]
                                    registration_data.update({
                                        "webinarGeekId": sub.get("id"),
                                        "watchLink": sub.get("watch_link"),
                                        "confirmationLink": sub.get("confirmation_link"),
                                        "emailVerified": sub.get("email_verified", False),
                                        "webinarGeekResponse": existing_sub_data
                                    })
                                    logger.info(f"Fetched existing subscription for {registration.email} (client: {client_id})")
                            except Exception as fetch_error:
                                logger.error(f"Error fetching existing subscription: {str(fetch_error)}")
                    else:
                        logger.error(f"❌ WebinarGeek registration FAILED with status {response.status_code} (client: {client_id})")
                        
                except Exception as e:
                    logger.error(f"❌ WebinarGeek API exception for {registration.email} (client: {client_id}): {str(e)}")
                    registration_data["webinarGeekError"] = str(e)
                    registration_data["status"]["webinarGeekSent"] = False
        else:
            if not webinar_geek_api_key:
                logger.warning(f"WebinarGeek API key not configured for client {client_id}")
            if not broadcast_id or str(broadcast_id) in ["None", "not_available", ""]:
                logger.warning(f"Invalid broadcast ID: {broadcast_id} for client {client_id}")

        # Update DB with WebinarGeek response
        if wg_success:
            registration_data["status"]["webinarGeekSent"] = True
            registration_data["status"]["lastUpdated"] = datetime.now()
        
        doc_id = doc_id_ref[0]
        
        if (wg_success or registration_data.get("alreadyRegistered")) and doc_id:
            try:
                await db.webinar_registrants.update_one(
                    {"_id": doc_id}, 
                    {"$set": registration_data}
                )
            except Exception as update_error:
                logger.error(f"Failed to update DB with WebinarGeek data: {str(update_error)}")

        # ------------------------------------------------------------------
        # NON-BLOCKING: Fire GHL webhook in background
        # ------------------------------------------------------------------
        
        enriched_doc = registration_data.copy()
        
        async def send_ghl_webhook_background():
            """Send GHL webhook in background"""
            if not ghl_webhook_url:
                logger.info(f"⏭️ GHL webhook skipped for client {client_id} - not configured")
                return
                
            try:
                ghl_payload = serialize_datetime_objects(enriched_doc)
                if "companyName" not in ghl_payload:
                    ghl_payload["companyName"] = None
                
                logger.info(f"GHL webhook sending for {ghl_payload.get('email', 'N/A')} (client: {client_id})")
                
                async with httpx.AsyncClient() as client_http:
                    ghl_response = await client_http.post(ghl_webhook_url, json=ghl_payload, timeout=10.0)
                    
                    if ghl_response.is_success:
                        logger.info(f"✅ GHL webhook success for {enriched_doc.get('email', 'N/A')} (client: {client_id})")
                        if doc_id:
                            try:
                                await db.webinar_registrants.update_one(
                                    {"_id": doc_id},
                                    {"$set": {"status.ghlSent": True, "status.lastUpdated": datetime.now()}}
                                )
                            except Exception as db_update_error:
                                logger.warning(f"⚠️ GHL succeeded but couldn't update DB status: {str(db_update_error)}")
                    else:
                        logger.error(f"❌ GHL webhook failed ({ghl_response.status_code}) for client {client_id}")
                        
            except Exception as e:
                logger.error(f"❌ GHL webhook exception for client {client_id}: {str(e)}")
        
        # Fire GHL webhook in background
        asyncio.create_task(send_ghl_webhook_background())
        
        logger.info(f"🚀 Webhooks fired for {registration.email} (client: {client_id})")

        # ------------------------------------------------------------------
        # Build response
        # ------------------------------------------------------------------
        response_data = {
            "success": True,
            "message": "Registration successful",
            "client_id": client_id
        }
        
        is_already_registered = enriched_doc.get("alreadyRegistered", False)
        has_watch_link = enriched_doc.get("watchLink") is not None
        
        if not db_available:
            response_data["watchLink"] = enriched_doc.get("watchLink")
            response_data["confirmationLink"] = enriched_doc.get("confirmationLink")
            response_data["webinarGeekStatus"] = "registered" if wg_success else "pending"
            response_data["message"] = "Registration successful! Your information has been recorded."
            response_data["note"] = "You'll receive your confirmation and webinar details via email shortly."
        elif (wg_success and has_watch_link) or enriched_doc.get("watchLink"):
            response_data["watchLink"] = enriched_doc.get("watchLink")
            response_data["confirmationLink"] = enriched_doc.get("confirmationLink")
            response_data["webinarGeekStatus"] = "registered"
            
            if is_already_registered:
                response_data["message"] = "You're already registered for this broadcast"
                response_data["note"] = "Your webinar access link is below."
            else:
                response_data["message"] = "Registration successful"
                # Increment display counter for new registrations
                try:
                    await increment_display_counter(db, client_id, broadcast_id)
                except Exception as counter_error:
                    logger.error(f"Failed to increment display counter (non-critical): {str(counter_error)}")
        else:
            response_data["watchLink"] = None
            response_data["confirmationLink"] = None
            
            if is_already_registered:
                response_data["webinarGeekStatus"] = "registered"
                response_data["message"] = "You're already registered for this broadcast"
                response_data["note"] = "You should have received your webinar details via email."
            elif enriched_doc.get("webinarGeekError"):
                response_data["webinarGeekStatus"] = "pending"
                response_data["message"] = "Registration saved! You'll receive your confirmation via email shortly."
            else:
                response_data["webinarGeekStatus"] = "pending"
                response_data["message"] = "Registration saved. Processing webinar details..."
        
        response_data["data"] = {
            "email": enriched_doc.get("email"),
            "firstName": enriched_doc.get("firstName"),
            "broadcastId": enriched_doc.get("broadcastId"),
            "registeredAt": enriched_doc.get("submittedAt")
        }
        
        return response_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in register_webinar: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.get("/webinars/{client_id}")
async def get_webinars(client_id: str):
    """
    Get list of upcoming webinars from WebinarGeek API for a specific client.
    
    Args:
        client_id: Client identifier for multi-tenant isolation
    """
    try:
        db = get_db()
        client_config = await get_client_config(client_id, db)
        
        if not client_config:
            raise HTTPException(status_code=404, detail=f"Client '{client_id}' not found or inactive")
        
        webinar_geek_api_key = client_config.get("webinar_geek_api_key")
        if not webinar_geek_api_key:
            raise HTTPException(status_code=500, detail=f"WebinarGeek API key not configured for client '{client_id}'")
        
        async with httpx.AsyncClient() as client_http:
            response = await client_http.get(
                "https://app.webinargeek.com/api/v2/webinars",
                headers={
                    "Api-Token": webinar_geek_api_key,
                    "Accept": "application/json",
                },
                timeout=10.0
            )
            
            if not response.is_success:
                raise HTTPException(status_code=response.status_code, detail="Failed to fetch webinars from WebinarGeek API")
            
            webinars_data = response.json()
            
            # Filter for upcoming webinars
            upcoming_webinars = []
            for webinar in webinars_data.get("data", []):
                upcoming_webinars.append({
                    "id": webinar.get("id"),
                    "title": webinar.get("title"),
                    "description": webinar.get("description", ""),
                    "status": "Upcoming",
                })
            
            return {"client_id": client_id, "webinars": upcoming_webinars}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_webinars for client {client_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.get("/webinar-details/{client_id}/{webinar_id}")
async def get_webinar_details(client_id: str, webinar_id: str):
    """
    Get detailed information about a specific webinar.
    
    Args:
        client_id: Client identifier for multi-tenant isolation
        webinar_id: WebinarGeek webinar ID
    """
    try:
        db = get_db()
        client_config = await get_client_config(client_id, db)
        
        if not client_config:
            raise HTTPException(status_code=404, detail=f"Client '{client_id}' not found or inactive")
        
        webinar_geek_api_key = client_config.get("webinar_geek_api_key")
        if not webinar_geek_api_key:
            raise HTTPException(status_code=500, detail=f"WebinarGeek API key not configured for client '{client_id}'")
        
        async with httpx.AsyncClient() as client_http:
            response = await client_http.get(
                f"https://app.webinargeek.com/api/v2/webinars/{webinar_id}",
                headers={
                    "Api-Token": webinar_geek_api_key,
                    "Accept": "application/json",
                },
                timeout=10.0
            )
            
            if not response.is_success:
                raise HTTPException(status_code=response.status_code, detail="Failed to fetch webinar details")
            
            detail_data = response.json().get("data", {})
            
            # Find next broadcast
            next_broadcast = None
            broadcasts = detail_data.get("broadcasts", [])
            if broadcasts:
                now = datetime.now().timestamp()
                future_broadcasts = [b for b in broadcasts if b.get("starts_at_timestamp", 0) > now and not b.get("cancelled", False)]
                
                if future_broadcasts:
                    future_broadcasts.sort(key=lambda b: b.get("starts_at_timestamp", 0))
                    next_broadcast = future_broadcasts[0]
            
            processed_webinar = {
                "client_id": client_id,
                "id": detail_data.get("id", "N/A"),
                "title": detail_data.get("title", "N/A"),
                "description": detail_data.get("description", "No description available"),
                "language": detail_data.get("language", "en"),
                "image_url": detail_data.get("image_url", ""),
                "status": detail_data.get("status", "unknown"),
                "timezone": detail_data.get("timezone", "UTC"),
                "duration": detail_data.get("duration", 60),
                "registration_url": detail_data.get("registration_url", ""),
                "current_subscribers": detail_data.get("current_subscribers", 0),
                "next_broadcast": {
                    "starts_at": next_broadcast.get("starts_at", "N/A") if next_broadcast else "N/A",
                    "starts_at_timestamp": next_broadcast.get("starts_at_timestamp", 0) if next_broadcast else 0,
                    "timezone": next_broadcast.get("timezone", "UTC") if next_broadcast else "UTC",
                    "duration": next_broadcast.get("duration", 60) if next_broadcast else 60,
                } if next_broadcast else None,
                "presenter": detail_data.get("presenter", {})
            }
            
            return processed_webinar
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_webinar_details for client {client_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.post("/submit-lead")
async def submit_lead(lead: LeadSubmission):
    """
    Submit lead information to Google Sheets.
    Note: This endpoint may need client_id if lead capture needs multi-tenant support.
    """
    try:
        if not lead.submittedAt:
            lead.submittedAt = datetime.now()
        
        logger.info(f"Lead submission received: {lead.dict()}")
        
        return {"success": True, "message": "Lead submitted successfully"}
    
    except Exception as e:
        logger.error(f"Error in submit_lead: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to submit lead: {str(e)}")
