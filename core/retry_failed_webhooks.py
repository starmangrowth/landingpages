"""
Retry mechanism for failed webhook deliveries.
This module handles retrying failed deliveries to WebinarGeek, GHL, and Google Sheets.

MULTI-TENANT: This module fetches client-specific configuration for each registration
and uses the appropriate API keys and webhook URLs per client.

Created: September 25, 2025
Updated: January 2026 - Multi-tenant support
"""

import httpx
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from app.db.mongo import get_db
from app.core.client_config import get_client_config
import asyncio
import json

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


async def is_broadcast_still_active(broadcast_id: str, client_id: str, db=None) -> bool:
    """
    Check if a broadcast is still active (not ended and not cancelled).
    
    MULTI-TENANT: Checks broadcast status filtered by client_id.
    
    Args:
        broadcast_id (str): The broadcast ID to check
        client_id (str): The client identifier
        db: MongoDB database connection (optional)
        
    Returns:
        bool: True if broadcast is still active, False if ended
    """
    if db is None:
        db = get_db()
    
    if not client_id:
        logger.warning(f"No client_id provided for broadcast check {broadcast_id}")
        return False
    
    try:
        # Check broadcast record for this client
        broadcast_record = await db.broadcasts.find_one({
            "client_id": client_id,
            "broadcast_id": str(broadcast_id)
        })
        
        if broadcast_record:
            has_ended = broadcast_record.get("has_ended", False)
            cancelled = broadcast_record.get("cancelled", False)
            
            if has_ended or cancelled:
                logger.debug(f"Broadcast {broadcast_id} for client {client_id} is inactive: has_ended={has_ended}, cancelled={cancelled}")
                return False
            
            # Check if broadcast date has passed (safety check)
            broadcast_date = broadcast_record.get("date")
            if broadcast_date:
                current_time = datetime.now().timestamp()
                # Add 24 hours buffer after broadcast date to handle replays
                if current_time > (broadcast_date + 86400):  # 24 hours in seconds
                    logger.debug(f"Broadcast {broadcast_id} for client {client_id} is past its date (+24h buffer)")
                    return False
            
            logger.debug(f"Broadcast {broadcast_id} for client {client_id} is active")
            return True
        
        # Check upcoming broadcast collection for this client
        upcoming_broadcast = await db["upcoming-broadcast"].find_one({"client_id": client_id})
        if upcoming_broadcast and str(upcoming_broadcast.get("broadcast_id")) == str(broadcast_id):
            logger.debug(f"Broadcast {broadcast_id} is the current upcoming broadcast for client {client_id}")
            return True
        
        # For unknown broadcasts, assume active (conservative approach)
        logger.warning(f"No broadcast record found for {broadcast_id} / client {client_id}, assuming active")
        return True
        
    except Exception as e:
        logger.error(f"Error checking broadcast status for {broadcast_id} / client {client_id}: {str(e)}")
        return True


async def retry_failed_webhooks():
    """
    Retry failed webhook deliveries for registrations.
    
    MULTI-TENANT: This function:
    1. Queries registrations with failed webhook status
    2. For each registration, fetches the client configuration
    3. Uses client-specific API keys and webhook URLs for retries
    4. Only processes registrations for active upcoming broadcasts
    """
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info(f"🔄 MULTI-TENANT RETRY JOB STARTED at {start_time.isoformat()}")
    logger.info("=" * 80)
    
    try:
        db = get_db()
        
        # Get all upcoming broadcasts (one per client)
        upcoming_broadcasts = await db["upcoming-broadcast"].find({
            "broadcast_id": {"$ne": None}
        }).to_list(100)
        
        if not upcoming_broadcasts:
            logger.info("⏭️  No upcoming broadcasts found for any client - skipping retry job")
            logger.info("=" * 80)
            return
        
        # Create a map of client_id -> upcoming_broadcast_id
        client_broadcast_map = {}
        for ub in upcoming_broadcasts:
            client_id = ub.get("client_id")
            broadcast_id = ub.get("broadcast_id")
            if client_id and broadcast_id:
                client_broadcast_map[client_id] = str(broadcast_id)
        
        logger.info(f"📊 Found {len(client_broadcast_map)} clients with upcoming broadcasts")
        for client_id, broadcast_id in client_broadcast_map.items():
            logger.info(f"   • {client_id}: broadcast {broadcast_id}")
        
        # IMPORTANT MULTI-TENANT SAFETY:
        # Do NOT query only by broadcastId across all clients, because different WebinarGeek
        # accounts *could* theoretically overlap broadcast IDs. Always include client_id.
        failed_registrations = []
        for cid, bid in client_broadcast_map.items():
            batch = await db.webinar_registrants.find(
                {
                    "client_id": cid,
                    "broadcastId": bid,
                    "$or": [
                        {"status.webinarGeekSent": False},
                        {"status.ghlSent": False},
                        {"status.googleSheetsSent": False},
                    ],
                }
            ).to_list(100)
            failed_registrations.extend(batch)

        logger.info(
            f"Found {len(failed_registrations)} registrations with pending deliveries "
            f"across {len(client_broadcast_map)} clients"
        )
        
        # Track stats per client
        client_stats = {}
        processed_count = 0
        skipped_count = 0
        
        for registration in failed_registrations:
            try:
                client_id = registration.get("client_id")
                broadcast_id = registration.get("broadcastId")
                
                if not client_id:
                    logger.warning(f"Registration {registration.get('_id')} has no client_id, skipping")
                    skipped_count += 1
                    continue
                
                # Initialize stats for this client
                if client_id not in client_stats:
                    client_stats[client_id] = {
                        "processed": 0,
                        "webinargeek_retried": 0,
                        "ghl_retried": 0,
                        "sheets_retried": 0,
                        "skipped": 0
                    }
                
                # Verify this is for an active upcoming broadcast
                expected_broadcast = client_broadcast_map.get(client_id)
                if not expected_broadcast or str(broadcast_id) != expected_broadcast:
                    logger.debug(f"Skipping registration for non-current broadcast {broadcast_id} (client {client_id})")
                    client_stats[client_id]["skipped"] += 1
                    skipped_count += 1
                    continue
                
                # Get client configuration
                client_config = await get_client_config(client_id, db)
                
                if not client_config:
                    logger.warning(f"Client '{client_id}' config not found or inactive, skipping registration")
                    client_stats[client_id]["skipped"] += 1
                    skipped_count += 1
                    continue
                
                # Extract client-specific credentials
                webinar_geek_api_key = client_config.get("webinar_geek_api_key")
                ghl_webhook_url = client_config.get("ghl_url")
                google_sheet_webhook_url = client_config.get("google_sheet_url")
                
                processed_count += 1
                client_stats[client_id]["processed"] += 1
                
                # Retry WebinarGeek if needed
                if not registration.get("status", {}).get("webinarGeekSent", False):
                    if webinar_geek_api_key and not registration.get("webinarGeekId"):
                        await retry_webinargeek_registration(registration, webinar_geek_api_key, db)
                        client_stats[client_id]["webinargeek_retried"] += 1
                
                # Retry GHL if needed
                if not registration.get("status", {}).get("ghlSent", False) and ghl_webhook_url:
                    await retry_ghl_webhook(registration, ghl_webhook_url, db)
                    client_stats[client_id]["ghl_retried"] += 1
                
                # Retry Google Sheets if needed (with backoff)
                if not registration.get("status", {}).get("googleSheetsSent", False) and google_sheet_webhook_url:
                    status = registration.get("status", {})
                    next_retry_at = status.get("googleSheetsNextRetryAt")
                    if next_retry_at and isinstance(next_retry_at, datetime):
                        if datetime.now() < next_retry_at:
                            logger.info(f"⏳ Google Sheets retry deferred for {registration.get('email','N/A')} until {next_retry_at.isoformat()}")
                        else:
                            await retry_google_sheets_webhook(registration, google_sheet_webhook_url, db)
                            client_stats[client_id]["sheets_retried"] += 1
                    else:
                        await retry_google_sheets_webhook(registration, google_sheet_webhook_url, db)
                        client_stats[client_id]["sheets_retried"] += 1
                    
            except Exception as e:
                logger.error(f"Error processing registration {registration.get('_id')}: {str(e)}")
                continue
        
        # Log completion
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("=" * 80)
        logger.info(f"✅ MULTI-TENANT RETRY JOB COMPLETED")
        logger.info(f"   Duration: {duration:.2f} seconds")
        logger.info(f"   Processed: {processed_count} registrations")
        logger.info(f"   Skipped: {skipped_count} registrations")
        logger.info("")
        logger.info("   Per-Client Breakdown:")
        for client_id, stats in client_stats.items():
            logger.info(f"   📌 {client_id}:")
            logger.info(f"      Processed: {stats['processed']}, Skipped: {stats['skipped']}")
            logger.info(f"      WebinarGeek: {stats['webinargeek_retried']}, GHL: {stats['ghl_retried']}, Sheets: {stats['sheets_retried']}")
        logger.info("=" * 80)
        
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.error("=" * 80)
        logger.error(f"❌ RETRY JOB FAILED after {duration:.2f} seconds")
        logger.error(f"Error: {str(e)}")
        logger.error("=" * 80)


async def retry_webinargeek_registration(registration: Dict, api_key: str, db) -> bool:
    """
    Retry WebinarGeek registration for a failed registration.
    
    Args:
        registration: The registration document
        api_key: Client-specific WebinarGeek API key
        db: MongoDB database connection
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        broadcast_id = registration.get("broadcastId")
        client_id = registration.get("client_id")
        
        if not broadcast_id:
            return False
        
        url = f"https://app.webinargeek.com/api/v2/broadcasts/{broadcast_id}/subscriptions"
        
        # Build payload
        payload = {
            "firstname": registration.get("firstName", ""),
            "surname": registration.get("lastName", ""),
            "email": registration.get("email"),
            "company": registration.get("companyName"),
            "phone": registration.get("phone"),
            "skip_confirmation_mail": True  # Don't resend confirmation
        }

        # Remove None values
        payload = {k: v for k, v in payload.items() if v not in (None, "")}
        if "company" not in payload:
            payload["company"] = None
        
        logger.info(f"WebinarGeek retry for {registration.get('email', 'N/A')} (client: {client_id}, broadcast: {broadcast_id})")
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                json=payload,
                headers={
                    "Api-Token": api_key,
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                },
                timeout=15.0
            )
            
            if response.status_code == 201:
                # Success - update database
                data = response.json()
                await db.webinar_registrants.update_one(
                    {"_id": registration["_id"]},
                    {"$set": {
                        "webinarGeekId": data.get("id"),
                        "watchLink": data.get("watch_link"),
                        "confirmationLink": data.get("confirmation_link"),
                        "status.webinarGeekSent": True,
                        "status.lastUpdated": datetime.now()
                    }}
                )
                logger.info(f"✅ WebinarGeek retry SUCCESS for {registration.get('email')} (client: {client_id})")
                return True
                
            elif response.status_code == 422:
                # Already registered - mark as sent
                await db.webinar_registrants.update_one(
                    {"_id": registration["_id"]},
                    {"$set": {
                        "status.webinarGeekSent": True,
                        "alreadyRegistered": True,
                        "status.lastUpdated": datetime.now()
                    }}
                )
                logger.info(f"✅ User {registration.get('email')} already registered on WebinarGeek (client: {client_id})")
                return True
            else:
                logger.error(f"❌ WebinarGeek retry FAILED - Status: {response.status_code} for {registration.get('email')}")
                return False
                
    except Exception as e:
        logger.error(f"❌ WebinarGeek retry exception: {str(e)}")
        return False


async def retry_ghl_webhook(registration: Dict, webhook_url: str, db) -> bool:
    """
    Retry GHL webhook delivery.
    
    Args:
        registration: The registration document
        webhook_url: Client-specific GHL webhook URL
        db: MongoDB database connection
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        client_id = registration.get("client_id")
        
        # Prepare payload
        payload = serialize_datetime_objects(registration)
        payload.pop("_id", None)  # Remove MongoDB ID
        if "companyName" not in payload:
            payload["companyName"] = None
        payload.update({
            "timestamp": datetime.now().isoformat(),
            "submitted_at": int(datetime.now().timestamp()),
            "retry": True  # Mark as retry
        })
        
        logger.info(f"GHL retry for {payload.get('email', 'N/A')} (client: {client_id})")
        
        async with httpx.AsyncClient() as client:
            response = await client.post(webhook_url, json=payload, timeout=60.0)
            
            if response.is_success:
                # Update status
                await db.webinar_registrants.update_one(
                    {"_id": registration["_id"]},
                    {"$set": {
                        "status.ghlSent": True,
                        "status.lastUpdated": datetime.now()
                    }}
                )
                logger.info(f"✅ GHL webhook retry SUCCESS for {registration.get('email')} (client: {client_id})")
                return True
            else:
                logger.error(f"❌ GHL webhook retry FAILED - Status: {response.status_code} for {registration.get('email')}")
                return False
                
    except Exception as e:
        logger.error(f"❌ GHL webhook retry exception: {str(e)}")
        return False


async def retry_google_sheets_webhook(registration: Dict, webhook_url: str, db) -> bool:
    """
    Retry Google Sheets webhook delivery with 2-minute timeout.
    
    Args:
        registration: The registration document
        webhook_url: Client-specific Google Sheets webhook URL
        db: MongoDB database connection
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        client_id = registration.get("client_id")
        
        # Respect max attempts (cap at 5)
        retry_count = registration.get("status", {}).get("googleSheetsRetryCount", 0) or 0
        if retry_count >= 5:
            logger.warning(f"🛑 Google Sheets retry exhausted (count={retry_count}) for {registration.get('email','N/A')} (client: {client_id})")
            await db.webinar_registrants.update_one(
                {"_id": registration["_id"]},
                {"$set": {
                    "status.googleSheetsInProgress": False,
                    "status.googleSheetsFinalState": "exhausted",
                    "status.lastUpdated": datetime.now()
                }}
            )
            return False

        # Atomic check-and-set: only proceed if googleSheetsSent is still false
        claim_result = await db.webinar_registrants.find_one_and_update(
            {
                "_id": registration["_id"],
                "status.googleSheetsSent": False
            },
            {
                "$set": {
                    "status.googleSheetsInProgress": True,
                    "status.lastUpdated": datetime.now()
                }
            },
            return_document=False
        )
        
        if not claim_result:
            logger.info(f"⏭️ Google Sheets retry - Already sent/in-progress for {registration.get('email', 'N/A')}, skipping")
            return False
        
        # Prepare payload
        payload = serialize_datetime_objects(registration)
        payload.pop("_id", None)  # Remove MongoDB ID
        if "companyName" not in payload:
            payload["companyName"] = None
        
        # Add timestamp
        payload.update({
            "timestamp": datetime.now().isoformat(),
            "submitted_at": int(datetime.now().timestamp()),
            "retry": True  # Mark as retry
        })
        
        # Add channel ID if present
        channel_id = registration.get("id") or registration.get("ID")
        if channel_id:
            payload["ID"] = channel_id
        
        logger.info(f"Google Sheets retry for {payload.get('email', 'N/A')} (client: {client_id})")
        
        async with httpx.AsyncClient(follow_redirects=True) as client:
            # Use 2 minutes (120 seconds) timeout for Google Sheets
            response = await client.post(webhook_url, json=payload, timeout=120.0)
            
            if response.is_success:
                sheet_success = False
                try:
                    response_json = response.json()

                    if response_json.get('ok') is True:
                        sheet_success = True
                    elif response_json.get('ok') is False:
                        if response_json.get('skipped') is True:
                            sheet_success = True
                            logger.info(f"✅ Google Sheets retry DUPLICATE - {response_json.get('reason', 'Already processed')}")
                        else:
                            logger.error(f"❌ Google Sheets retry FAILED - Script error: {response_json.get('error', 'Unknown')}")
                            sheet_success = False
                    else:
                        logger.warning("⚠️ Google Sheets retry response missing 'ok' flag; treating HTTP 200 as success")
                        sheet_success = True
                        
                except json.JSONDecodeError:
                    logger.warning("⚠️ Google Sheets retry returned non-JSON; treating HTTP 200 as success")
                    sheet_success = True
                except Exception as parse_error:
                    logger.error(f"❌ Google Sheets retry parse error: {str(parse_error)}")
                    sheet_success = False

                if sheet_success:
                    await db.webinar_registrants.update_one(
                        {"_id": registration["_id"]},
                        {"$set": {
                            "status.googleSheetsSent": True,
                            "status.googleSheetsInProgress": False,
                            "status.googleSheetsRetryCount": retry_count,
                            "status.googleSheetsNextRetryAt": None,
                            "status.lastUpdated": datetime.now()
                        }}
                    )
                    logger.info(f"✅ Google Sheets retry SUCCESS for {registration.get('email')} (client: {client_id})")
                    return True
                else:
                    # Reset in-progress flag with backoff
                    backoff_minutes = {0: 1, 1: 2, 2: 5, 3: 10}.get(retry_count, 30)
                    next_retry = datetime.now() + timedelta(minutes=backoff_minutes)
                    await db.webinar_registrants.update_one(
                        {"_id": registration["_id"]},
                        {"$set": {
                            "status.googleSheetsInProgress": False,
                            "status.googleSheetsRetryCount": retry_count + 1,
                            "status.googleSheetsNextRetryAt": next_retry,
                            "status.lastUpdated": datetime.now()
                        }}
                    )
                    return False
            else:
                # HTTP error - reset with backoff
                backoff_minutes = {0: 1, 1: 2, 2: 5, 3: 10}.get(retry_count, 30)
                next_retry = datetime.now() + timedelta(minutes=backoff_minutes)
                await db.webinar_registrants.update_one(
                    {"_id": registration["_id"]},
                    {"$set": {
                        "status.googleSheetsInProgress": False,
                        "status.googleSheetsRetryCount": retry_count + 1,
                        "status.googleSheetsNextRetryAt": next_retry,
                        "status.lastUpdated": datetime.now()
                    }}
                )
                logger.error(f"❌ Google Sheets retry FAILED - Status: {response.status_code} for {registration.get('email')}")
                return False
                
    except Exception as e:
        # Reset in-progress flag on exception
        try:
            retry_count = registration.get("status", {}).get("googleSheetsRetryCount", 0) or 0
            backoff_minutes = {0: 1, 1: 2, 2: 5, 3: 10}.get(retry_count, 30)
            next_retry = datetime.now() + timedelta(minutes=backoff_minutes)
            await db.webinar_registrants.update_one(
                {"_id": registration["_id"]},
                {"$set": {
                    "status.googleSheetsInProgress": False,
                    "status.googleSheetsRetryCount": retry_count + 1,
                    "status.googleSheetsNextRetryAt": next_retry,
                    "status.lastUpdated": datetime.now()
                }}
            )
        except Exception as update_error:
            logger.error(f"Failed to reset in-progress flag: {str(update_error)}")
        
        logger.error(f"❌ Google Sheets retry exception: {str(e)}")
        return False


# For testing
if __name__ == "__main__":
    asyncio.run(retry_failed_webhooks())
