"""
Broadcast API endpoints for accessing broadcast data from the database.

MULTI-TENANT: All endpoints require client_id for proper data isolation.
Each client has their own broadcasts, upcoming-broadcast, and sync info.

Updated: January 2026 - Full multi-tenant support
"""

from fastapi import APIRouter, HTTPException, status
from typing import Dict, Any, Optional
from app.db.mongo import get_db
from app.core.client_config import get_client_config, validate_client_id
from datetime import datetime
import logging
import httpx

# Set up router
router = APIRouter()
logger = logging.getLogger(__name__)


async def fetch_upcoming_broadcast_from_webinargeek(client_id: str, api_key: str) -> Optional[Dict[str, Any]]:
    """
    Fallback: Fetch upcoming broadcast directly from WebinarGeek API.
    Used when database is unavailable.
    
    Args:
        client_id: Client identifier
        api_key: Client-specific WebinarGeek API key
        
    Returns:
        dict: Upcoming broadcast data or None
    """
    if not api_key:
        logger.warning(f"No WebinarGeek API key for client {client_id}")
        return None
    
    try:
        async with httpx.AsyncClient() as client:
            url = "https://app.webinargeek.com/api/v2/webinars"
            
            response = await client.get(
                url,
                headers={
                    "Api-Token": api_key,
                    "Accept": "application/json",
                },
                timeout=10.0
            )
            
            if not response.is_success:
                logger.error(f"WebinarGeek API error for client {client_id}: {response.status_code}")
                return None
            
            data = response.json()
            
            # Find the next upcoming broadcast
            webinars = data.get("webinars", [data]) if "webinars" in data else [data]
            
            for webinar in webinars:
                broadcasts = webinar.get("broadcasts", [])
                current_time = datetime.now().timestamp()
                
                for broadcast in broadcasts:
                    broadcast_date = broadcast.get("date")
                    if broadcast_date and broadcast_date > current_time and not broadcast.get("has_ended") and not broadcast.get("cancelled"):
                        return {
                            "client_id": client_id,
                            "broadcast_id": broadcast.get("id"),
                            "date": broadcast_date,
                            "has_ended": broadcast.get("has_ended", False),
                            "cancelled": broadcast.get("cancelled", False),
                            "webinar_title": webinar.get("title"),
                            "source": "webinargeek_api_fallback"
                        }
            
            return None
            
    except Exception as e:
        logger.error(f"Error fetching from WebinarGeek API for client {client_id}: {str(e)}")
        return None


@router.get("/upcoming-broadcast/{client_id}", status_code=status.HTTP_200_OK)
async def get_upcoming_broadcast(client_id: str):
    """
    Get the latest upcoming broadcast for a specific client.
    Falls back to WebinarGeek API if database is unavailable.

    Args:
        client_id: Client identifier for multi-tenant isolation
        
    Returns:
        dict: Latest upcoming broadcast data or null if none found
    """
    upcoming_broadcast = None
    source = "database"
    
    try:
        db = get_db()
        
        # Validate client and get config
        client_config = await get_client_config(client_id, db)
        if not client_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Client '{client_id}' not found or inactive"
            )
        
        # Try database first - filter by client_id
        upcoming_broadcast = await db["upcoming-broadcast"].find_one({"client_id": client_id})
        
        if upcoming_broadcast:
            upcoming_broadcast.pop("_id", None)
            source = "database"
            
    except HTTPException:
        raise
    except Exception as db_error:
        logger.warning(f"⚠️ Database unavailable for client {client_id}, falling back to WebinarGeek API: {str(db_error)}")
        source = "webinargeek_api_fallback"
        
        # Get client config for API key
        try:
            db = get_db()
            client_config = await get_client_config(client_id, db)
        except:
            client_config = None
    
    # Fallback to WebinarGeek API if DB failed or no data
    if not upcoming_broadcast or not upcoming_broadcast.get("broadcast_id"):
        if client_config and client_config.get("webinar_geek_api_key"):
            upcoming_broadcast = await fetch_upcoming_broadcast_from_webinargeek(
                client_id, 
                client_config.get("webinar_geek_api_key")
            )
            if upcoming_broadcast:
                source = "webinargeek_api_fallback"
                logger.info(f"✅ Fetched upcoming broadcast from WebinarGeek API for client {client_id}")
    
    # If still no data, return empty response
    if not upcoming_broadcast or not upcoming_broadcast.get("broadcast_id"):
        return {
            "client_id": client_id,
            "broadcast_id": None,
            "message": "No upcoming broadcasts found",
            "countdown_data": None,
            "source": source
        }

    # Calculate countdown data
    countdown_data = None
    if upcoming_broadcast.get("broadcast_id"):
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

    return {
        "client_id": client_id,
        "broadcast_id": upcoming_broadcast.get("broadcast_id"),
        "broadcast_data": upcoming_broadcast,
        "countdown_data": countdown_data,
        "source": source
    }


@router.get("/all-broadcasts/{client_id}", status_code=status.HTTP_200_OK)
async def get_all_broadcasts(client_id: str, limit: int = 100):
    """
    Get all broadcasts for a specific client from the database.

    Args:
        client_id: Client identifier for multi-tenant isolation
        limit (int): Maximum number of broadcasts to return

    Returns:
        dict: List of all broadcasts for this client
    """
    try:
        db = get_db()
        
        # Validate client
        if not await validate_client_id(client_id, db):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Client '{client_id}' not found or inactive"
            )

        # Get broadcasts for THIS client only
        cursor = db.broadcasts.find({"client_id": client_id}).sort("date", -1).limit(limit)
        broadcasts = await cursor.to_list(length=limit)

        # Process for API response
        result = []
        for broadcast in broadcasts:
            broadcast.pop("_id", None)
            result.append(broadcast)

        return {
            "client_id": client_id,
            "broadcasts": result,
            "count": len(result),
            "source": "database"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching broadcasts for client {client_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch broadcasts: {str(e)}"
        )


@router.get("/sync-status/{client_id}", status_code=status.HTTP_200_OK)
async def get_sync_status(client_id: str):
    """
    Get the last sync status for a specific client.

    Args:
        client_id: Client identifier for multi-tenant isolation

    Returns:
        dict: Sync status information for this client
    """
    try:
        db = get_db()
        
        # Validate client
        if not await validate_client_id(client_id, db):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Client '{client_id}' not found or inactive"
            )

        # Get sync info for THIS client
        sync_info = await db.broadcast_sync_info.find_one({"client_id": client_id})

        if not sync_info:
            return {
                "client_id": client_id,
                "last_sync": None,
                "message": "No sync data found for this client"
            }

        sync_info.pop("_id", None)
        
        return {
            "client_id": client_id,
            "sync_info": sync_info
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching sync status for client {client_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch sync status: {str(e)}"
        )
