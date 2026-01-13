"""
Database webinar routes module.
This module provides API endpoints for accessing webinar data from the local database.

MULTI-TENANT: All endpoints require client_id for proper data isolation.

Updated: January 2026 - Full multi-tenant support
"""

from fastapi import APIRouter, HTTPException, status
from typing import Dict, Any, List, Optional
from app.db.mongo import get_db
from app.core.client_config import get_client_config, validate_client_id
from datetime import datetime
import logging

# Set up router
router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/db-webinars/{client_id}", status_code=status.HTTP_200_OK)
async def get_db_webinars(client_id: str, upcoming_only: bool = False):
    """
    Get list of webinars/broadcasts from the local database for a specific client.
    
    Args:
        client_id: Client identifier for multi-tenant isolation
        upcoming_only (bool): If True, only return webinars with future broadcasts
        
    Returns:
        dict: List of webinars for this client
    """
    try:
        db = get_db()
        
        # Validate client
        if not await validate_client_id(client_id, db):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Client '{client_id}' not found or inactive"
            )
        
        # Build query with client_id filter
        query = {"client_id": client_id}
        
        # Filter for upcoming webinars if requested
        if upcoming_only:
            now = datetime.now().timestamp()
            query.update({
                "date": {"$gt": now},
                "cancelled": False,
                "has_ended": False
            })
        
        # Get broadcasts from db (broadcasts are the webinar data now)
        cursor = db.broadcasts.find(query).sort("date", 1)
        broadcasts = await cursor.to_list(length=100)
        
        # Process for API response
        result = []
        for broadcast in broadcasts:
            broadcast.pop("_id", None)
            result.append(broadcast)
        
        return {
            "client_id": client_id,
            "webinars": result,
            "count": len(result),
            "source": "database"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching webinars for client {client_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch webinars: {str(e)}"
        )


@router.get("/db-webinar/{client_id}/{broadcast_id}", status_code=status.HTTP_200_OK) 
async def get_db_webinar(client_id: str, broadcast_id: str):
    """
    Get a specific broadcast from the local database by ID for a specific client.
    
    Args:
        client_id: Client identifier for multi-tenant isolation
        broadcast_id: The broadcast ID
        
    Returns:
        dict: Broadcast details
    """
    try:
        db = get_db()
        
        # Validate client
        if not await validate_client_id(client_id, db):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Client '{client_id}' not found or inactive"
            )
        
        # Find broadcast for THIS client
        broadcast = await db.broadcasts.find_one({
            "client_id": client_id,
            "broadcast_id": broadcast_id
        })
        
        if not broadcast:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Broadcast '{broadcast_id}' not found for client '{client_id}'"
            )
        
        broadcast.pop("_id", None)
        
        return {
            "client_id": client_id,
            "broadcast": broadcast
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching broadcast {broadcast_id} for client {client_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch broadcast: {str(e)}"
        )


@router.get("/last-sync/{client_id}", status_code=status.HTTP_200_OK)
async def get_last_sync_time(client_id: str):
    """
    Get the timestamp of the last successful webinar sync for a specific client.
    
    Args:
        client_id: Client identifier for multi-tenant isolation
        
    Returns:
        dict: Sync information for this client
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
        
        return {
            "client_id": client_id,
            "last_sync": sync_info.get("timestamp"),
            "broadcasts_count": sync_info.get("broadcasts_count"),
            "has_upcoming_broadcast": sync_info.get("has_upcoming_broadcast"),
            "upcoming_broadcast_id": sync_info.get("upcoming_broadcast_id"),
            "success": sync_info.get("success"),
            "error": sync_info.get("error")
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching last sync time for client {client_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch last sync time: {str(e)}"
        )


@router.get("/registrations/{client_id}", status_code=status.HTTP_200_OK)
async def get_registrations(client_id: str, broadcast_id: Optional[str] = None, limit: int = 100):
    """
    Get registrations for a specific client, optionally filtered by broadcast.
    
    Args:
        client_id: Client identifier for multi-tenant isolation
        broadcast_id: Optional broadcast ID to filter registrations
        limit: Maximum number of registrations to return
        
    Returns:
        dict: List of registrations for this client
    """
    try:
        db = get_db()
        
        # Validate client
        if not await validate_client_id(client_id, db):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Client '{client_id}' not found or inactive"
            )
        
        # Build query with client_id filter
        query = {"client_id": client_id}
        
        if broadcast_id:
            query["broadcastId"] = broadcast_id
        
        # Get registrations
        cursor = db.webinar_registrants.find(
            query,
            {
                "_id": 0,
                "email": 1,
                "firstName": 1,
                "lastName": 1,
                "companyName": 1,
                "broadcastId": 1,
                "submittedAt": 1,
                "watchLink": 1,
                "status": 1,
                "alreadyRegistered": 1
            }
        ).sort("submittedAt", -1).limit(limit)
        
        registrations = await cursor.to_list(length=limit)
        
        return {
            "client_id": client_id,
            "broadcast_id": broadcast_id,
            "registrations": registrations,
            "count": len(registrations)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching registrations for client {client_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch registrations: {str(e)}"
        )


@router.get("/registration-stats/{client_id}", status_code=status.HTTP_200_OK)
async def get_registration_stats(client_id: str, broadcast_id: Optional[str] = None):
    """
    Get registration statistics for a specific client.
    
    Args:
        client_id: Client identifier for multi-tenant isolation
        broadcast_id: Optional broadcast ID to filter stats
        
    Returns:
        dict: Registration statistics
    """
    try:
        db = get_db()
        
        # Validate client
        if not await validate_client_id(client_id, db):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Client '{client_id}' not found or inactive"
            )
        
        # Build query with client_id filter
        query = {"client_id": client_id}
        
        if broadcast_id:
            query["broadcastId"] = broadcast_id
        
        # Get counts
        total_registrations = await db.webinar_registrants.count_documents(query)
        
        # Count by status
        webinargeek_sent = await db.webinar_registrants.count_documents({
            **query,
            "status.webinarGeekSent": True
        })
        
        ghl_sent = await db.webinar_registrants.count_documents({
            **query,
            "status.ghlSent": True
        })
        
        sheets_sent = await db.webinar_registrants.count_documents({
            **query,
            "status.googleSheetsSent": True
        })
        
        already_registered = await db.webinar_registrants.count_documents({
            **query,
            "alreadyRegistered": True
        })
        
        pending_webhooks = await db.webinar_registrants.count_documents({
            **query,
            "$or": [
                {"status.webinarGeekSent": False},
                {"status.ghlSent": False},
                {"status.googleSheetsSent": False}
            ]
        })
        
        return {
            "client_id": client_id,
            "broadcast_id": broadcast_id,
            "stats": {
                "total_registrations": total_registrations,
                "webinargeek_sent": webinargeek_sent,
                "ghl_sent": ghl_sent,
                "google_sheets_sent": sheets_sent,
                "already_registered": already_registered,
                "pending_webhooks": pending_webhooks
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching registration stats for client {client_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch registration stats: {str(e)}"
        )
