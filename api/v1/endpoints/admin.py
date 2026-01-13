"""
Client management endpoints for multi-tenant admin operations.
Provides CRUD operations for managing client configurations.
"""

from fastapi import APIRouter, HTTPException, status
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

from app.db.mongo import get_db
from app.models.client import (
    Client, ClientCreate, ClientUpdate, ClientResponse,
    WebinarGeekConfig, WebhooksConfig, DisplaySettings
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/clients", status_code=status.HTTP_201_CREATED)
async def create_client(client_data: ClientCreate) -> Dict[str, Any]:
    """
    Create a new client configuration.
    
    Args:
        client_data: Client configuration including API keys and webhooks
        
    Returns:
        dict: Created client data (without sensitive fields)
    """
    try:
        db = get_db()
        
        # Check if client_id already exists
        existing = await db.clients.find_one({"client_id": client_data.client_id})
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Client with ID '{client_data.client_id}' already exists"
            )
        
        # Create client document
        now = datetime.utcnow()
        client_doc = {
            "client_id": client_data.client_id,
            "client_name": client_data.client_name,
            "status": "active",
            "created_at": now,
            "updated_at": now,
            "webinar_geek": client_data.webinar_geek.dict(),
            "webhooks": client_data.webhooks.dict(),
            "landing_pages": [lp.dict() for lp in client_data.landing_pages],
            "display": client_data.display.dict()
        }
        
        result = await db.clients.insert_one(client_doc)
        
        if result.inserted_id:
            logger.info(f"✅ Created new client: {client_data.client_id}")
            
            # Return response without sensitive data
            return {
                "success": True,
                "message": f"Client '{client_data.client_id}' created successfully",
                "client": {
                    "client_id": client_data.client_id,
                    "client_name": client_data.client_name,
                    "status": "active",
                    "created_at": now.isoformat()
                }
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create client"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating client: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create client: {str(e)}"
        )


@router.get("/clients", status_code=status.HTTP_200_OK)
async def list_clients(status_filter: Optional[str] = None) -> Dict[str, Any]:
    """
    List all clients (without sensitive configuration data).
    
    Args:
        status_filter: Optional filter by status (active/inactive/suspended)
        
    Returns:
        dict: List of clients
    """
    try:
        db = get_db()
        
        # Build query
        query = {}
        if status_filter:
            query["status"] = status_filter
        
        # Fetch clients with projection to exclude sensitive fields
        cursor = db.clients.find(
            query,
            {
                "_id": 0,
                "client_id": 1,
                "client_name": 1,
                "status": 1,
                "created_at": 1,
                "updated_at": 1,
                "landing_pages": 1,
                "display": 1
            }
        ).sort("created_at", -1)
        
        clients = await cursor.to_list(length=100)
        
        return {
            "success": True,
            "clients": clients,
            "count": len(clients)
        }
        
    except Exception as e:
        logger.error(f"Error listing clients: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list clients: {str(e)}"
        )


@router.get("/clients/{client_id}", status_code=status.HTTP_200_OK)
async def get_client(client_id: str, include_config: bool = False) -> Dict[str, Any]:
    """
    Get a specific client's configuration.
    
    Args:
        client_id: The client identifier
        include_config: If True, include sensitive webhook/API config (for admin use)
        
    Returns:
        dict: Client data
    """
    try:
        db = get_db()
        
        client = await db.clients.find_one({"client_id": client_id})
        
        if not client:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Client '{client_id}' not found"
            )
        
        # Remove MongoDB _id
        client.pop("_id", None)
        
        # Hide sensitive fields unless specifically requested
        if not include_config:
            client.pop("webinar_geek", None)
            client.pop("webhooks", None)
        else:
            # Mask API key for security (show only first/last 5 chars)
            if "webinar_geek" in client and "api_key" in client["webinar_geek"]:
                api_key = client["webinar_geek"]["api_key"]
                if len(api_key) > 10:
                    client["webinar_geek"]["api_key"] = f"{api_key[:5]}...{api_key[-5:]}"
        
        return {
            "success": True,
            "client": client
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting client {client_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get client: {str(e)}"
        )


@router.patch("/clients/{client_id}", status_code=status.HTTP_200_OK)
async def update_client(client_id: str, update_data: ClientUpdate) -> Dict[str, Any]:
    """
    Update a client's configuration.
    
    Args:
        client_id: The client identifier
        update_data: Fields to update
        
    Returns:
        dict: Updated client data
    """
    try:
        db = get_db()
        
        # Check if client exists
        existing = await db.clients.find_one({"client_id": client_id})
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Client '{client_id}' not found"
            )
        
        # Build update document
        update_doc = {"updated_at": datetime.utcnow()}
        
        if update_data.client_name is not None:
            update_doc["client_name"] = update_data.client_name
        if update_data.status is not None:
            if update_data.status not in ["active", "inactive", "suspended"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Status must be one of: active, inactive, suspended"
                )
            update_doc["status"] = update_data.status
        if update_data.webinar_geek is not None:
            update_doc["webinar_geek"] = update_data.webinar_geek.dict()
        if update_data.webhooks is not None:
            update_doc["webhooks"] = update_data.webhooks.dict()
        if update_data.landing_pages is not None:
            update_doc["landing_pages"] = [lp.dict() for lp in update_data.landing_pages]
        if update_data.display is not None:
            update_doc["display"] = update_data.display.dict()
        
        # Update
        result = await db.clients.update_one(
            {"client_id": client_id},
            {"$set": update_doc}
        )
        
        if result.modified_count > 0:
            logger.info(f"✅ Updated client: {client_id}")
            return {
                "success": True,
                "message": f"Client '{client_id}' updated successfully"
            }
        else:
            return {
                "success": True,
                "message": "No changes made"
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating client {client_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update client: {str(e)}"
        )


@router.delete("/clients/{client_id}", status_code=status.HTTP_200_OK)
async def delete_client(client_id: str, force: bool = False) -> Dict[str, Any]:
    """
    Delete a client configuration.
    
    Args:
        client_id: The client identifier
        force: If True, permanently delete. If False, just set status to 'suspended'
        
    Returns:
        dict: Deletion result
    """
    try:
        db = get_db()
        
        # Check if client exists
        existing = await db.clients.find_one({"client_id": client_id})
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Client '{client_id}' not found"
            )
        
        if force:
            # Permanent deletion
            result = await db.clients.delete_one({"client_id": client_id})
            if result.deleted_count > 0:
                logger.info(f"🗑️ Permanently deleted client: {client_id}")
                return {
                    "success": True,
                    "message": f"Client '{client_id}' permanently deleted"
                }
        else:
            # Soft delete (suspend)
            result = await db.clients.update_one(
                {"client_id": client_id},
                {"$set": {"status": "suspended", "updated_at": datetime.utcnow()}}
            )
            if result.modified_count > 0:
                logger.info(f"⏸️ Suspended client: {client_id}")
                return {
                    "success": True,
                    "message": f"Client '{client_id}' suspended"
                }
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete client"
        )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting client {client_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete client: {str(e)}"
        )


# Helper function to get client config (used by registration endpoint)
async def get_client_config(client_id: str) -> Optional[Dict[str, Any]]:
    """
    Get client configuration for internal use.
    
    Args:
        client_id: The client identifier
        
    Returns:
        dict: Full client configuration or None if not found/inactive
    """
    try:
        db = get_db()
        client = await db.clients.find_one({
            "client_id": client_id,
            "status": "active"
        })
        
        if client:
            client.pop("_id", None)
            return client
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching client config for {client_id}: {str(e)}")
        return None
