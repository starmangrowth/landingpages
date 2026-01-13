"""
Client configuration utility module for multi-tenant support.
This module provides helper functions to fetch and manage client-specific
configurations including API keys, webhook URLs, and display settings.

All features in the system should use these helpers to get client-specific
configuration instead of using global environment variables.

Created: January 2026
"""

import logging
from typing import Optional, Dict, Any, List
from app.db.mongo import get_db

logger = logging.getLogger(__name__)


async def get_client_config(client_id: str, db=None) -> Optional[Dict[str, Any]]:
    """
    Fetch active client configuration from database.
    
    This is the primary method for getting client-specific settings including:
    - WebinarGeek API key and webinar ID
    - Webhook URLs (Google Sheets, GHL, custom)
    - Display settings (base subscriber count, timezone)
    - Field mappings for WebinarGeek extra fields
    
    Args:
        client_id (str): The unique client identifier (slug format)
        db: MongoDB database connection (optional, will get from pool if not provided)
        
    Returns:
        dict: Client configuration with all settings, or None if client not found/inactive
        
    Example:
        config = await get_client_config("growth-club")
        if config:
            api_key = config["webinar_geek_api_key"]
            sheet_url = config["google_sheet_url"]
    """
    if not client_id:
        logger.warning("get_client_config called with empty client_id")
        return None
    
    if db is None:
        db = get_db()
    
    try:
        # Find active client by client_id
        client = await db.clients.find_one({
            "client_id": client_id,
            "status": "active"
        })
        
        if not client:
            logger.warning(f"Client '{client_id}' not found or inactive")
            return None
        
        # Extract and normalize configuration
        webinar_geek = client.get("webinar_geek", {})
        webhooks = client.get("webhooks", {})
        display = client.get("display", {})
        field_mappings = webinar_geek.get("field_mappings", {})
        
        config = {
            # Client identification
            "client_id": client["client_id"],
            "client_name": client.get("client_name", client_id),
            "status": client.get("status", "active"),
            
            # WebinarGeek configuration
            "webinar_geek_api_key": webinar_geek.get("api_key"),
            "webinar_id": webinar_geek.get("webinar_id"),
            
            # WebinarGeek field mappings for UTM tracking
            "field_utm_source": field_mappings.get("utm_source", "extra_field_101"),
            "field_utm_medium": field_mappings.get("utm_medium", "extra_field_102"),
            "field_utm_campaign": field_mappings.get("utm_campaign", "extra_field_103"),
            "field_submitted_from_url": field_mappings.get("submitted_from_url", "extra_field_1527745"),
            
            # Webhook URLs
            "google_sheet_url": webhooks.get("google_sheet_url"),
            "ghl_url": webhooks.get("ghl_url"),
            "custom_webhooks": webhooks.get("custom_webhooks", []),
            
            # Display settings
            "base_subscriber_count": display.get("base_subscriber_count", 0),
            "timezone": display.get("timezone", "UTC"),
            
            # Landing pages
            "landing_pages": client.get("landing_pages", []),
            
            # Timestamps
            "created_at": client.get("created_at"),
            "updated_at": client.get("updated_at")
        }
        
        logger.debug(f"Loaded config for client '{client_id}'")
        return config
        
    except Exception as e:
        logger.error(f"Error fetching config for client '{client_id}': {str(e)}")
        return None


async def get_all_active_clients(db=None) -> List[Dict[str, Any]]:
    """
    Get all active clients for batch operations like webinar sync.
    
    This function is used by scheduled jobs that need to process
    all clients (e.g., sync_webinars, retry_failed_webhooks).
    
    Args:
        db: MongoDB database connection (optional)
        
    Returns:
        list: List of active client configurations
        
    Example:
        clients = await get_all_active_clients()
        for client in clients:
            api_key = client["webinar_geek"]["api_key"]
            # Process client...
    """
    if db is None:
        db = get_db()
    
    try:
        cursor = db.clients.find({"status": "active"})
        clients = await cursor.to_list(length=100)
        
        logger.info(f"Found {len(clients)} active clients")
        return clients
        
    except Exception as e:
        logger.error(f"Error fetching active clients: {str(e)}")
        return []


async def validate_client_id(client_id: str, db=None) -> bool:
    """
    Validate that a client_id exists and is active.
    
    Use this for quick validation before processing requests.
    For full config, use get_client_config() instead.
    
    Args:
        client_id (str): The client identifier to validate
        db: MongoDB database connection (optional)
        
    Returns:
        bool: True if client exists and is active, False otherwise
    """
    if not client_id:
        return False
    
    if db is None:
        db = get_db()
    
    try:
        count = await db.clients.count_documents({
            "client_id": client_id,
            "status": "active"
        })
        return count > 0
        
    except Exception as e:
        logger.error(f"Error validating client '{client_id}': {str(e)}")
        return False


async def get_client_api_key(client_id: str, db=None) -> Optional[str]:
    """
    Quick helper to get just the WebinarGeek API key for a client.
    
    Args:
        client_id (str): The client identifier
        db: MongoDB database connection (optional)
        
    Returns:
        str: WebinarGeek API key or None if not found
    """
    config = await get_client_config(client_id, db)
    if config:
        return config.get("webinar_geek_api_key")
    return None


async def get_client_webhooks(client_id: str, db=None) -> Dict[str, Any]:
    """
    Quick helper to get webhook URLs for a client.
    
    Args:
        client_id (str): The client identifier
        db: MongoDB database connection (optional)
        
    Returns:
        dict: Webhook URLs (google_sheet_url, ghl_url, custom_webhooks)
    """
    config = await get_client_config(client_id, db)
    if config:
        return {
            "google_sheet_url": config.get("google_sheet_url"),
            "ghl_url": config.get("ghl_url"),
            "custom_webhooks": config.get("custom_webhooks", [])
        }
    return {
        "google_sheet_url": None,
        "ghl_url": None,
        "custom_webhooks": []
    }


async def get_client_display_settings(client_id: str, db=None) -> Dict[str, Any]:
    """
    Quick helper to get display settings for a client.
    
    Args:
        client_id (str): The client identifier
        db: MongoDB database connection (optional)
        
    Returns:
        dict: Display settings (base_subscriber_count, timezone)
    """
    config = await get_client_config(client_id, db)
    if config:
        return {
            "base_subscriber_count": config.get("base_subscriber_count", 0),
            "timezone": config.get("timezone", "UTC")
        }
    return {
        "base_subscriber_count": 0,
        "timezone": "UTC"
    }
