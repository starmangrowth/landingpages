"""
WebinarGeek API Broadcast integration and data synchronization.
This module provides functions to fetch broadcast data from WebinarGeek API
and sync it with our MongoDB database using the new broadcast-based approach.

MULTI-TENANT: This module syncs broadcasts for ALL active clients.
Each client has their own WebinarGeek API key stored in the clients collection.
All broadcast data is tagged with client_id for proper isolation.

Updated: January 2026 - Multi-tenant support
"""

import httpx
import logging
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from app.db.mongo import get_db
from app.core.client_config import get_all_active_clients
from pymongo.errors import PyMongoError

# Set up logger
logger = logging.getLogger(__name__)


async def make_api_request(endpoint: str, api_key: str, params: dict = None, retry_count: int = 0) -> Optional[Dict]:
    """
    Make API request to WebinarGeek with rate limiting and retry logic.
    
    Args:
        endpoint (str): API endpoint path (e.g., "/broadcasts")
        api_key (str): WebinarGeek API key for the specific client
        params (dict): Query parameters
        retry_count (int): Current retry attempt
    
    Returns:
        dict: API response data or None if failed
    """
    if not api_key:
        logger.error("API key is required for WebinarGeek API requests")
        return None
    
    try:
        # Log API request (obscuring part of the API key for security)
        masked_key = api_key[:5] + '...' + api_key[-5:] if len(api_key) > 10 else '***masked***'
        
        base_url = "https://app.webinargeek.com/api/v2"
        url = f"{base_url}{endpoint}"
        
        headers = {
            "Api-Token": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # Rate limiting and retry settings
        max_retries = 3
        backoff_factor = 2
        rate_limit_delay = 1

        # Add delay for rate limiting (only for initial requests)
        if retry_count == 0:
            time.sleep(rate_limit_delay)

        logger.info(f"Making request to: {url} (API key: {masked_key})")
        if params:
            logger.debug(f"Parameters: {params}")

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, params=params, timeout=30.0)

            # Handle rate limiting
            if response.status_code == 429:
                if retry_count < max_retries:
                    wait_time = backoff_factor ** retry_count
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds before retry {retry_count + 1}/{max_retries}")
                    time.sleep(wait_time)
                    return await make_api_request(endpoint, api_key, params, retry_count + 1)
                else:
                    logger.error("Max retries exceeded for rate limiting")
                    return None

            # Handle authentication errors
            if response.status_code in [401, 403]:
                logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                return None

            # Handle other errors
            if response.status_code != 200:
                logger.error(f"API request failed: {response.status_code} - {response.text}")
                return None

            return response.json()

    except Exception as e:
        logger.error(f"Request error: {e}")
        return None


async def fetch_all_broadcasts_paginated(api_key: str) -> Optional[Dict]:
    """
    Fetch ALL broadcasts using efficient pagination from WebinarGeek API.
    
    Args:
        api_key (str): WebinarGeek API key for the specific client
    
    Returns:
        dict: Complete broadcasts data with all pages combined or None if fetching failed
    """
    try:
        # Set optimal pagination parameters
        filters = {
            'per_page': 1000,  # Use maximum page size
            'order': 'date',
            'sort': 'desc'
        }

        all_broadcasts = []
        current_page = 1
        total_pages = 1

        logger.info(f"Starting paginated broadcast retrieval with {filters['per_page']} per page")

        while current_page <= total_pages:
            # Update page number
            filters['page'] = current_page

            logger.info(f"Fetching page {current_page} of {total_pages}")

            # Make request with client-specific API key
            response = await make_api_request("/broadcasts", api_key, params=filters)

            if not response:
                logger.error(f"Failed to fetch page {current_page}")
                break

            # Extract broadcasts from this page
            broadcasts = response.get("broadcasts", [])
            all_broadcasts.extend(broadcasts)

            # Update pagination info
            pages_info = response.get("pages", {})
            current_page = pages_info.get("page", current_page)
            total_pages = pages_info.get("total_pages", total_pages)

            logger.info(f"Page {current_page}: Retrieved {len(broadcasts)} broadcasts (Total so far: {len(all_broadcasts)})")

            # If this is the last page, break
            if current_page >= total_pages:
                break

            current_page += 1

        # Create combined response
        combined_response = {
            "total_count": len(all_broadcasts),
            "broadcasts": all_broadcasts,
            "pages": {
                "next": None,  # We've fetched everything
                "page": 1,     # Reset to page 1 since we have all data
                "per_page": filters['per_page'],
                "total_pages": 1  # All data in single response
            },
            "metadata": {
                "retrieved_at": datetime.now().isoformat(),
                "total_pages_fetched": current_page,
                "api_calls_made": current_page,
                "optimization": f"Reduced API calls by {current_page}x using max page size"
            }
        }

        logger.info(f"✅ Completed! Retrieved {len(all_broadcasts)} total broadcasts in {current_page} API calls")

        return combined_response

    except Exception as e:
        logger.error(f"Exception occurred while fetching broadcasts: {e}")
        return None


def get_next_immediate_upcoming_broadcast(broadcasts_data: Dict) -> Optional[Dict]:
    """
    Get the next immediate upcoming broadcast from the broadcasts data.
    This function finds the broadcast that is closest to the current date but still in the future.
    
    Args:
        broadcasts_data (dict): Pre-loaded broadcasts data
    
    Returns:
        dict: Next immediate upcoming broadcast data or None if not found
    """
    if not broadcasts_data or 'broadcasts' not in broadcasts_data:
        logger.error("No broadcasts data available")
        return None

    total_broadcasts = len(broadcasts_data['broadcasts'])
    logger.info(f"🔍 Analyzing {total_broadcasts} broadcasts for upcoming selection...")
    
    # Get current timestamp for comparison
    current_timestamp = datetime.utcnow().timestamp()
    logger.info(f"⏰ Current timestamp: {current_timestamp} ({convert_timestamp(current_timestamp)})")
    
    # Find upcoming active broadcasts (not ended, not cancelled, and in the future)
    upcoming_broadcasts = []
    ended_count = 0
    cancelled_count = 0
    past_count = 0
    invalid_date_count = 0
    
    for broadcast in broadcasts_data['broadcasts']:
        broadcast_id = broadcast.get('id')
        broadcast_timestamp = broadcast.get('date')
        has_ended = broadcast.get('has_ended', False)
        cancelled = broadcast.get('cancelled', False)
        
        # Count reasons for exclusion
        if has_ended:
            ended_count += 1
        elif cancelled:
            cancelled_count += 1
        elif not broadcast_timestamp:
            invalid_date_count += 1
        elif broadcast_timestamp <= current_timestamp:
            past_count += 1
        elif (
            not has_ended and 
            not cancelled and 
            broadcast_timestamp and 
            broadcast_timestamp > current_timestamp
        ):
            upcoming_broadcasts.append(broadcast)
            logger.debug(f"✅ Qualified upcoming broadcast: ID {broadcast_id}, Date: {convert_timestamp(broadcast_timestamp)}")

    # Log selection statistics
    logger.info(f"📈 Broadcast Analysis Summary:")
    logger.info(f"   Total broadcasts: {total_broadcasts}")
    logger.info(f"   Ended broadcasts: {ended_count}")
    logger.info(f"   Cancelled broadcasts: {cancelled_count}")
    logger.info(f"   Past broadcasts: {past_count}")
    logger.info(f"   Invalid date broadcasts: {invalid_date_count}")
    logger.info(f"   Qualified upcoming broadcasts: {len(upcoming_broadcasts)}")
    
    if not upcoming_broadcasts:
        logger.warning("❌ No upcoming active broadcasts found in the future")
        return None

    # Sort by date (earliest first) and return the next immediate upcoming broadcast
    upcoming_broadcasts.sort(key=lambda x: x['date'], reverse=False)
    next_broadcast = upcoming_broadcasts[0]

    logger.info(f"🎯 SELECTED next immediate upcoming broadcast: ID {next_broadcast['id']}, Date: {convert_timestamp(next_broadcast['date'])}")
    
    return next_broadcast


# Keep the old function name for backward compatibility
def get_latest_upcoming_broadcast(broadcasts_data: Dict) -> Optional[Dict]:
    """
    Get the next immediate upcoming broadcast (alias for backward compatibility).
    """
    return get_next_immediate_upcoming_broadcast(broadcasts_data)


def convert_timestamp(timestamp) -> str:
    """
    Convert Unix timestamp to human-readable date string.
    
    Args:
        timestamp: Unix timestamp as integer
        
    Returns:
        str: Formatted date string or 'N/A' if invalid
    """
    if not timestamp:
        return 'N/A'
    try:
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError):
        return str(timestamp)


def process_broadcast_for_storage(broadcast: Dict, client_id: str) -> Dict:
    """
    Process raw broadcast data for storage, formatting dates and enriching the data.
    
    Args:
        broadcast (dict): Raw broadcast data from API
        client_id (str): Client identifier for multi-tenant isolation
        
    Returns:
        dict: Processed broadcast data ready for storage with client_id
    """
    # Create a complete record for DB storage with client_id
    processed_broadcast = {
        "client_id": client_id,  # MULTI-TENANT: Tag with client
        "broadcast_id": broadcast.get("id"),
        "date": broadcast.get("date"),
        "readable_date": convert_timestamp(broadcast.get("date")),
        "has_ended": broadcast.get("has_ended", False),
        "cancelled": broadcast.get("cancelled", False),
        "subscriptions_count": broadcast.get("subscriptions_count", 0),
        "viewers_count": broadcast.get("viewers_count", 0),
        "live_viewers_count": broadcast.get("live_viewers_count", 0),
        "replay_link": broadcast.get("public_replay_link"),
        "raw_data": broadcast,
        "last_synced": datetime.utcnow()
    }
    
    return processed_broadcast


async def sync_client_webinars(client: Dict, db) -> Dict:
    """
    Synchronize webinars for a single client.
    
    Args:
        client (dict): Client document from database
        db: MongoDB database connection
        
    Returns:
        dict: Sync results for this client
    """
    client_id = client.get("client_id")
    client_name = client.get("client_name", client_id)
    api_key = client.get("webinar_geek", {}).get("api_key")
    
    result = {
        "client_id": client_id,
        "client_name": client_name,
        "success": False,
        "broadcasts_count": 0,
        "new_count": 0,
        "updated_count": 0,
        "error_count": 0,
        "upcoming_broadcast_id": None,
        "error": None
    }
    
    if not api_key:
        logger.warning(f"⚠️ Client '{client_id}' has no WebinarGeek API key - skipping")
        result["error"] = "No WebinarGeek API key configured"
        return result
    
    logger.info(f"🔄 Syncing broadcasts for client: {client_id} ({client_name})")
    
    try:
        # Fetch all broadcasts from API using CLIENT's API key
        broadcasts_response = await fetch_all_broadcasts_paginated(api_key)
        
        if not broadcasts_response or 'broadcasts' not in broadcasts_response:
            logger.error(f"Failed to fetch broadcasts for client '{client_id}'")
            result["error"] = "Failed to fetch broadcasts from WebinarGeek API"
            return result
        
        all_broadcasts = broadcasts_response.get('broadcasts', [])
        logger.info(f"Successfully fetched {len(all_broadcasts)} broadcasts for client '{client_id}'")
        
        result["broadcasts_count"] = len(all_broadcasts)
        
        # Process and store all broadcasts with client_id
        for broadcast in all_broadcasts:
            try:
                broadcast_id = broadcast.get("id")
                if not broadcast_id:
                    logger.warning(f"Skipping broadcast with no ID for client '{client_id}'")
                    continue
                
                # Process broadcast data with client_id
                processed_broadcast = process_broadcast_for_storage(broadcast, client_id)
                
                # Update or insert in database (upsert) - unique by client_id + broadcast_id
                db_result = await db.broadcasts.update_one(
                    {
                        "client_id": client_id,
                        "broadcast_id": processed_broadcast["broadcast_id"]
                    },
                    {"$set": processed_broadcast},
                    upsert=True
                )
                
                if db_result.matched_count > 0:
                    result["updated_count"] += 1
                else:
                    result["new_count"] += 1
            
            except Exception as e:
                result["error_count"] += 1
                logger.error(f"Error processing broadcast {broadcast.get('id', 'unknown')} for client '{client_id}': {str(e)}")
        
        logger.info(f"Processed {len(all_broadcasts)} broadcasts for client '{client_id}': "
                   f"{result['new_count']} new, {result['updated_count']} updated, {result['error_count']} errors")
        
        # Find the latest upcoming broadcast for this client
        latest_upcoming_broadcast = get_latest_upcoming_broadcast(broadcasts_response)
        
        # Update upcoming-broadcast for THIS CLIENT
        try:
            if latest_upcoming_broadcast:
                processed_upcoming = process_broadcast_for_storage(latest_upcoming_broadcast, client_id)
                processed_upcoming["sync_metadata"] = {
                    "sync_job_timestamp": datetime.utcnow(),
                    "total_broadcasts_processed": len(all_broadcasts),
                    "selection_criteria_met": True
                }
                
                # Upsert by client_id - one upcoming broadcast per client
                await db["upcoming-broadcast"].update_one(
                    {"client_id": client_id},
                    {"$set": processed_upcoming},
                    upsert=True
                )
                
                result["upcoming_broadcast_id"] = latest_upcoming_broadcast['id']
                logger.info(f"✅ Updated upcoming broadcast for client '{client_id}': ID {latest_upcoming_broadcast['id']}")
            else:
                # No upcoming broadcast - store null document for this client
                null_doc = {
                    "client_id": client_id,
                    "broadcast_id": None,
                    "message": "No upcoming broadcasts found",
                    "last_synced": datetime.utcnow(),
                    "sync_metadata": {
                        "sync_job_timestamp": datetime.utcnow(),
                        "total_broadcasts_processed": len(all_broadcasts),
                        "selection_criteria_met": False
                    }
                }
                
                await db["upcoming-broadcast"].update_one(
                    {"client_id": client_id},
                    {"$set": null_doc},
                    upsert=True
                )
                
                logger.warning(f"⚠️ No upcoming broadcasts found for client '{client_id}'")
        
        except Exception as e:
            logger.error(f"Error updating upcoming broadcast for client '{client_id}': {str(e)}")
            result["error"] = f"Failed to update upcoming broadcast: {str(e)}"
        
        result["success"] = True
        return result
        
    except Exception as e:
        logger.error(f"Error syncing broadcasts for client '{client_id}': {str(e)}")
        result["error"] = str(e)
        return result


async def sync_webinars():
    """
    Synchronize WebinarGeek broadcasts for ALL active clients.
    
    MULTI-TENANT: This function loops through all active clients in the database,
    fetches broadcasts using each client's WebinarGeek API key, and stores
    the data with proper client_id isolation.
    
    The synchronization process for EACH client:
    1. Fetches ALL broadcasts from WebinarGeek API using client's API key
    2. Processes each broadcast for storage (formatting dates, enriching data)
    3. Stores all broadcasts in the 'broadcasts' collection with client_id
    4. Identifies the latest upcoming broadcast (has_ended=False AND cancelled=False)
    5. Stores the upcoming broadcast in 'upcoming-broadcast' collection with client_id
    6. Records sync info per client
    """
    start_time = datetime.utcnow()
    logger.info("=" * 80)
    logger.info(f"🚀 MULTI-TENANT BROADCAST SYNC STARTED at {start_time}")
    logger.info("=" * 80)
    
    try:
        # Get MongoDB database
        db = get_db()
        
        # Get all active clients
        clients = await get_all_active_clients(db)
        
        if not clients:
            logger.warning("⚠️ No active clients found - skipping sync")
            return False
        
        logger.info(f"📊 Found {len(clients)} active clients to sync")
        
        # Track overall results
        total_results = {
            "clients_processed": 0,
            "clients_successful": 0,
            "clients_failed": 0,
            "total_broadcasts": 0,
            "total_new": 0,
            "total_updated": 0,
            "total_errors": 0,
            "client_results": []
        }
        
        # Sync each client
        for client in clients:
            client_id = client.get("client_id")
            logger.info(f"\n{'─' * 40}")
            logger.info(f"📌 Processing client: {client_id}")
            logger.info(f"{'─' * 40}")
            
            # Sync this client
            client_result = await sync_client_webinars(client, db)
            total_results["client_results"].append(client_result)
            total_results["clients_processed"] += 1
            
            if client_result["success"]:
                total_results["clients_successful"] += 1
                total_results["total_broadcasts"] += client_result["broadcasts_count"]
                total_results["total_new"] += client_result["new_count"]
                total_results["total_updated"] += client_result["updated_count"]
                total_results["total_errors"] += client_result["error_count"]
            else:
                total_results["clients_failed"] += 1
            
            # Update sync info for this client
            try:
                sync_info = {
                    "client_id": client_id,
                    "timestamp": datetime.utcnow(),
                    "broadcasts_count": client_result["broadcasts_count"],
                    "new_count": client_result["new_count"],
                    "updated_count": client_result["updated_count"],
                    "error_count": client_result["error_count"],
                    "has_upcoming_broadcast": client_result["upcoming_broadcast_id"] is not None,
                    "upcoming_broadcast_id": client_result["upcoming_broadcast_id"],
                    "success": client_result["success"],
                    "error": client_result.get("error")
                }
                
                await db.broadcast_sync_info.update_one(
                    {"client_id": client_id},
                    {"$set": sync_info},
                    upsert=True
                )
            except Exception as e:
                logger.warning(f"Could not update sync info for client '{client_id}': {str(e)}")
        
        # Log final results
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "=" * 80)
        logger.info("🏁 MULTI-TENANT BROADCAST SYNC COMPLETED")
        logger.info("=" * 80)
        logger.info(f"⏱️  Duration: {duration:.2f} seconds")
        logger.info(f"👥 Clients processed: {total_results['clients_processed']}")
        logger.info(f"✅ Successful: {total_results['clients_successful']}")
        logger.info(f"❌ Failed: {total_results['clients_failed']}")
        logger.info(f"📊 Total broadcasts: {total_results['total_broadcasts']}")
        logger.info(f"🆕 New: {total_results['total_new']}")
        logger.info(f"🔄 Updated: {total_results['total_updated']}")
        logger.info(f"⚠️  Errors: {total_results['total_errors']}")
        logger.info("=" * 80)
        
        # Log per-client summary
        for result in total_results["client_results"]:
            status = "✅" if result["success"] else "❌"
            logger.info(f"  {status} {result['client_id']}: {result['broadcasts_count']} broadcasts, "
                       f"upcoming: {result['upcoming_broadcast_id'] or 'None'}")
        
        return total_results["clients_failed"] == 0
    
    except PyMongoError as db_error:
        logger.error(f"❌ MongoDB error during broadcast sync: {str(db_error)}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error during broadcast sync: {str(e)}")
        return False
