"""
Scheduler management endpoints.
This module provides API endpoints to manage and monitor scheduled jobs.
"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from typing import Dict, Any, List
from app.core.scheduler import add_job, remove_job, scheduler
from app.core.webinar_sync import sync_webinars
from datetime import datetime
import logging

# Set up router
router = APIRouter()
logger = logging.getLogger(__name__)

# Job configurations
WEBINAR_SYNC_JOB_ID = "webinar_sync_job"
RETRY_WEBHOOKS_JOB_ID = "retry_webhooks_job"

@router.post("/start-webinar-sync", status_code=status.HTTP_200_OK)
async def start_webinar_sync():
    """
    Start the scheduled job to sync webinars from WebinarGeek API.
    The job will run every hour by default.
    
    Returns:
        dict: Status of the operation
    """
    try:
        job_added = add_job(
            job_id=WEBINAR_SYNC_JOB_ID,
            func=sync_webinars,
            trigger="interval",
            minutes=1,  # Run every 1 minute
            # Alternative: use cron trigger for specific times
            # trigger="cron",
            # minute="*/1"  # Every 1 minute
        )
        
        if job_added:
            # Run job immediately
            background_tasks = BackgroundTasks()
            background_tasks.add_task(sync_webinars)
            
            return {
                "status": "success", 
                "message": "Webinar sync job scheduled successfully",
                "job_id": WEBINAR_SYNC_JOB_ID,
                "next_run": scheduler.get_job(WEBINAR_SYNC_JOB_ID).next_run_time.isoformat() if scheduler.get_job(WEBINAR_SYNC_JOB_ID) else None
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to schedule webinar sync job"
            )
    except Exception as e:
        logger.error(f"Error starting webinar sync job: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start webinar sync job: {str(e)}"
        )

@router.post("/stop-webinar-sync", status_code=status.HTTP_200_OK)
async def stop_webinar_sync():
    """
    Stop the scheduled webinar sync job.
    
    Returns:
        dict: Status of the operation
    """
    try:
        job_removed = remove_job(WEBINAR_SYNC_JOB_ID)
        if job_removed:
            return {"status": "success", "message": "Webinar sync job stopped"}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {WEBINAR_SYNC_JOB_ID} not found"
            )
    except Exception as e:
        logger.error(f"Error stopping webinar sync job: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop webinar sync job: {str(e)}"
        )

@router.post("/run-webinar-sync-now", status_code=status.HTTP_200_OK)
async def run_webinar_sync_now(background_tasks: BackgroundTasks):
    """
    Run the webinar sync job immediately as a background task.
    
    Returns:
        dict: Status of the operation
    """
    try:
        background_tasks.add_task(sync_webinars)
        return {"status": "success", "message": "Webinar sync job started in background"}
    except Exception as e:
        logger.error(f"Error running webinar sync job: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to run webinar sync job: {str(e)}"
        )

@router.get("/jobs", status_code=status.HTTP_200_OK)
async def get_scheduled_jobs():
    """
    Get all scheduled jobs and their status.
    
    Returns:
        list: List of job information
    """
    try:
        jobs = []
        for job in scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "trigger": str(job.trigger),
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "func": job.func.__name__ if hasattr(job.func, "__name__") else str(job.func),
            })
        return {"jobs": jobs}
    except Exception as e:
        logger.error(f"Error getting scheduled jobs: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get scheduled jobs: {str(e)}"
        )

@router.get("/webinar-sync-status", status_code=status.HTTP_200_OK)
async def get_webinar_sync_status():
    """
    Get the status of the webinar sync job.
    
    Returns:
        dict: Job status information
    """
    try:
        job = scheduler.get_job(WEBINAR_SYNC_JOB_ID)
        if job:
            return {
                "status": "active",
                "job_id": job.id,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger)
            }
        else:
            return {"status": "inactive", "job_id": WEBINAR_SYNC_JOB_ID}
    except Exception as e:
        logger.error(f"Error getting webinar sync status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get webinar sync status: {str(e)}"
        )

@router.post("/start-retry-webhooks", status_code=status.HTTP_200_OK)
async def start_retry_webhooks():
    """
    Start the scheduled job to retry failed webhook deliveries.
    The job will run every 5 minutes by default.
    
    Returns:
        dict: Status of the operation
    """
    try:
        from app.core.retry_failed_webhooks import retry_failed_webhooks
        
        job_added = add_job(
            job_id=RETRY_WEBHOOKS_JOB_ID,
            func=retry_failed_webhooks,
            trigger="interval",
            minutes=5,  # Run every 5 minutes
        )
        
        if job_added:
            return {
                "status": "success", 
                "message": "Webhook retry job scheduled successfully",
                "job_id": RETRY_WEBHOOKS_JOB_ID,
                "next_run": scheduler.get_job(RETRY_WEBHOOKS_JOB_ID).next_run_time.isoformat() if scheduler.get_job(RETRY_WEBHOOKS_JOB_ID) else None
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to schedule webhook retry job"
            )
    except Exception as e:
        logger.error(f"Error starting webhook retry job: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start webhook retry job: {str(e)}"
        )

@router.post("/stop-retry-webhooks", status_code=status.HTTP_200_OK)
async def stop_retry_webhooks():
    """
    Stop the scheduled webhook retry job.
    
    Returns:
        dict: Status of the operation
    """
    try:
        job_removed = remove_job(RETRY_WEBHOOKS_JOB_ID)
        if job_removed:
            return {"status": "success", "message": "Webhook retry job stopped"}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {RETRY_WEBHOOKS_JOB_ID} not found"
            )
    except Exception as e:
        logger.error(f"Error stopping webhook retry job: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop webhook retry job: {str(e)}"
        )

@router.post("/run-retry-webhooks-now", status_code=status.HTTP_200_OK)
async def run_retry_webhooks_now(background_tasks: BackgroundTasks):
    """
    Run the webhook retry job immediately as a background task.
    
    Returns:
        dict: Status of the operation
    """
    try:
        from app.core.retry_failed_webhooks import retry_failed_webhooks
        background_tasks.add_task(retry_failed_webhooks)
        return {"status": "success", "message": "Webhook retry job started in background"}
    except Exception as e:
        logger.error(f"Error running webhook retry job: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to run webhook retry job: {str(e)}"
        )
