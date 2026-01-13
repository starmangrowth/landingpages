"""
Scheduler module for handling cron jobs and scheduled tasks in the application.
This module provides functions to set up and manage scheduled tasks using APScheduler.
"""

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
from datetime import datetime
import logging

# Set up logger
logger = logging.getLogger(__name__)

# Configure the scheduler with desired job stores and executors
jobstores = {
    'default': MemoryJobStore()
}
executors = {
    'default': AsyncIOExecutor()
}
job_defaults = {
    'coalesce': True,      # Skip missed runs instead of executing all
    'max_instances': 1     # Only allow ONE retry job at a time (prevents piling up)
}

scheduler = AsyncIOScheduler(
    jobstores=jobstores,
    executors=executors,
    job_defaults=job_defaults,
    timezone='UTC'
)

def init_scheduler():
    """Initialize the scheduler if it's not already running."""
    if not scheduler.running:
        try:
            scheduler.start()
            logger.info(f"Scheduler started successfully at {datetime.now()}")
        except Exception as e:
            logger.error(f"Failed to start scheduler: {str(e)}")
    else:
        logger.info("Scheduler is already running")

def add_job(job_id, func, trigger, **trigger_args):
    """Add a job to the scheduler with the specified trigger."""
    try:
        scheduler.add_job(
            func=func,
            trigger=trigger,
            id=job_id,
            replace_existing=True,
            **trigger_args
        )
        logger.info(f"Job {job_id} added successfully with trigger: {trigger}")
        return True
    except Exception as e:
        logger.error(f"Failed to add job {job_id}: {str(e)}")
        return False

def remove_job(job_id):
    """Remove a job from the scheduler by its ID."""
    try:
        scheduler.remove_job(job_id)
        logger.info(f"Job {job_id} removed successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to remove job {job_id}: {str(e)}")
        return False

def shutdown():
    """Shutdown the scheduler."""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler shut down successfully")
