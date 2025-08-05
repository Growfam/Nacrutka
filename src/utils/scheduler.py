"""
Task scheduler for periodic jobs
"""
from typing import Callable, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime

from src.utils.logger import get_logger

logger = get_logger(__name__)


class BotScheduler:
    """Manages scheduled tasks"""

    def __init__(self):
        self.scheduler = AsyncIOScheduler(
            timezone="UTC",
            job_defaults={
                'coalesce': True,
                'max_instances': 1,
                'misfire_grace_time': 30
            }
        )

    def add_job(
            self,
            func: Callable,
            interval_seconds: int,
            job_id: str,
            start_date: Optional[datetime] = None
    ):
        """Add a job to scheduler"""
        trigger = IntervalTrigger(
            seconds=interval_seconds,
            start_date=start_date
        )

        self.scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            replace_existing=True,
            name=job_id
        )

        logger.info(
            "Job scheduled",
            job_id=job_id,
            interval=interval_seconds
        )

    def remove_job(self, job_id: str):
        """Remove a job from scheduler"""
        try:
            self.scheduler.remove_job(job_id)
            logger.info("Job removed", job_id=job_id)
        except Exception as e:
            logger.error("Failed to remove job", job_id=job_id, error=str(e))

    def start(self):
        """Start scheduler"""
        self.scheduler.start()
        logger.info("Scheduler started")

    def stop(self):
        """Stop scheduler"""
        self.scheduler.shutdown(wait=True)
        logger.info("Scheduler stopped")

    def get_jobs(self):
        """Get all scheduled jobs"""
        return self.scheduler.get_jobs()

    def pause_job(self, job_id: str):
        """Pause a job"""
        self.scheduler.pause_job(job_id)
        logger.info("Job paused", job_id=job_id)

    def resume_job(self, job_id: str):
        """Resume a job"""
        self.scheduler.resume_job(job_id)
        logger.info("Job resumed", job_id=job_id)