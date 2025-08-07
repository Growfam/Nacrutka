"""
Scheduler for periodic tasks
"""
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from src.core.post_processor import post_processor
from src.services.order_manager import order_manager
from src.services.twiboost_client import twiboost_client
from src.database.repositories.post_repo import post_repo
from src.database.repositories.channel_repo import channel_repo
from src.database.repositories.service_repo import service_repo
from src.utils.logger import get_logger, LoggerMixin
from src.config import settings


logger = get_logger(__name__)


class TaskScheduler(LoggerMixin):
    """Manages scheduled tasks"""

    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.tasks_running = False
        self.task_stats = {
            "process_posts": {"runs": 0, "errors": 0},
            "execute_orders": {"runs": 0, "errors": 0},
            "check_statuses": {"runs": 0, "errors": 0},
            "sync_services": {"runs": 0, "errors": 0},
            "cleanup": {"runs": 0, "errors": 0}
        }

    async def start(self):
        """Start scheduler with all tasks"""
        try:
            # Process new posts every 10 seconds
            self.scheduler.add_job(
                self._process_posts_task,
                IntervalTrigger(seconds=settings.process_interval),
                id="process_posts",
                name="Process new posts",
                misfire_grace_time=30
            )

            # Execute pending orders every 15 seconds
            self.scheduler.add_job(
                self._execute_orders_task,
                IntervalTrigger(seconds=15),
                id="execute_orders",
                name="Execute pending orders",
                misfire_grace_time=30
            )

            # Check order statuses every minute
            self.scheduler.add_job(
                self._check_statuses_task,
                IntervalTrigger(minutes=1),
                id="check_statuses",
                name="Check order statuses",
                misfire_grace_time=60
            )

            # Sync Twiboost services every hour
            self.scheduler.add_job(
                self._sync_services_task,
                IntervalTrigger(hours=1),
                id="sync_services",
                name="Sync Twiboost services"
            )

            # Retry failed orders every 5 minutes
            self.scheduler.add_job(
                self._retry_failed_task,
                IntervalTrigger(minutes=5),
                id="retry_failed",
                name="Retry failed orders"
            )

            # Daily cleanup at 3 AM
            self.scheduler.add_job(
                self._cleanup_task,
                CronTrigger(hour=3, minute=0),
                id="cleanup",
                name="Daily cleanup"
            )

            # Weekly statistics report on Mondays at 9 AM
            self.scheduler.add_job(
                self._generate_stats_task,
                CronTrigger(day_of_week=0, hour=9, minute=0),
                id="weekly_stats",
                name="Weekly statistics"
            )

            # Start scheduler
            self.scheduler.start()
            self.tasks_running = True

            self.log_info(
                "Scheduler started",
                jobs_count=len(self.scheduler.get_jobs())
            )

        except Exception as e:
            self.log_error("Failed to start scheduler", error=e)
            raise

    async def stop(self):
        """Stop scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
            self.tasks_running = False
            self.log_info("Scheduler stopped")

    async def _process_posts_task(self):
        """Task: Process new posts"""
        task_name = "process_posts"
        try:
            self.log_debug(f"Running task: {task_name}")

            result = await post_processor.process_new_posts(limit=50)

            self.task_stats[task_name]["runs"] += 1

            if result > 0:
                self.log_info(f"Processed {result} new posts")

        except Exception as e:
            self.task_stats[task_name]["errors"] += 1
            self.log_error(f"Task {task_name} failed", error=e)

    async def _execute_orders_task(self):
        """Task: Execute pending orders"""
        task_name = "execute_orders"
        try:
            self.log_debug(f"Running task: {task_name}")

            result = await order_manager.execute_pending_orders(limit=20)

            self.task_stats[task_name]["runs"] += 1

            if result > 0:
                self.log_info(f"Executed {result} orders")

        except Exception as e:
            self.task_stats[task_name]["errors"] += 1
            self.log_error(f"Task {task_name} failed", error=e)

    async def _check_statuses_task(self):
        """Task: Check order statuses"""
        task_name = "check_statuses"
        try:
            self.log_debug(f"Running task: {task_name}")

            result = await order_manager.check_orders_status(limit=100)

            self.task_stats[task_name]["runs"] += 1

            if result > 0:
                self.log_info(f"Updated {result} order statuses")

        except Exception as e:
            self.task_stats[task_name]["errors"] += 1
            self.log_error(f"Task {task_name} failed", error=e)

    async def _sync_services_task(self):
        """Task: Sync Twiboost services"""
        task_name = "sync_services"
        try:
            self.log_info(f"Running task: {task_name}")

            # 1. Отримуємо сервіси з API
            services = await twiboost_client.get_services(force_refresh=True)

            # 2. ВАЖЛИВО! Зберігаємо всі сервіси в БД з їх оригінальними ID
            synced_count = await service_repo.sync_services(services)

            self.log_info(f"Synced {synced_count} Telegram services to database")

            # 3. Отримуємо сервіси з БД для оновлення каналів
            view_services = await service_repo.get_services_by_type("views")
            reaction_mapping = await service_repo.get_reaction_services()
            repost_services = await service_repo.get_services_by_type("reposts")

            # 4. Оновлюємо service_ids для всіх каналів
            channels = await channel_repo.get_active_channels()

            for channel in channels:
                # Оновлюємо views
                if view_services:
                    # Шукаємо оптимальний сервіс (наприклад з "10 в минуту")
                    best_view = None
                    for service in view_services:
                        if '10 в минуту' in service.name or '10 в мин' in service.name:
                            best_view = service.service_id
                            break

                    if not best_view:
                        best_view = view_services[0].service_id

                    await channel_repo.update_service_ids(
                        channel.id,
                        "views",
                        {"views": best_view}
                    )

                    self.log_debug(
                        f"Channel {channel.id}: views service set to {best_view}"
                    )

                # Оновлюємо reactions
                if reaction_mapping:
                    await channel_repo.update_service_ids(
                        channel.id,
                        "reactions",
                        reaction_mapping
                    )

                    self.log_debug(
                        f"Channel {channel.id}: reaction services mapped",
                        mapping=reaction_mapping
                    )

                # Оновлюємо reposts
                if repost_services:
                    await channel_repo.update_service_ids(
                        channel.id,
                        "reposts",
                        {"reposts": repost_services[0].service_id}
                    )

                    self.log_debug(
                        f"Channel {channel.id}: repost service set to {repost_services[0].service_id}"
                    )

            self.task_stats[task_name]["runs"] += 1

            self.log_info(
                "Service sync completed",
                total_services=len(services),
                synced_to_db=synced_count,
                channels_updated=len(channels)
            )

        except Exception as e:
            self.task_stats[task_name]["errors"] += 1
            self.log_error(f"Task {task_name} failed", error=e)

    async def _retry_failed_task(self):
        """Task: Retry failed orders"""
        try:
            self.log_debug("Running task: retry_failed")

            # Retry failed posts
            failed_posts = await post_processor.reprocess_failed_posts(limit=10)

            # Retry failed orders
            failed_orders = await order_manager.retry_failed_orders(limit=10)

            if failed_posts > 0 or failed_orders > 0:
                self.log_info(
                    "Retried failed items",
                    posts=failed_posts,
                    orders=failed_orders
                )

        except Exception as e:
            self.log_error("Task retry_failed failed", error=e)

    async def _cleanup_task(self):
        """Task: Daily cleanup"""
        task_name = "cleanup"
        try:
            self.log_info(f"Running task: {task_name}")

            # Clean old posts (30 days)
            posts_deleted = await post_repo.cleanup_old_posts(days=30)

            # Clean old orders (7 days)
            orders_deleted = await order_manager.cleanup_completed_orders(days=7)

            self.task_stats[task_name]["runs"] += 1

            self.log_info(
                "Cleanup completed",
                posts_deleted=posts_deleted,
                orders_deleted=orders_deleted
            )

        except Exception as e:
            self.task_stats[task_name]["errors"] += 1
            self.log_error(f"Task {task_name} failed", error=e)

    async def _generate_stats_task(self):
        """Task: Generate weekly statistics"""
        try:
            self.log_info("Generating weekly statistics")

            # Get statistics
            post_stats = await post_processor.get_processing_stats()
            order_stats = await order_manager.get_execution_stats()

            # Get balance
            balance = await twiboost_client.get_balance()

            # Log comprehensive stats
            self.log_info(
                "📊 WEEKLY STATISTICS",
                posts_processed=post_stats.get("total", 0),
                posts_success_rate=post_stats.get("success_rate", 0),
                orders_executed=order_stats.get("total_orders", 0),
                orders_success_rate=order_stats.get("success_rate", 0),
                active_orders=order_stats.get("active_orders", 0),
                balance=balance.get("balance"),
                currency=balance.get("currency"),
                task_stats=self.task_stats
            )

            # Reset session counters
            post_processor.processed_count = 0
            post_processor.error_count = 0
            order_manager.execution_count = 0
            order_manager.error_count = 0

        except Exception as e:
            self.log_error("Task generate_stats failed", error=e)

    def get_job_info(self) -> List[Dict[str, Any]]:
        """Get information about scheduled jobs"""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger),
                "stats": self.task_stats.get(job.id, {})
            })
        return jobs

    async def trigger_job(self, job_id: str):
        """Manually trigger a job"""
        job = self.scheduler.get_job(job_id)
        if job:
            job.modify(next_run_time=datetime.now())
            self.log_info(f"Manually triggered job: {job_id}")
            return True
        return False

    def pause_job(self, job_id: str):
        """Pause a job"""
        job = self.scheduler.get_job(job_id)
        if job:
            job.pause()
            self.log_info(f"Paused job: {job_id}")
            return True
        return False

    def resume_job(self, job_id: str):
        """Resume a paused job"""
        job = self.scheduler.get_job(job_id)
        if job:
            job.resume()
            self.log_info(f"Resumed job: {job_id}")
            return True
        return False


# Global scheduler instance
task_scheduler = TaskScheduler()