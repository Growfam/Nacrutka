"""
Scheduler for periodic tasks with dual API support
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
from src.services.nakrutochka_client import nakrutochka_client
from src.database.repositories.post_repo import post_repo
from src.database.repositories.channel_repo import channel_repo
from src.database.repositories.service_repo import service_repo
from src.database.repositories.order_repo import order_repo
from src.utils.logger import get_logger, LoggerMixin
from src.config import settings


logger = get_logger(__name__)


class TaskScheduler(LoggerMixin):
    """Manages scheduled tasks with dual API support"""

    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.tasks_running = False
        self._process_posts_running = False
        self.task_stats = {
            "process_posts": {"runs": 0, "errors": 0},
            "execute_orders": {"runs": 0, "errors": 0},
            "check_statuses": {"runs": 0, "errors": 0},
            "sync_services": {"runs": 0, "errors": 0},
            "cleanup": {"runs": 0, "errors": 0},
            "api_health": {"runs": 0, "errors": 0}
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

            # Sync services from both APIs every hour
            self.scheduler.add_job(
                self._sync_services_task,
                IntervalTrigger(hours=1),
                id="sync_services",
                name="Sync API services"
            )

            # Check API health every 5 minutes
            self.scheduler.add_job(
                self._api_health_check_task,
                IntervalTrigger(minutes=5),
                id="api_health",
                name="API health check"
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
                "Scheduler started with dual API support",
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
        """Task: Process new posts with duplicate prevention"""
        task_name = "process_posts"

        # Check if already running
        if self._process_posts_running:
            self.log_debug(f"Task {task_name} already running, skipping")
            return

        try:
            self._process_posts_running = True
            self.log_debug(f"Running task: {task_name}")

            # Reset stuck posts first
            stuck_count = await post_repo.reset_stuck_processing(minutes=30)
            if stuck_count > 0:
                self.log_info(f"Reset {stuck_count} stuck posts before processing")

            # Check how many unprocessed posts
            unprocessed = await post_repo.get_unprocessed_posts_count()
            if unprocessed > 0:
                self.log_info(f"Found {unprocessed} unprocessed posts total")

            # Process posts
            result = await post_processor.process_new_posts(limit=50)

            self.task_stats[task_name]["runs"] += 1

            if result > 0:
                self.log_info(
                    f"Processed {result} new posts",
                    total_unprocessed_remaining=unprocessed - result
                )

            # Cleanup duplicates every 100 runs
            if self.task_stats[task_name]["runs"] % 100 == 0:
                duplicates = await order_repo.cleanup_duplicate_orders()
                if duplicates > 0:
                    self.log_warning(f"Cleaned {duplicates} duplicate orders")

        except Exception as e:
            self.task_stats[task_name]["errors"] += 1
            self.log_error(f"Task {task_name} failed", error=e)
        finally:
            self._process_posts_running = False

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
        """Task: Check order statuses from both APIs"""
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
        """Task: Sync services from both APIs"""
        task_name = "sync_services"
        try:
            self.log_info(f"Running task: {task_name}")

            # 1. Sync Twiboost services
            self.log_info("Syncing Twiboost services...")
            try:
                twiboost_services = await twiboost_client.get_services(force_refresh=True)
                twiboost_synced = await service_repo.sync_twiboost_services(twiboost_services)
                self.log_info(f"Synced {twiboost_synced} Twiboost services")
            except Exception as e:
                self.log_error(f"Failed to sync Twiboost services: {e}")

            # 2. Sync Nakrutochka services
            self.log_info("Syncing Nakrutochka services...")
            try:
                nakrutochka_services = await nakrutochka_client.get_services(force_refresh=True)
                nakrutochka_synced = await service_repo.sync_nakrutochka_services(nakrutochka_services)
                self.log_info(f"Synced {nakrutochka_synced} Nakrutochka services")
            except Exception as e:
                self.log_error(f"Failed to sync Nakrutochka services: {e}")

            # 3. Get service counts
            service_counts = await service_repo.get_all_active_services_count()

            self.log_info(
                "Services synced",
                twiboost=service_counts.get("twiboost", {}),
                nakrutochka=service_counts.get("nakrutochka", {})
            )

            # 4. Update channel mappings
            channels = await channel_repo.get_active_channels()

            for channel in channels:
                # Set API preferences
                api_preferences = {
                    "views": "twiboost",
                    "reactions": "nakrutochka",
                    "reposts": "nakrutochka"
                }
                await channel_repo.update_api_preferences(channel.id, api_preferences)

                # Update service mappings for Twiboost (views)
                view_services = await service_repo.get_services_by_type("views")
                if view_services:
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
                        {"views": best_view},
                        api_provider="twiboost"
                    )

                # Update service mappings for Nakrutochka (reactions)
                reaction_mapping = await service_repo.get_nakrutochka_reaction_services()
                if reaction_mapping:
                    await channel_repo.update_service_ids(
                        channel.id,
                        "reactions",
                        reaction_mapping,
                        api_provider="nakrutochka"
                    )

                # Update service mappings for Nakrutochka (reposts)
                repost_services = await service_repo.get_nakrutochka_services_by_type("reposts")
                if repost_services:
                    await channel_repo.update_service_ids(
                        channel.id,
                        "reposts",
                        {"reposts": repost_services[0].service_id},
                        api_provider="nakrutochka"
                    )

                self.log_debug(f"Updated mappings for channel {channel.title}")

            # 5. Get API balances
            balances = await order_manager.get_api_balances()

            self.log_info(
                "API Balances",
                twiboost=f"{balances['twiboost'].get('balance', 0)} {balances['twiboost'].get('currency', 'USD')}",
                nakrutochka=f"{balances['nakrutochka'].get('balance', 0)} {balances['nakrutochka'].get('currency', 'RUB')}"
            )

            # 6. Ensure all channels have usernames
            await channel_repo.ensure_all_usernames()

            self.task_stats[task_name]["runs"] += 1

        except Exception as e:
            self.task_stats[task_name]["errors"] += 1
            self.log_error(f"Task {task_name} failed", error=e)

    async def _api_health_check_task(self):
        """Task: Check API health and availability"""
        task_name = "api_health"
        try:
            self.log_debug(f"Running task: {task_name}")

            health_status = {
                "twiboost": False,
                "nakrutochka": False
            }

            # Check Twiboost
            try:
                balance = await twiboost_client.get_balance()
                health_status["twiboost"] = True
                self.log_debug(f"Twiboost health: OK, balance: {balance['balance']}")
            except Exception as e:
                self.log_warning(f"Twiboost health check failed: {e}")

            # Check Nakrutochka
            try:
                balance = await nakrutochka_client.get_balance()
                health_status["nakrutochka"] = True
                self.log_debug(f"Nakrutochka health: OK, balance: {balance['balance']}")
            except Exception as e:
                self.log_warning(f"Nakrutochka health check failed: {e}")

            # Update API availability in settings if needed
            if not health_status["twiboost"] and health_status["nakrutochka"]:
                self.log_warning("Twiboost unavailable, using Nakrutochka for all services")
            elif health_status["twiboost"] and not health_status["nakrutochka"]:
                self.log_warning("Nakrutochka unavailable, using Twiboost for all services")
            elif not health_status["twiboost"] and not health_status["nakrutochka"]:
                self.log_error("Both APIs unavailable!")

            self.task_stats[task_name]["runs"] += 1

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

            # Clean duplicate orders
            duplicates_deleted = await order_repo.cleanup_duplicate_orders()

            self.task_stats[task_name]["runs"] += 1

            self.log_info(
                "Cleanup completed",
                posts_deleted=posts_deleted,
                orders_deleted=orders_deleted,
                duplicates_deleted=duplicates_deleted
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
            api_stats = await order_repo.get_statistics_by_api()

            # Get balances
            balances = await order_manager.get_api_balances()

            # Log comprehensive stats
            self.log_info(
                "📊 WEEKLY STATISTICS",
                posts_processed=post_stats.get("total", 0),
                posts_success_rate=post_stats.get("success_rate", 0),
                posts_unprocessed=post_stats.get("unprocessed", 0),
                orders_executed=order_stats.get("total_orders", 0),
                orders_success_rate=order_stats.get("success_rate", 0),
                active_orders=order_stats.get("active_orders", 0),
                twiboost_balance=f"{balances['twiboost'].get('balance', 0)} {balances['twiboost'].get('currency', 'USD')}",
                nakrutochka_balance=f"{balances['nakrutochka'].get('balance', 0)} {balances['nakrutochka'].get('currency', 'RUB')}",
                api_distribution=api_stats,
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

    def get_task_statistics(self) -> Dict[str, Any]:
        """Get detailed task statistics"""
        total_runs = sum(stats["runs"] for stats in self.task_stats.values())
        total_errors = sum(stats["errors"] for stats in self.task_stats.values())

        return {
            "total_runs": total_runs,
            "total_errors": total_errors,
            "error_rate": (total_errors / total_runs * 100) if total_runs > 0 else 0,
            "by_task": self.task_stats,
            "scheduler_running": self.scheduler.running,
            "jobs_count": len(self.scheduler.get_jobs())
        }


# Global scheduler instance
task_scheduler = TaskScheduler()