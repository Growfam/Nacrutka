#!/usr/bin/env python3
"""
Telegram SMM Bot - Main entry point
Complete implementation with all features
"""
import asyncio
import signal
import sys
import os
from typing import Optional
from datetime import datetime

from src.config import settings
from src.database.connection import db, test_connection
from src.utils.logger import get_logger, DatabaseLogger
from src.services.telegram import TelegramMonitor
from src.services.processor import PostProcessor
from src.services.nakrutka import NakrutkaClient
from src.utils.scheduler import BotScheduler
from src.utils.helpers import PerformanceMonitor

logger = get_logger(__name__)


class TelegramSMMBot:
    """Main bot application"""

    def __init__(self):
        self.db = db
        self.db_logger: Optional[DatabaseLogger] = None
        self.telegram_monitor: Optional[TelegramMonitor] = None
        self.post_processor: Optional[PostProcessor] = None
        self.nakrutka_client: Optional[NakrutkaClient] = None
        self.scheduler: Optional[BotScheduler] = None
        self.performance_monitor = PerformanceMonitor()
        self.running = False
        self.start_time = datetime.utcnow()

    async def verify_environment(self):
        """Verify environment variables and configuration"""
        logger.info("Verifying environment configuration...")

        required_vars = [
            'DATABASE_URL',
            'NAKRUTKA_API_KEY',
            'TELEGRAM_BOT_TOKEN'
        ]

        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)

        if missing_vars:
            logger.error(f"Missing required environment variables: {missing_vars}")
            raise Exception(f"Missing environment variables: {', '.join(missing_vars)}")

        logger.info("Environment configuration verified")
        return True

    async def verify_database(self):
        """Verify database tables and connection"""
        logger.info("Verifying database structure...")

        required_tables = [
            'channels', 'channel_settings', 'posts', 'orders',
            'order_portions', 'services', 'portion_templates',
            'channel_reaction_services', 'api_keys', 'logs'
        ]

        try:
            # Check each table
            missing_tables = []
            for table in required_tables:
                try:
                    count = await self.db.fetchval(f"SELECT COUNT(*) FROM {table}")
                    logger.info(f"Table '{table}' exists, records: {count}")
                except Exception:
                    missing_tables.append(table)

            if missing_tables:
                logger.error(f"Missing tables: {missing_tables}")
                return False

            # Check active channels
            active_channels = await self.db.fetchval(
                "SELECT COUNT(*) FROM channels WHERE is_active = true"
            )
            logger.info(f"Active channels: {active_channels}")

            if active_channels == 0:
                logger.warning("No active channels found! Add channels to database.")

            # Check services
            services_count = await self.db.fetchval(
                "SELECT COUNT(*) FROM services WHERE is_active = true"
            )
            logger.info(f"Active services: {services_count}")

            if services_count == 0:
                logger.error("No active services found in database!")
                return False

            # Check API key
            api_key_exists = await self.db.fetchval(
                "SELECT COUNT(*) FROM api_keys WHERE service_name = 'nakrutka' AND is_active = true"
            )

            if not api_key_exists:
                logger.warning("No Nakrutka API key in database, using from environment")

            return True

        except Exception as e:
            logger.error(f"Database verification failed: {str(e)}")
            return False

    async def verify_nakrutka(self):
        """Verify Nakrutka API connection"""
        logger.info("Verifying Nakrutka API...")

        try:
            balance = await self.nakrutka_client.get_balance()

            # Check if response is valid
            if not isinstance(balance, dict):
                logger.error(f"Invalid balance response: {balance}")
                return False

            if 'balance' not in balance:
                logger.error(f"No balance in response: {balance}")
                return False

            balance_amount = float(balance.get('balance', 0))
            currency = balance.get('currency', 'USD')

            logger.info(
                f"Nakrutka API connected successfully",
                balance=balance_amount,
                currency=currency
            )

            # Check minimum balance
            if balance_amount < 1:
                logger.warning(
                    f"Very low Nakrutka balance: {balance_amount} {currency}"
                )
                await self.db_logger.warning(
                    "Low Nakrutka balance",
                    balance=balance_amount,
                    currency=currency
                )

            # Test service availability
            try:
                services = await self.nakrutka_client.get_services()
                logger.info(f"Nakrutka has {len(services)} services available")
            except Exception as e:
                logger.warning(f"Could not fetch services: {e}")

            return True

        except Exception as e:
            logger.error(f"Nakrutka API verification failed: {str(e)}")
            return False

    async def verify_telegram(self):
        """Verify Telegram Bot API"""
        logger.info("Verifying Telegram Bot API...")

        try:
            bot_info = await self.telegram_monitor.bot.get_me()
            logger.info(
                f"Telegram bot connected",
                username=bot_info.username,
                bot_id=bot_info.id,
                can_read_all_group_messages=bot_info.can_read_all_group_messages
            )

            return True

        except Exception as e:
            logger.error(f"Telegram API verification failed: {str(e)}")
            return False

    async def setup(self):
        """Initialize all components"""
        logger.info("=" * 50)
        logger.info("Starting Telegram SMM Bot setup...")
        logger.info(f"Environment: {settings.environment}")
        logger.info(f"Log level: {settings.log_level}")
        logger.info(f"Check interval: {settings.check_interval}s")
        logger.info(f"Timezone: {settings.timezone}")

        try:
            # Verify environment
            await self.verify_environment()

            # Initialize database
            logger.info("Initializing database connection...")
            await self.db.init()
            logger.info("Database pool created successfully")

            # Test connection
            logger.info("Testing database connection...")
            if not await test_connection():
                logger.error("Database connection test failed")
                raise Exception("Database connection failed")
            logger.info("Database connection test passed")

            # Verify database structure
            if not await self.verify_database():
                raise Exception("Database structure verification failed")

            # Initialize database logger
            logger.info("Initializing database logger...")
            self.db_logger = DatabaseLogger(self.db)
            await self.db_logger.info(
                "Bot starting",
                environment=settings.environment,
                version="1.0.0"
            )
            logger.info("Database logger initialized")

            # Initialize services
            logger.info("Initializing Nakrutka client...")
            self.nakrutka_client = NakrutkaClient()
            logger.info("Nakrutka client initialized")

            # Verify Nakrutka API
            nakrutka_ok = await self.verify_nakrutka()
            if not nakrutka_ok:
                logger.error("Nakrutka API not working properly!")
                await self.db_logger.warning(
                    "Nakrutka API verification failed",
                    message="Bot will continue but orders may fail"
                )

            logger.info("Initializing Telegram monitor...")
            self.telegram_monitor = TelegramMonitor(self.db)
            self.telegram_monitor.nakrutka_client = self.nakrutka_client  # Pass reference
            await self.telegram_monitor.setup_bot()
            logger.info("Telegram monitor initialized")

            # Verify Telegram Bot API
            telegram_ok = await self.verify_telegram()
            if not telegram_ok:
                logger.error("Telegram Bot API not working properly!")
                await self.db_logger.warning(
                    "Telegram Bot API verification failed",
                    message="Bot commands may not work"
                )

            logger.info("Initializing post processor...")
            self.post_processor = PostProcessor(self.db, self.nakrutka_client)
            logger.info("Post processor initialized")

            # Initialize scheduler
            logger.info("Initializing scheduler...")
            self.scheduler = BotScheduler()
            logger.info("Scheduler initialized")

            # Schedule tasks
            await self._schedule_tasks()

            logger.info("Bot setup completed successfully")
            logger.info("=" * 50)

            # Log summary
            await self.db_logger.info(
                "Bot setup completed",
                nakrutka_ok=nakrutka_ok,
                telegram_ok=telegram_ok,
                environment=settings.environment
            )

        except Exception as e:
            logger.error(f"Setup failed: {str(e)}", exc_info=True)
            if self.db_logger:
                await self.db_logger.error(
                    "Bot setup failed",
                    error=str(e)
                )
            raise

    async def _schedule_tasks(self):
        """Schedule all periodic tasks"""
        logger.info("Scheduling periodic tasks...")

        # Channel monitoring - main task
        self.scheduler.add_job(
            self._monitor_channels_with_metrics,
            interval_seconds=settings.check_interval,
            job_id="channel_monitor"
        )
        logger.info(f"Channel monitor scheduled (interval: {settings.check_interval}s)")

        # Post processing - frequent
        self.scheduler.add_job(
            self._process_posts_with_metrics,
            interval_seconds=10,
            job_id="post_processor"
        )
        logger.info("Post processor scheduled (interval: 10s)")

        # Status checking - less frequent
        self.scheduler.add_job(
            self._check_status_with_metrics,
            interval_seconds=60,
            job_id="status_checker"
        )
        logger.info("Status checker scheduled (interval: 60s)")

        # Cleanup old logs - daily
        self.scheduler.add_job(
            self._cleanup_logs,
            interval_seconds=86400,  # 24 hours
            job_id="log_cleanup"
        )
        logger.info("Log cleanup scheduled (interval: 24h)")

        # Health check - every 5 minutes
        self.scheduler.add_job(
            self._health_check,
            interval_seconds=300,
            job_id="health_check"
        )
        logger.info("Health check scheduled (interval: 5m)")

    async def _monitor_channels_with_metrics(self):
        """Monitor channels with performance metrics"""
        await self.performance_monitor.measure(
            "channel_monitoring",
            self.telegram_monitor.check_channels
        )

    async def _process_posts_with_metrics(self):
        """Process posts with performance metrics"""
        await self.performance_monitor.measure(
            "post_processing",
            self.post_processor.process_new_posts
        )

    async def _check_status_with_metrics(self):
        """Check order status with performance metrics"""
        await self.performance_monitor.measure(
            "status_checking",
            self.post_processor.check_order_status
        )

    async def _cleanup_logs(self):
        """Clean up old log entries"""
        try:
            from src.database.queries import Queries
            queries = Queries(self.db)
            deleted = await queries.cleanup_old_logs(days=7)
            logger.info(f"Cleaned up {deleted} old log entries")
        except Exception as e:
            logger.error(f"Log cleanup failed: {e}")

    async def _health_check(self):
        """Perform health check"""
        try:
            # Check database
            db_ok = await test_connection()

            # Check Nakrutka
            try:
                balance = await self.nakrutka_client.get_balance()
                nakrutka_ok = 'balance' in balance
            except:
                nakrutka_ok = False

            # Get performance stats
            stats = {
                'uptime_hours': (datetime.utcnow() - self.start_time).total_seconds() / 3600,
                'database': 'OK' if db_ok else 'FAIL',
                'nakrutka': 'OK' if nakrutka_ok else 'FAIL',
                'performance': {
                    name: self.performance_monitor.get_stats(name)
                    for name in ['channel_monitoring', 'post_processing', 'status_checking']
                }
            }

            logger.info(
                "Health check completed",
                uptime_hours=f"{stats['uptime_hours']:.1f}",
                database=stats['database'],
                nakrutka=stats['nakrutka']
            )

            # Log to database
            await self.db_logger.info("Health check", **stats)

        except Exception as e:
            logger.error(f"Health check failed: {e}")

    async def start(self):
        """Start the bot"""
        self.running = True
        logger.info("Starting bot services...")

        try:
            # Start scheduler
            logger.info("Starting scheduler...")
            self.scheduler.start()
            logger.info("Scheduler started successfully")

            # Start telegram bot
            logger.info("Starting Telegram bot...")
            await self.telegram_monitor.start_bot()
            logger.info("Telegram bot started successfully")

            await self.db_logger.info(
                "Bot started successfully",
                scheduled_jobs=len(self.scheduler.get_jobs())
            )

            logger.info("Bot is running. Press Ctrl+C to stop.")
            logger.info("Entering main loop...")

            # Main loop
            loop_counter = 0
            last_stats_log = datetime.utcnow()

            while self.running:
                await asyncio.sleep(1)
                loop_counter += 1

                # Log heartbeat every minute
                if loop_counter % 60 == 0:
                    logger.debug(
                        f"Bot heartbeat",
                        uptime_seconds=loop_counter,
                        active_jobs=len(self.scheduler.get_jobs())
                    )

                # Log detailed stats every hour
                if (datetime.utcnow() - last_stats_log).total_seconds() >= 3600:
                    await self._log_hourly_stats()
                    last_stats_log = datetime.utcnow()

        except Exception as e:
            logger.error(f"Error in start: {str(e)}", exc_info=True)
            raise

    async def _log_hourly_stats(self):
        """Log hourly statistics"""
        try:
            from src.database.queries import Queries
            queries = Queries(self.db)

            # Get monitoring stats
            stats = await queries.get_monitoring_stats()

            logger.info(
                "Hourly statistics",
                active_channels=stats.get('active_channels', 0),
                posts_24h=stats.get('posts_24h', 0),
                orders_24h=stats.get('orders_24h', 0),
                cost_today=f"${stats.get('cost_today', 0):.2f}"
            )

            # Performance metrics
            for metric_name in ['channel_monitoring', 'post_processing', 'status_checking']:
                metric_stats = self.performance_monitor.get_stats(metric_name)
                if metric_stats:
                    logger.info(
                        f"Performance: {metric_name}",
                        calls=metric_stats['count'],
                        success_rate=f"{metric_stats['success_rate'] * 100:.1f}%",
                        avg_duration=f"{metric_stats['avg_duration']:.2f}s"
                    )

        except Exception as e:
            logger.error(f"Failed to log hourly stats: {e}")

    async def stop(self):
        """Stop the bot gracefully"""
        logger.info("Stopping bot...")
        self.running = False

        try:
            # Stop scheduler first
            if self.scheduler:
                logger.info("Stopping scheduler...")
                self.scheduler.stop()
                logger.info("Scheduler stopped")

            # Stop telegram bot
            if self.telegram_monitor:
                logger.info("Stopping Telegram bot...")
                await self.telegram_monitor.stop_bot()
                logger.info("Telegram bot stopped")

            # Log final stats
            uptime = datetime.utcnow() - self.start_time
            logger.info(
                "Bot shutting down",
                uptime_hours=f"{uptime.total_seconds() / 3600:.1f}",
                environment=settings.environment
            )

            # Log shutdown to database
            if self.db_logger:
                logger.info("Logging shutdown to database...")
                await self.db_logger.info(
                    "Bot stopping",
                    uptime_hours=uptime.total_seconds() / 3600,
                    reason="User requested"
                )

            # Close Nakrutka session
            if self.nakrutka_client and self.nakrutka_client.session:
                logger.info("Closing Nakrutka session...")
                await self.nakrutka_client.session.close()

            # Close database last
            logger.info("Closing database connection...")
            await self.db.close()
            logger.info("Database connection closed")

            logger.info("Bot stopped successfully")

        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}", exc_info=True)

    def handle_signal(self, sig, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {sig}")
        # Create task to stop bot
        asyncio.create_task(self.stop())


async def main():
    """Main entry point"""
    logger.info("=== TELEGRAM SMM BOT STARTING ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Platform: {sys.platform}")
    logger.info(f"Process ID: {os.getpid()}")

    # Log environment info (without sensitive data)
    logger.info(f"DATABASE_URL configured: {'Yes' if settings.database_url else 'No'}")
    logger.info(f"NAKRUTKA_API_KEY configured: {'Yes' if settings.nakrutka_api_key else 'No'}")
    logger.info(f"TELEGRAM_BOT_TOKEN configured: {'Yes' if settings.telegram_bot_token else 'No'}")
    logger.info(f"Environment: {settings.environment}")

    bot = TelegramSMMBot()

    # Setup signal handlers
    signal.signal(signal.SIGINT, bot.handle_signal)
    signal.signal(signal.SIGTERM, bot.handle_signal)
    logger.info("Signal handlers configured")

    try:
        # Setup bot
        logger.info("Starting bot setup...")
        await bot.setup()

        # Start bot
        logger.info("Starting bot main loop...")
        await bot.start()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Bot crashed: {str(e)}", error=str(e), exc_info=True)
        if bot.db_logger:
            try:
                await bot.db_logger.error(
                    "Bot crashed",
                    error=str(e),
                    error_type=type(e).__name__
                )
            except:
                logger.error("Failed to log crash to database")
    finally:
        logger.info("Shutting down...")
        await bot.stop()
        logger.info("=== TELEGRAM SMM BOT STOPPED ===")


if __name__ == "__main__":
    logger.info("Script started directly")

    # Windows event loop policy fix
    if sys.platform == "win32":
        logger.info("Windows detected, setting event loop policy")
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Run bot
    logger.info("Starting asyncio event loop...")

    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Failed to run bot: {e}")
        sys.exit(1)