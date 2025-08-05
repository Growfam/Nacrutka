#!/usr/bin/env python3
"""
Telegram SMM Bot - Main entry point with database initialization
"""
import asyncio
import signal
import sys
import os
from typing import Optional
from datetime import datetime

from src.config import settings
from src.database.connection import db, test_connection
from src.database.migrations import initialize_database, DatabaseMigrations
from src.database.initialize import DatabaseInitializer
from src.utils.logger import get_logger, DatabaseLogger
from src.services.telegram import TelegramMonitor
from src.services.processor import PostProcessor
from src.services.nakrutka import NakrutkaClient
from src.utils.scheduler import BotScheduler
from src.utils.helpers import PerformanceMonitor
from src.utils.validators import ConfigValidator

logger = get_logger(__name__)


class TelegramSMMBot:
    """Main bot application with enhanced initialization"""

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

        # Track initialization status
        self.init_status = {
            'environment': False,
            'database': False,
            'database_schema': False,
            'services': False,
            'nakrutka': False,
            'telegram': False
        }

    async def verify_environment(self):
        """Verify environment variables and configuration"""
        logger.info("=" * 50)
        logger.info("Verifying environment configuration...")

        # Use config validator
        errors = ConfigValidator.validate_environment()

        if errors:
            logger.error("Environment validation failed", errors=errors)
            for error in errors:
                logger.error(f"  - {error}")
            raise Exception(f"Invalid environment: {'; '.join(errors)}")

        logger.info("✅ Environment configuration verified")
        self.init_status['environment'] = True
        return True

    async def initialize_database(self):
        """Initialize database with migrations and default data"""
        logger.info("=" * 50)
        logger.info("Initializing database...")

        try:
            # Initialize connection pool
            logger.info("Creating database connection pool...")
            await self.db.init()
            logger.info("✅ Database pool created")

            # Test connection
            logger.info("Testing database connection...")
            if not await test_connection():
                raise Exception("Database connection test failed")
            logger.info("✅ Database connection established")

            self.init_status['database'] = True

            # Run migrations
            logger.info("Checking database schema...")
            integrity = await initialize_database(self.db)

            # Validate schema
            schema_errors = ConfigValidator.validate_database_schema(integrity)
            if schema_errors:
                logger.error("Database schema validation failed", errors=schema_errors)
                for error in schema_errors:
                    logger.error(f"  - {error}")
                raise Exception("Invalid database schema")

            logger.info(
                "✅ Database schema verified",
                tables=len(integrity.get('tables', {})),
                functions=len(integrity.get('functions', {})),
                views=len(integrity.get('views', {}))
            )
            self.init_status['database_schema'] = True

            # Initialize default data
            logger.info("Initializing default data...")
            initializer = DatabaseInitializer(self.db)
            await initializer.initialize_all()

            # Verify initialization
            verify_result = await initializer.verify_initialization()
            if verify_result['errors']:
                logger.warning(
                    "Database initialization warnings",
                    warnings=verify_result['errors']
                )

            logger.info(
                "✅ Database initialized",
                services=verify_result['services'],
                channels=verify_result['channels'],
                api_keys=verify_result['api_keys']
            )
            self.init_status['services'] = True

            return True

        except Exception as e:
            logger.error("Database initialization failed", error=str(e), exc_info=True)
            raise

    async def verify_nakrutka(self):
        """Verify Nakrutka API connection and configuration"""
        logger.info("=" * 50)
        logger.info("Verifying Nakrutka API...")

        try:
            # Perform health check
            health = await self.nakrutka_client.health_check()

            if health['status'] != 'healthy':
                logger.error(
                    "Nakrutka health check failed",
                    status=health['status'],
                    errors=health.get('errors', [])
                )

                # Don't fail completely, just warn
                if health['status'] == 'auth_error':
                    logger.error("❌ Nakrutka authentication failed - check API key")
                    await self.db_logger.error(
                        "Nakrutka auth failed",
                        message="Invalid API key or authentication error"
                    )
                elif health['status'] == 'connection_error':
                    logger.error("❌ Cannot connect to Nakrutka API")
                    await self.db_logger.error(
                        "Nakrutka connection failed",
                        message="Cannot reach Nakrutka API servers"
                    )

                self.init_status['nakrutka'] = False
                return False

            # Log success
            logger.info(
                "✅ Nakrutka API connected",
                balance=f"${health.get('balance', 0):.2f}",
                currency=health.get('currency', 'USD'),
                services=health.get('service_count', 0)
            )

            # Check minimum balance
            balance = float(health.get('balance', 0))
            if balance < 1:
                logger.error(
                    "❌ Nakrutka balance critically low",
                    balance=balance,
                    currency=health.get('currency', 'USD')
                )
                await self.db_logger.error(
                    "Critical: Low Nakrutka balance",
                    balance=balance,
                    message="Bot may not be able to create orders"
                )
            elif balance < 10:
                logger.warning(
                    "⚠️ Low Nakrutka balance",
                    balance=balance,
                    currency=health.get('currency', 'USD')
                )
                await self.db_logger.warning(
                    "Low Nakrutka balance",
                    balance=balance,
                    message="Consider adding funds soon"
                )

            self.init_status['nakrutka'] = True
            return True

        except Exception as e:
            logger.error("Nakrutka verification failed", error=str(e))
            self.init_status['nakrutka'] = False
            return False

    async def verify_telegram(self):
        """Verify Telegram Bot API"""
        logger.info("=" * 50)
        logger.info("Verifying Telegram Bot API...")

        try:
            bot_info = await self.telegram_monitor.bot.get_me()
            logger.info(
                "✅ Telegram bot verified",
                username=bot_info.username,
                bot_id=bot_info.id,
                can_read_all_group_messages=bot_info.can_read_all_group_messages
            )

            if not bot_info.can_read_all_group_messages:
                logger.warning(
                    "⚠️ Bot cannot read all group messages",
                    hint="Bot may not see channel posts if not admin"
                )

            self.init_status['telegram'] = True
            return True

        except Exception as e:
            logger.error("Telegram API verification failed", error=str(e))
            self.init_status['telegram'] = False
            return False

    async def setup(self):
        """Initialize all components with enhanced error handling"""
        logger.info("=" * 70)
        logger.info("🚀 TELEGRAM SMM BOT STARTING")
        logger.info("=" * 70)
        logger.info(f"Environment: {settings.environment}")
        logger.info(f"Log level: {settings.log_level}")
        logger.info(f"Check interval: {settings.check_interval}s")
        logger.info(f"Timezone: {settings.timezone}")

        try:
            # 1. Verify environment
            await self.verify_environment()

            # 2. Initialize and verify database
            await self.initialize_database()

            # 3. Initialize database logger
            logger.info("Initializing database logger...")
            self.db_logger = DatabaseLogger(self.db)
            await self.db_logger.info(
                "Bot starting",
                environment=settings.environment,
                version="1.0.0",
                python_version=sys.version
            )
            logger.info("✅ Database logger initialized")

            # 4. Initialize Nakrutka client
            logger.info("Initializing Nakrutka client...")
            self.nakrutka_client = NakrutkaClient()
            logger.info("✅ Nakrutka client initialized")

            # 5. Verify Nakrutka API
            nakrutka_ok = await self.verify_nakrutka()
            if not nakrutka_ok:
                logger.error("⚠️ Nakrutka API not working properly!")
                await self.db_logger.warning(
                    "Nakrutka API issues",
                    message="Bot will continue but orders may fail"
                )

            # 6. Initialize Telegram monitor with Nakrutka client
            logger.info("Initializing Telegram monitor...")
            self.telegram_monitor = TelegramMonitor(self.db, self.nakrutka_client)
            await self.telegram_monitor.setup_bot()
            logger.info("✅ Telegram monitor initialized")

            # 7. Verify Telegram Bot API
            telegram_ok = await self.verify_telegram()
            if not telegram_ok:
                logger.error("⚠️ Telegram Bot API not working properly!")
                await self.db_logger.warning(
                    "Telegram Bot API issues",
                    message="Bot commands may not work"
                )

            # 8. Initialize post processor
            logger.info("Initializing post processor...")
            self.post_processor = PostProcessor(self.db, self.nakrutka_client)
            logger.info("✅ Post processor initialized")

            # 9. Initialize scheduler
            logger.info("Initializing scheduler...")
            self.scheduler = BotScheduler()
            logger.info("✅ Scheduler initialized")

            # 10. Schedule tasks
            await self._schedule_tasks()

            # Log final status
            logger.info("=" * 70)
            logger.info("🎉 BOT SETUP COMPLETED")
            logger.info("=" * 70)

            # Summary of initialization
            success_count = sum(1 for v in self.init_status.values() if v)
            total_count = len(self.init_status)

            logger.info(
                "Initialization summary",
                successful=f"{success_count}/{total_count}",
                status=self.init_status
            )

            await self.db_logger.info(
                "Bot setup completed",
                initialization_status=self.init_status,
                nakrutka_ok=nakrutka_ok,
                telegram_ok=telegram_ok,
                environment=settings.environment
            )

            # Warn if critical components failed
            if not self.init_status['nakrutka']:
                logger.warning("⚠️ Running without Nakrutka - orders will fail!")
            if not self.init_status['telegram']:
                logger.warning("⚠️ Running without Telegram commands!")

        except Exception as e:
            logger.error("Setup failed", error=str(e), exc_info=True)
            if self.db_logger:
                await self.db_logger.error(
                    "Bot setup failed",
                    error=str(e),
                    error_type=type(e).__name__,
                    initialization_status=self.init_status
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
        logger.info(f"✅ Channel monitor scheduled (interval: {settings.check_interval}s)")

        # Post processing - frequent
        self.scheduler.add_job(
            self._process_posts_with_metrics,
            interval_seconds=10,
            job_id="post_processor"
        )
        logger.info("✅ Post processor scheduled (interval: 10s)")

        # Status checking - less frequent
        self.scheduler.add_job(
            self._check_status_with_metrics,
            interval_seconds=60,
            job_id="status_checker"
        )
        logger.info("✅ Status checker scheduled (interval: 60s)")

        # Cleanup old logs - daily
        self.scheduler.add_job(
            self._cleanup_logs,
            interval_seconds=86400,  # 24 hours
            job_id="log_cleanup"
        )
        logger.info("✅ Log cleanup scheduled (interval: 24h)")

        # Health check - every 5 minutes
        self.scheduler.add_job(
            self._health_check,
            interval_seconds=300,
            job_id="health_check"
        )
        logger.info("✅ Health check scheduled (interval: 5m)")

        # Balance check - every hour
        if self.init_status['nakrutka']:
            self.scheduler.add_job(
                self._check_balance,
                interval_seconds=3600,
                job_id="balance_check"
            )
            logger.info("✅ Balance check scheduled (interval: 1h)")

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

            # Also clean performance metrics
            self.performance_monitor.clear_metrics()

        except Exception as e:
            logger.error(f"Log cleanup failed: {e}")

    async def _health_check(self):
        """Perform comprehensive health check"""
        try:
            health_status = {
                'timestamp': datetime.utcnow().isoformat(),
                'uptime_hours': (datetime.utcnow() - self.start_time).total_seconds() / 3600,
                'components': {}
            }

            # Check database
            try:
                db_test = await test_connection()
                health_status['components']['database'] = 'OK' if db_test else 'FAIL'
            except:
                health_status['components']['database'] = 'FAIL'

            # Check Nakrutka
            if self.nakrutka_client and self.init_status['nakrutka']:
                try:
                    nakrutka_health = await self.nakrutka_client.health_check()
                    health_status['components']['nakrutka'] = nakrutka_health['status']
                    health_status['nakrutka_balance'] = nakrutka_health.get('balance', 0)
                except:
                    health_status['components']['nakrutka'] = 'ERROR'
            else:
                health_status['components']['nakrutka'] = 'DISABLED'

            # Check Telegram
            health_status['components']['telegram'] = (
                'OK' if self.telegram_monitor else 'DISABLED'
            )

            # Get performance stats
            health_status['performance'] = {
                name: self.performance_monitor.get_stats(name)
                for name in ['channel_monitoring', 'post_processing', 'status_checking']
            }

            # Get monitoring stats
            if self.telegram_monitor:
                health_status['monitoring'] = self.telegram_monitor.get_monitor_stats()

            # Log health status
            logger.info(
                "Health check completed",
                uptime_hours=f"{health_status['uptime_hours']:.1f}",
                components=health_status['components']
            )

            # Log to database
            await self.db_logger.info("Health check", **health_status)

            # Alert if issues found
            issues = [
                comp for comp, status in health_status['components'].items()
                if status not in ['OK', 'DISABLED']
            ]
            if issues:
                logger.warning(f"Health check found issues: {issues}")
                await self.db_logger.warning(
                    "Health check issues",
                    failed_components=issues,
                    details=health_status
                )

        except Exception as e:
            logger.error(f"Health check failed: {e}")

    async def _check_balance(self):
        """Check Nakrutka balance periodically"""
        if not self.nakrutka_client:
            return

        try:
            balance_info = await self.nakrutka_client.get_balance()
            balance = float(balance_info.get('balance', 0))
            currency = balance_info.get('currency', 'USD')

            logger.info(f"Balance check: ${balance:.2f} {currency}")

            # Alert on low balance
            if balance < 1:
                logger.error(f"CRITICAL: Balance too low: ${balance:.2f}")
                await self.db_logger.error(
                    "Critical low balance",
                    balance=balance,
                    currency=currency,
                    message="Bot cannot create new orders!"
                )
            elif balance < 10:
                logger.warning(f"Low balance warning: ${balance:.2f}")
                await self.db_logger.warning(
                    "Low balance warning",
                    balance=balance,
                    currency=currency,
                    message="Consider adding funds"
                )

        except Exception as e:
            logger.error(f"Balance check failed: {e}")

    async def start(self):
        """Start the bot"""
        self.running = True
        logger.info("Starting bot services...")

        try:
            # Start scheduler
            logger.info("Starting scheduler...")
            self.scheduler.start()
            logger.info("✅ Scheduler started")

            # Start telegram bot
            logger.info("Starting Telegram bot...")
            await self.telegram_monitor.start_bot()
            logger.info("✅ Telegram bot started")

            await self.db_logger.info(
                "Bot started successfully",
                scheduled_jobs=len(self.scheduler.get_jobs()),
                initialization_status=self.init_status
            )

            logger.info("=" * 70)
            logger.info("🟢 BOT IS RUNNING")
            logger.info("Press Ctrl+C to stop")
            logger.info("=" * 70)

            # Main loop
            loop_counter = 0
            last_stats_log = datetime.utcnow()

            while self.running:
                await asyncio.sleep(1)
                loop_counter += 1

                # Log heartbeat every minute
                if loop_counter % 60 == 0:
                    logger.debug(
                        "Bot heartbeat",
                        uptime_seconds=loop_counter,
                        active_jobs=len(self.scheduler.get_jobs())
                    )

                # Log detailed stats every hour
                if (datetime.utcnow() - last_stats_log).total_seconds() >= 3600:
                    await self._log_hourly_stats()
                    last_stats_log = datetime.utcnow()

        except Exception as e:
            logger.error("Error in main loop", error=str(e), exc_info=True)
            raise

    async def _log_hourly_stats(self):
        """Log hourly statistics"""
        try:
            from src.database.queries import Queries
            queries = Queries(self.db)

            # Get monitoring stats
            stats = await queries.get_monitoring_stats()

            logger.info(
                "📊 Hourly statistics",
                active_channels=stats.get('active_channels', 0),
                posts_24h=stats.get('posts_24h', 0),
                orders_24h=stats.get('orders_24h', 0),
                cost_today=f"${stats.get('cost_today', 0):.2f}"
            )

            # Performance metrics
            for metric_name in ['channel_monitoring', 'post_processing', 'status_checking']:
                metric_stats = self.performance_monitor.get_stats(metric_name)
                if metric_stats and metric_stats['count'] > 0:
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
        logger.info("🛑 Stopping bot...")
        self.running = False

        try:
            # Stop scheduler first
            if self.scheduler:
                logger.info("Stopping scheduler...")
                self.scheduler.stop()
                logger.info("✅ Scheduler stopped")

            # Stop telegram bot
            if self.telegram_monitor:
                logger.info("Stopping Telegram bot...")
                await self.telegram_monitor.stop_bot()
                logger.info("✅ Telegram bot stopped")

            # Log final stats
            uptime = datetime.utcnow() - self.start_time
            logger.info(
                "Bot shutting down",
                uptime_hours=f"{uptime.total_seconds() / 3600:.1f}",
                environment=settings.environment
            )

            # Log shutdown to database
            if self.db_logger:
                await self.db_logger.info(
                    "Bot stopping",
                    uptime_hours=uptime.total_seconds() / 3600,
                    reason="User requested",
                    final_status=self.init_status
                )

            # Close Nakrutka session
            if self.nakrutka_client:
                logger.info("Closing Nakrutka session...")
                await self.nakrutka_client.close()
                logger.info("✅ Nakrutka session closed")

            # Close database last
            logger.info("Closing database connection...")
            await self.db.close()
            logger.info("✅ Database connection closed")

            logger.info("=" * 70)
            logger.info("🔴 BOT STOPPED")
            logger.info("=" * 70)

        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}", exc_info=True)

    def handle_signal(self, sig, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {sig}")
        # Create task to stop bot
        asyncio.create_task(self.stop())


async def main():
    """Main entry point with enhanced initialization"""
    print("=" * 70)
    print("🚀 TELEGRAM SMM BOT")
    print("=" * 70)
    print(f"Python version: {sys.version}")
    print(f"Platform: {sys.platform}")
    print(f"Process ID: {os.getpid()}")
    print(f"Working directory: {os.getcwd()}")
    print("=" * 70)

    # Configure logging
    logger.info("Starting Telegram SMM Bot")
    logger.info(f"Python {sys.version}")
    logger.info(f"Platform: {sys.platform}")
    logger.info(f"Process ID: {os.getpid()}")

    # Log environment info (without sensitive data)
    env_info = {
        'DATABASE_URL': 'configured' if settings.database_url else 'missing',
        'NAKRUTKA_API_KEY': 'configured' if settings.nakrutka_api_key else 'missing',
        'TELEGRAM_BOT_TOKEN': 'configured' if settings.telegram_bot_token else 'missing',
        'ENVIRONMENT': settings.environment,
        'CHECK_INTERVAL': settings.check_interval,
        'ADMIN_ID': 'configured' if settings.admin_telegram_id else 'not set'
    }
    logger.info("Environment configuration", **env_info)

    bot = TelegramSMMBot()

    # Setup signal handlers
    for sig in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, bot.handle_signal)
    logger.info("Signal handlers configured")

    try:
        # Setup bot with all initialization
        logger.info("Initializing bot...")
        await bot.setup()

        # Start bot
        logger.info("Starting bot main loop...")
        await bot.start()

    except KeyboardInterrupt:
        logger.info("⌨️ Keyboard interrupt received")
    except Exception as e:
        logger.error(f"💥 Bot crashed: {str(e)}", error=str(e), exc_info=True)
        if bot.db_logger:
            try:
                await bot.db_logger.error(
                    "Bot crashed",
                    error=str(e),
                    error_type=type(e).__name__,
                    traceback=True
                )
            except:
                logger.error("Failed to log crash to database")
        print(f"\n❌ FATAL ERROR: {str(e)}")
        print("Check logs for details")
    finally:
        logger.info("Shutting down...")
        await bot.stop()
        print("\n👋 Bot stopped")


if __name__ == "__main__":
    print("Starting bot...")

    # Windows event loop policy fix
    if sys.platform == "win32":
        print("Windows detected, setting event loop policy")
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Run bot
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"\n❌ Failed to run bot: {e}")
        sys.exit(1)