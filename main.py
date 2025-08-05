#!/usr/bin/env python3
"""
Telegram SMM Bot - Main entry point
"""
import asyncio
import signal
import sys
from typing import Optional

from src.config import settings
from src.database.connection import db, test_connection
from src.utils.logger import get_logger, DatabaseLogger
from src.services.telegram import TelegramMonitor
from src.services.processor import PostProcessor
from src.services.nakrutka import NakrutkaClient
from src.utils.scheduler import BotScheduler

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
        self.running = False

    async def verify_database(self):
        """Verify database tables and connection"""
        logger.info("Verifying database structure...")

        required_tables = [
            'channels', 'channel_settings', 'posts', 'orders',
            'order_portions', 'services', 'portion_templates',
            'channel_reaction_services', 'api_keys', 'logs'
        ]

        try:
            for table in required_tables:
                count = await self.db.fetchval(f"SELECT COUNT(*) FROM {table}")
                logger.info(f"Table '{table}' exists, records: {count}")

            # Check active channels
            active_channels = await self.db.fetchval(
                "SELECT COUNT(*) FROM channels WHERE is_active = true"
            )
            logger.info(f"Active channels: {active_channels}")

            if active_channels == 0:
                logger.warning("No active channels found!")

            return True

        except Exception as e:
            logger.error(f"Database verification failed: {str(e)}")
            return False

    async def verify_nakrutka(self):
        """Verify Nakrutka API connection"""
        logger.info("Verifying Nakrutka API...")

        try:
            balance = await self.nakrutka_client.get_balance()
            logger.info(f"Nakrutka API connected, balance: {balance.get('balance')} {balance.get('currency')}")

            # Check if API key is valid
            if 'error' in balance:
                logger.error(f"Nakrutka API error: {balance['error']}")
                return False

            return True

        except Exception as e:
            logger.error(f"Nakrutka API verification failed: {str(e)}")
            return False

    async def verify_telegram(self):
        """Verify Telegram Bot API"""
        logger.info("Verifying Telegram Bot API...")

        try:
            bot_info = await self.telegram_monitor.bot.get_me()
            logger.info(f"Telegram bot connected: @{bot_info.username}")
            return True

        except Exception as e:
            logger.error(f"Telegram API verification failed: {str(e)}")
            return False

    async def setup(self):
        """Initialize all components"""
        logger.info("Starting Telegram SMM Bot setup...")
        logger.info(f"Environment: {settings.environment}")
        logger.info(f"Log level: {settings.log_level}")
        logger.info(f"Check interval: {settings.check_interval}s")

        try:
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
            await self.db_logger.info("Bot starting", environment=settings.environment)
            logger.info("Database logger initialized")

            # Initialize services
            logger.info("Initializing Nakrutka client...")
            self.nakrutka_client = NakrutkaClient()
            logger.info("Nakrutka client initialized")

            # Verify Nakrutka API
            if not await self.verify_nakrutka():
                logger.error("Nakrutka API not working properly!")
                # Continue anyway, will fail on actual orders

            logger.info("Initializing Telegram monitor...")
            self.telegram_monitor = TelegramMonitor(self.db)
            logger.info("Telegram monitor initialized")

            # Verify Telegram Bot API
            if not await self.verify_telegram():
                logger.error("Telegram Bot API not working properly!")
                # Continue anyway

            logger.info("Initializing post processor...")
            self.post_processor = PostProcessor(self.db, self.nakrutka_client)
            logger.info("Post processor initialized")

            # Initialize scheduler
            logger.info("Initializing scheduler...")
            self.scheduler = BotScheduler()
            logger.info("Scheduler initialized")

            # Schedule tasks
            logger.info("Scheduling tasks...")

            self.scheduler.add_job(
                self.telegram_monitor.check_channels,
                interval_seconds=settings.check_interval,
                job_id="channel_monitor"
            )
            logger.info(f"Channel monitor scheduled (interval: {settings.check_interval}s)")

            self.scheduler.add_job(
                self.post_processor.process_new_posts,
                interval_seconds=10,
                job_id="post_processor"
            )
            logger.info("Post processor scheduled (interval: 10s)")

            self.scheduler.add_job(
                self.post_processor.check_order_status,
                interval_seconds=60,
                job_id="status_checker"
            )
            logger.info("Status checker scheduled (interval: 60s)")

            logger.info("Bot setup completed successfully")
            logger.info("=" * 50)

        except Exception as e:
            logger.error(f"Setup failed: {str(e)}", exc_info=True)
            raise

    async def start(self):
        """Start the bot"""
        self.running = True
        logger.info("Starting bot services...")

        try:
            # Start scheduler
            logger.info("Starting scheduler...")
            self.scheduler.start()
            logger.info("Scheduler started successfully")

            # Start telegram bot if needed
            if hasattr(self.telegram_monitor, 'start_bot'):
                logger.info("Starting Telegram bot...")
                await self.telegram_monitor.start_bot()
                logger.info("Telegram bot started successfully")

            await self.db_logger.info("Bot started successfully")
            logger.info("Bot is running. Press Ctrl+C to stop.")
            logger.info("Entering main loop...")

            # Keep running
            loop_counter = 0
            while self.running:
                await asyncio.sleep(1)
                loop_counter += 1

                # Log heartbeat every minute
                if loop_counter % 60 == 0:
                    logger.debug(f"Bot heartbeat - running for {loop_counter} seconds")

                    # Log active jobs
                    jobs = self.scheduler.get_jobs()
                    logger.debug(f"Active scheduled jobs: {len(jobs)}")

        except Exception as e:
            logger.error(f"Error in start: {str(e)}", exc_info=True)
            raise

    async def stop(self):
        """Stop the bot"""
        logger.info("Stopping bot...")
        self.running = False

        try:
            # Stop scheduler
            if self.scheduler:
                logger.info("Stopping scheduler...")
                self.scheduler.stop()
                logger.info("Scheduler stopped")

            # Log shutdown
            if self.db_logger:
                logger.info("Logging shutdown to database...")
                await self.db_logger.info("Bot stopping")

            # Close database
            logger.info("Closing database connection...")
            await self.db.close()
            logger.info("Database connection closed")

            logger.info("Bot stopped successfully")

        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}", exc_info=True)

    def handle_signal(self, sig, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {sig}")
        asyncio.create_task(self.stop())


async def main():
    """Main entry point"""
    logger.info("=== TELEGRAM SMM BOT STARTING ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Platform: {sys.platform}")

    # Log environment variables (without sensitive data)
    logger.info(f"DATABASE_URL configured: {'Yes' if settings.database_url else 'No'}")
    logger.info(f"NAKRUTKA_API_KEY configured: {'Yes' if settings.nakrutka_api_key else 'No'}")
    logger.info(f"TELEGRAM_BOT_TOKEN configured: {'Yes' if settings.telegram_bot_token else 'No'}")

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
                await bot.db_logger.error("Bot crashed", error=str(e))
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
    asyncio.run(main())