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

    async def setup(self):
        """Initialize all components"""
        logger.info("Starting Telegram SMM Bot setup...")

        # Initialize database
        await self.db.init()

        # Test connection
        if not await test_connection():
            raise Exception("Database connection failed")

        # Initialize database logger
        self.db_logger = DatabaseLogger(self.db)
        await self.db_logger.info("Bot starting", environment=settings.environment)

        # Initialize services
        self.nakrutka_client = NakrutkaClient()
        self.telegram_monitor = TelegramMonitor(self.db)
        self.post_processor = PostProcessor(self.db, self.nakrutka_client)

        # Initialize scheduler
        self.scheduler = BotScheduler()

        # Schedule tasks
        self.scheduler.add_job(
            self.telegram_monitor.check_channels,
            interval_seconds=settings.check_interval,
            job_id="channel_monitor"
        )

        self.scheduler.add_job(
            self.post_processor.process_new_posts,
            interval_seconds=10,
            job_id="post_processor"
        )

        self.scheduler.add_job(
            self.post_processor.check_order_status,
            interval_seconds=60,
            job_id="status_checker"
        )

        logger.info("Bot setup completed")

    async def start(self):
        """Start the bot"""
        self.running = True
        logger.info("Starting bot services...")

        # Start scheduler
        self.scheduler.start()

        # Start telegram bot if needed
        if hasattr(self.telegram_monitor, 'start_bot'):
            await self.telegram_monitor.start_bot()

        await self.db_logger.info("Bot started successfully")
        logger.info("Bot is running. Press Ctrl+C to stop.")

        # Keep running
        while self.running:
            await asyncio.sleep(1)

    async def stop(self):
        """Stop the bot"""
        logger.info("Stopping bot...")
        self.running = False

        # Stop scheduler
        if self.scheduler:
            self.scheduler.stop()

        # Log shutdown
        if self.db_logger:
            await self.db_logger.info("Bot stopping")

        # Close database
        await self.db.close()

        logger.info("Bot stopped")

    def handle_signal(self, sig, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {sig}")
        asyncio.create_task(self.stop())


async def main():
    """Main entry point"""
    bot = TelegramSMMBot()

    # Setup signal handlers
    signal.signal(signal.SIGINT, bot.handle_signal)
    signal.signal(signal.SIGTERM, bot.handle_signal)

    try:
        # Setup bot
        await bot.setup()

        # Start bot
        await bot.start()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error("Bot crashed", error=str(e), exc_info=True)
        if bot.db_logger:
            await bot.db_logger.error("Bot crashed", error=str(e))
    finally:
        await bot.stop()


if __name__ == "__main__":
    # Windows event loop policy fix
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Run bot
    asyncio.run(main())