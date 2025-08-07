"""
Telegram SMM Bot - Main entry point
Automated promotion service for Telegram channels
"""
import asyncio
import signal
import sys
from typing import Optional
from telegram.ext import Application

from src.config import settings
from src.utils.logger import setup_logging, get_logger
from src.database.connection import db, test_connection
from src.bot.telegram_monitor import TelegramMonitor, telegram_monitor
from src.bot.handlers import BotHandlers
from src.scheduler.tasks import task_scheduler
from src.services.twiboost_client import twiboost_client
from src.database.repositories.channel_repo import channel_repo


# Setup logging
setup_logging()
logger = get_logger(__name__)


class TelegramSMMBot:
    """Main bot application"""

    def __init__(self):
        self.app: Optional[Application] = None
        self.monitor: Optional[TelegramMonitor] = None
        self.handlers: Optional[BotHandlers] = None
        self.running = False

    async def initialize(self):
        """Initialize all components"""
        logger.info("🚀 Initializing Telegram SMM Bot...")

        try:
            # 1. Test database connection
            logger.info("Testing database connection...")
            await db.init()

            if not await test_connection():
                raise Exception("Database connection failed")

            logger.info("✅ Database connected")

            # 2. Initialize Twiboost client
            logger.info("Initializing Twiboost client...")
            await twiboost_client.init()

            # Test API connection
            balance = await twiboost_client.get_balance()
            logger.info(f"✅ Twiboost connected. Balance: {balance['balance']} {balance['currency']}")

            # 3. Sync Twiboost services
            logger.info("Syncing Twiboost services...")
            services = await twiboost_client.get_services()
            logger.info(f"✅ Found {len(services)} Twiboost services")

            # 4. Initialize Telegram bot
            logger.info("Initializing Telegram bot...")
            self.app = Application.builder().token(settings.telegram_bot_token).build()

            # 5. Setup handlers
            self.handlers = BotHandlers(self.app)
            logger.info("✅ Bot handlers configured")

            # 6. Initialize monitor
            self.monitor = TelegramMonitor(settings.telegram_bot_token)

            # Set global instance
            import src.bot.telegram_monitor
            src.bot.telegram_monitor.telegram_monitor = self.monitor

            logger.info("✅ Telegram monitor initialized")

            # 7. Validate existing channels
            logger.info("Validating existing channels...")
            await self.monitor.validate_all_channels()

            # 8. Start scheduler
            logger.info("Starting task scheduler...")
            await task_scheduler.start()
            logger.info("✅ Scheduler started")

            logger.info("✨ Bot initialization complete!")

        except Exception as e:
            logger.error(f"❌ Initialization failed: {e}")
            raise

    async def start(self):
        """Start the bot"""
        try:
            await self.initialize()

            self.running = True
            logger.info("🤖 Bot is starting...")

            # Start bot polling
            await self.app.initialize()
            await self.app.start()
            await self.app.updater.start_polling(drop_pending_updates=True)

            logger.info("✅ Bot polling started")

            # Start channel monitoring
            monitor_task = asyncio.create_task(self.monitor.start_monitoring())

            logger.info("✅ Channel monitoring started")
            logger.info("🎉 Bot is fully operational!")

            # Show admin info
            if settings.admin_telegram_id:
                logger.info(f"📱 Admin Telegram ID: {settings.admin_telegram_id}")
                logger.info("Send /start to the bot to begin")

            # Keep running
            while self.running:
                await asyncio.sleep(1)

            # Cleanup on exit
            logger.info("Shutting down...")
            monitor_task.cancel()
            await self.shutdown()

        except Exception as e:
            logger.error(f"Bot error: {e}")
            await self.shutdown()
            raise

    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("🛑 Shutting down bot...")

        self.running = False

        try:
            # Stop monitoring
            if self.monitor:
                await self.monitor.stop_monitoring()

            # Stop scheduler
            await task_scheduler.stop()

            # Stop bot
            if self.app:
                await self.app.updater.stop()
                await self.app.stop()
                await self.app.shutdown()

            # Close Twiboost client
            await twiboost_client.close()

            # Close database
            await db.close()

            logger.info("✅ Shutdown complete")

        except Exception as e:
            logger.error(f"Shutdown error: {e}")

    def handle_signal(self, signum, frame):
        """Handle system signals"""
        logger.info(f"Received signal {signum}")
        self.running = False


async def run_migrations():
    """Run database migrations"""
    logger.info("Running database migrations...")

    try:
        # Read migration file
        with open("migrations/001_initial_schema.sql", "r") as f:
            schema = f.read()

        # Execute migration
        await db.execute(schema)

        logger.info("✅ Migrations completed")

    except FileNotFoundError:
        logger.warning("Migration file not found, skipping migrations")
    except Exception as e:
        logger.error(f"Migration error: {e}")
        # Don't fail if tables already exist
        if "already exists" not in str(e):
            raise


async def main():
    """Main entry point"""
    logger.info("=" * 50)
    logger.info("TELEGRAM SMM BOT")
    logger.info("Automated Promotion Service")
    logger.info("=" * 50)

    # Check environment
    logger.info(f"Environment: {settings.environment}")
    logger.info(f"Check interval: {settings.check_interval}s")
    logger.info(f"Process interval: {settings.process_interval}s")

    try:
        # Initialize database connection first
        await db.init()

        # Run migrations
        await run_migrations()

        # Create and start bot
        bot = TelegramSMMBot()

        # Setup signal handlers
        signal.signal(signal.SIGINT, bot.handle_signal)
        signal.signal(signal.SIGTERM, bot.handle_signal)

        # Start bot
        await bot.start()

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Run the bot
    asyncio.run(main())