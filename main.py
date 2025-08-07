"""
Telegram SMM Bot - Main entry point
Automated promotion service for Telegram channels with dual API support
"""
import asyncio
import signal
import sys
from typing import Optional
from telegram.ext import Application

from src.config import settings
from src.utils.logger import setup_logging, get_logger
from src.database.connection import db, test_connection
from src.database.repositories.service_repo import service_repo
from src.bot.telegram_monitor import TelegramMonitor, telegram_monitor
from src.bot.handlers import BotHandlers
from src.scheduler.tasks import task_scheduler
from src.services.twiboost_client import twiboost_client
from src.services.nakrutochka_client import nakrutochka_client
from src.database.repositories.channel_repo import channel_repo


# Setup logging
setup_logging()
logger = get_logger(__name__)


class TelegramSMMBot:
    """Main bot application with dual API support"""

    def __init__(self):
        self.app: Optional[Application] = None
        self.monitor: Optional[TelegramMonitor] = None
        self.handlers: Optional[BotHandlers] = None
        self.running = False

    async def initialize(self):
        """Initialize all components"""
        logger.info("🚀 Initializing Telegram SMM Bot with Dual API Support...")

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

            # Test Twiboost API connection
            twiboost_balance = await twiboost_client.get_balance()
            logger.info(f"✅ Twiboost connected. Balance: {twiboost_balance['balance']} {twiboost_balance['currency']}")

            # 3. Initialize Nakrutochka client
            logger.info("Initializing Nakrutochka client...")
            await nakrutochka_client.init()

            # Test Nakrutochka API connection
            nakrutochka_balance = await nakrutochka_client.get_balance()
            logger.info(f"✅ Nakrutochka connected. Balance: {nakrutochka_balance['balance']} {nakrutochka_balance['currency']}")

            # 4. Sync services from BOTH APIs
            logger.info("🔄 Syncing services from both APIs...")

            # Sync Twiboost services (for views)
            logger.info("Syncing Twiboost services...")
            twiboost_services = await twiboost_client.get_services()
            twiboost_synced = await service_repo.sync_twiboost_services(twiboost_services)
            logger.info(f"✅ Synced {twiboost_synced} Twiboost services")

            # Sync Nakrutochka services (for reactions and reposts)
            logger.info("Syncing Nakrutochka services...")
            nakrutochka_services = await nakrutochka_client.get_services()
            nakrutochka_synced = await service_repo.sync_nakrutochka_services(nakrutochka_services)
            logger.info(f"✅ Synced {nakrutochka_synced} Nakrutochka services")

            # Get service counts
            view_services = await service_repo.get_services_by_type("views")
            reaction_services_nakrutochka = await service_repo.get_nakrutochka_services_by_type("reactions")
            repost_services_nakrutochka = await service_repo.get_nakrutochka_services_by_type("reposts")

            logger.info(
                f"📊 Services in database:\n"
                f"  • Views (Twiboost): {len(view_services)} services\n"
                f"  • Reactions (Nakrutochka): {len(reaction_services_nakrutochka)} services\n"
                f"  • Reposts (Nakrutochka): {len(repost_services_nakrutochka)} services"
            )

            # Display API configuration
            logger.info(
                f"🔧 API Configuration:\n"
                f"  • Views: {'Twiboost' if settings.use_twiboost_for_views else 'Nakrutochka'}\n"
                f"  • Reactions: {'Nakrutochka' if settings.use_nakrutochka_for_reactions else 'Twiboost'}\n"
                f"  • Reposts: {'Nakrutochka' if settings.use_nakrutochka_for_reposts else 'Twiboost'}\n"
                f"  • Fallback enabled: {settings.enable_api_fallback}"
            )

            # 5. Initialize Telegram bot
            logger.info("Initializing Telegram bot...")
            self.app = Application.builder().token(settings.telegram_bot_token).build()

            # 6. Setup handlers
            self.handlers = BotHandlers(self.app)
            logger.info("✅ Bot handlers configured")

            # 7. Initialize monitor
            self.monitor = TelegramMonitor(settings.telegram_bot_token)

            # Set global instance
            import src.bot.telegram_monitor
            src.bot.telegram_monitor.telegram_monitor = self.monitor

            logger.info("✅ Telegram monitor initialized")

            # 8. Validate existing channels
            logger.info("Validating existing channels...")
            await self.monitor.validate_all_channels()

            # 9. Update service mappings for all channels
            logger.info("Updating channel service mappings for dual API...")
            channels = await channel_repo.get_active_channels()

            for channel in channels:
                # Update API preferences
                api_preferences = {
                    "views": "twiboost",
                    "reactions": "nakrutochka",
                    "reposts": "nakrutochka"
                }

                await channel_repo.update_api_preferences(channel.id, api_preferences)

                # Update Twiboost service IDs (views)
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

                # Update Nakrutochka service IDs (reactions)
                if reaction_services_nakrutochka:
                    reaction_mapping = {}
                    for service in reaction_services_nakrutochka:
                        if service.emoji:
                            reaction_mapping[f"reaction_{service.emoji}"] = service.service_id

                    if reaction_mapping:
                        await channel_repo.update_service_ids(
                            channel.id,
                            "reactions",
                            reaction_mapping,
                            api_provider="nakrutochka"
                        )

                # Update Nakrutochka service IDs (reposts)
                if repost_services_nakrutochka:
                    await channel_repo.update_service_ids(
                        channel.id,
                        "reposts",
                        {"reposts": repost_services_nakrutochka[0].service_id},
                        api_provider="nakrutochka"
                    )

                logger.info(f"✅ Updated service mappings for channel: {channel.title}")

            # 10. Start scheduler
            logger.info("Starting task scheduler...")
            await task_scheduler.start()
            logger.info("✅ Scheduler started")

            logger.info("✨ Bot initialization complete with dual API support!")

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
            logger.info("🎉 Bot is fully operational with dual API support!")

            # Show admin info
            if settings.admin_telegram_id:
                logger.info(f"📱 Admin Telegram ID: {settings.admin_telegram_id}")
                logger.info("Send /start to the bot to begin")

            # Show API status
            logger.info("📊 API Status:")
            logger.info(f"  • Twiboost: Active for Views")
            logger.info(f"  • Nakrutochka: Active for Reactions & Reposts")

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

            # Close API clients
            await twiboost_client.close()
            await nakrutochka_client.close()

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
        # Read and execute initial migration
        with open("migrations/001_initial_schema.sql", "r") as f:
            schema = f.read()
        await db.execute(schema)
        logger.info("✅ Initial schema migration completed")

        # Read and execute Nakrutochka migration
        with open("migrations/002_nakrutochka_services.sql", "r") as f:
            nakrutochka_schema = f.read()
        await db.execute(nakrutochka_schema)
        logger.info("✅ Nakrutochka integration migration completed")

    except FileNotFoundError as e:
        logger.warning(f"Migration file not found: {e}")
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
    logger.info("Dual API Support: Twiboost + Nakrutochka")
    logger.info("=" * 50)

    # Check environment
    logger.info(f"Environment: {settings.environment}")
    logger.info(f"Check interval: {settings.check_interval}s")
    logger.info(f"Process interval: {settings.process_interval}s")

    # Show API configuration
    logger.info("API Configuration:")
    logger.info(f"  • Twiboost API: {settings.twiboost_api_url}")
    logger.info(f"  • Nakrutochka API: {settings.nakrutochka_api_url}")

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