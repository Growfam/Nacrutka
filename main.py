#!/usr/bin/env python3
"""
Telegram SMM Bot - Main entry point (SIMPLIFIED)
"""
import asyncio
import signal
import sys
import os
from typing import Optional
from datetime import datetime
import traceback

# Basic imports check
try:
    from src.config import settings
    from src.database.connection import db, test_connection
    from src.database.migrations import initialize_database
    from src.database.initialize import DatabaseInitializer
    from src.utils.logger import get_logger, DatabaseLogger
    from src.services.telegram import TelegramMonitor
    from src.services.processor import PostProcessor
    from src.services.nakrutka import NakrutkaClient
    from src.utils.scheduler import BotScheduler
except Exception as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

logger = get_logger(__name__)


class TelegramSMMBot:
    """Main bot application - simplified version"""

    def __init__(self):
        self.db = db
        self.db_logger: Optional[DatabaseLogger] = None
        self.telegram_monitor: Optional[TelegramMonitor] = None
        self.post_processor: Optional[PostProcessor] = None
        self.nakrutka_client: Optional[NakrutkaClient] = None
        self.scheduler: Optional[BotScheduler] = None
        self.running = False
        self.start_time = datetime.utcnow()

    async def setup(self):
        """Initialize all components - simplified"""
        try:
            print("🚀 Starting bot setup...")

            # 1. Database
            print("Initializing database...")
            await self.db.init()

            # Test connection
            if not await test_connection():
                raise Exception("Database connection failed")

            # Run migrations
            await initialize_database(self.db)

            # Initialize default data
            initializer = DatabaseInitializer(self.db)
            await initializer.initialize_all()

            print("✅ Database ready")

            # 2. Database logger
            self.db_logger = DatabaseLogger(self.db)
            await self.db_logger.info("Bot starting", environment=settings.environment)

            # 3. Nakrutka client
            print("Initializing Nakrutka...")
            self.nakrutka_client = NakrutkaClient()

            # Quick balance check
            try:
                balance_info = await self.nakrutka_client.get_balance()
                balance = float(balance_info.get('balance', 0))
                print(f"✅ Nakrutka connected. Balance: ${balance:.2f}")

                if balance < 1:
                    print("⚠️ WARNING: Low Nakrutka balance!")
            except Exception as e:
                print(f"⚠️ Nakrutka error: {e}")

            # 4. Telegram monitor
            print("Initializing Telegram...")
            self.telegram_monitor = TelegramMonitor(self.db, self.nakrutka_client)
            await self.telegram_monitor.setup_bot()
            print("✅ Telegram ready")

            # 5. Post processor
            self.post_processor = PostProcessor(self.db, self.nakrutka_client)
            print("✅ Post processor ready")

            # 6. Scheduler
            print("Setting up scheduler...")
            self.scheduler = BotScheduler()
            self._schedule_tasks()
            print("✅ Scheduler ready")

            print("\n✅ BOT SETUP COMPLETED")
            return True

        except Exception as e:
            print(f"\n❌ Setup failed: {e}")
            if self.db_logger:
                await self.db_logger.error("Bot setup failed", error=str(e))
            raise

    def _schedule_tasks(self):
        """Schedule periodic tasks - simplified"""
        # Main tasks only
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

    async def start(self):
        """Start the bot"""
        self.running = True
        print("Starting services...")

        try:
            # Start scheduler
            self.scheduler.start()

            # Start telegram bot
            await self.telegram_monitor.start_bot()

            await self.db_logger.info("Bot started successfully")

            print("\n" + "=" * 50)
            print("🟢 BOT IS RUNNING")
            print("Press Ctrl+C to stop")
            print("=" * 50 + "\n")

            # Main loop - simplified
            while self.running:
                await asyncio.sleep(60)
                # Simple heartbeat every minute
                logger.debug("Bot heartbeat")

        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            raise

    async def stop(self):
        """Stop the bot gracefully"""
        print("\n🛑 Stopping bot...")
        self.running = False

        try:
            # Stop services
            if self.scheduler:
                self.scheduler.stop()

            if self.telegram_monitor:
                await self.telegram_monitor.stop_bot()

            if self.nakrutka_client:
                await self.nakrutka_client.close()

            # Log shutdown
            if self.db_logger:
                uptime = datetime.utcnow() - self.start_time
                await self.db_logger.info(
                    "Bot stopping",
                    uptime_hours=uptime.total_seconds() / 3600
                )

            # Close database
            await self.db.close()

            print("🔴 BOT STOPPED")

        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

    def handle_signal(self, sig, frame):
        """Handle shutdown signals"""
        print(f"\n📡 Received signal {sig}")
        asyncio.create_task(self.stop())


async def main():
    """Main entry point"""
    print("\n" + "=" * 50)
    print("🚀 TELEGRAM SMM BOT")
    print("=" * 50)

    # Basic environment check
    required_vars = ['DATABASE_URL', 'NAKRUTKA_API_KEY', 'TELEGRAM_BOT_TOKEN']
    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        print(f"❌ Missing environment variables: {missing}")
        sys.exit(1)

    print(f"Environment: {settings.environment}")
    print(f"Check interval: {settings.check_interval}s")

    bot = TelegramSMMBot()

    # Setup signal handlers
    for sig in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, bot.handle_signal)

    try:
        # Setup and start
        await bot.setup()
        await bot.start()

    except KeyboardInterrupt:
        print("\n⌨️ Keyboard interrupt")
    except Exception as e:
        print(f"\n💥 Bot crashed: {e}")
        logger.error(f"Bot crashed: {e}", exc_info=True)

        if bot.db_logger:
            try:
                await bot.db_logger.error("Bot crashed", error=str(e))
            except:
                pass
    finally:
        await bot.stop()


if __name__ == "__main__":
    # Windows fix
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except Exception as e:
        print(f"❌ Failed to run: {e}")
        sys.exit(1)