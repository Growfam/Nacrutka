#!/usr/bin/env python3
"""
Telegram SMM Bot - FIXED VERSION with Polling
"""
import asyncio
import signal
import sys
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import components
from src.config import settings
from src.database.connection import db
from src.services.telegram_poller import TelegramPoller  # НОВИЙ КОМПОНЕНТ
from src.services.sender import OrderSender
from src.services.checker import StatusChecker
from src.database.queries import Queries


class TelegramSMMBot:
    """Main bot application - FIXED VERSION"""

    def __init__(self):
        self.db = db
        self.queries = Queries()
        self.poller = TelegramPoller()  # Замість monitor
        self.sender = OrderSender()
        self.checker = StatusChecker()
        self.running = False
        self.start_time = datetime.utcnow()

    async def setup(self):
        """Initialize database and Telegram polling"""
        try:
            logger.info("Initializing database...")
            await self.db.init()

            # Test connection
            result = await self.db.fetchval("SELECT 1")
            if result == 1:
                logger.info("✅ Database connected")
            else:
                raise Exception("Database test failed")

            # Setup Telegram polling
            logger.info("Setting up Telegram polling...")
            await self.poller.setup()

            # Check Nakrutka balance
            try:
                balance = await self.sender.nakrutka.get_balance()
                logger.info(f"✅ Nakrutka connected. Balance: ${balance.get('balance', 0)}")
            except Exception as e:
                logger.warning(f"⚠️ Nakrutka check failed: {e}")

            return True

        except Exception as e:
            logger.error(f"❌ Setup failed: {e}")
            raise

    async def polling_task(self):
        """Task for Telegram polling - отримує нові пости"""
        logger.info("🚀 Starting Telegram polling...")
        await self.poller.start_polling()

        # Polling працює в фоні
        while self.running:
            await asyncio.sleep(10)  # Просто чекаємо

    async def sender_task(self):
        """Task for sending orders"""
        while self.running:
            try:
                # Send pending orders
                await self.sender.send_pending_orders()

                # Check scheduled orders
                await self.sender.send_scheduled_orders()

            except Exception as e:
                logger.error(f"Sender error: {e}")

            await asyncio.sleep(settings.send_interval)

    async def checker_task(self):
        """Task for checking status"""
        while self.running:
            try:
                # Check order status
                await self.checker.check_order_status()

                # Move scheduled to pending
                await self.checker.check_scheduled_orders()

                # Cleanup old orders
                await self.checker.cleanup_old_orders()

            except Exception as e:
                logger.error(f"Checker error: {e}")

            await asyncio.sleep(settings.status_check_interval)

    async def stats_task(self):
        """Task for printing statistics"""
        while self.running:
            try:
                stats = await self.queries.get_stats()
                uptime = (datetime.utcnow() - self.start_time).total_seconds() / 3600

                # Додаємо статистику по постах
                posts_stats = await self.db.fetchrow("""
                                                     SELECT COUNT(CASE WHEN is_processed = false THEN 1 END)                   as unprocessed,
                                                            COUNT(CASE WHEN created_at > NOW() - INTERVAL '1 hour' THEN 1 END) as posts_1h
                                                     FROM posts
                                                     """)

                logger.info(
                    f"📊 Stats - "
                    f"Uptime: {uptime:.1f}h | "
                    f"Channels: {stats.get('active_channels', 0)} | "
                    f"New posts (1h): {posts_stats['posts_1h']} | "
                    f"Unprocessed: {posts_stats['unprocessed']} | "
                    f"Pending orders: {stats.get('pending_orders', 0)} | "
                    f"Sent: {stats.get('sent_orders', 0)}"
                )

            except Exception as e:
                logger.error(f"Stats error: {e}")

            await asyncio.sleep(300)  # Every 5 minutes

    async def process_old_posts_on_start(self):
        """Обробка старих постів при старті згідно налаштувань"""
        logger.info("Checking for old posts processing requirements...")

        channels = await self.db.fetch("""
                                       SELECT id, username, process_old_posts
                                       FROM channels
                                       WHERE is_active = true
                                         AND process_old_posts > 0
                                       """)

        for channel in channels:
            existing_posts = await self.db.fetchval(
                "SELECT COUNT(*) FROM posts WHERE channel_id = $1",
                channel['id']
            )

            if existing_posts == 0:
                logger.info(
                    f"Channel @{channel['username']}: "
                    f"will process {channel['process_old_posts']} old posts on first message"
                )

    async def start(self):
        """Start the bot"""
        self.running = True

        # Перевіряємо налаштування старих постів
        await self.process_old_posts_on_start()

        logger.info("Starting bot tasks...")

        # Create tasks
        tasks = [
            asyncio.create_task(self.polling_task()),  # Новий polling замість monitor
            asyncio.create_task(self.sender_task()),
            asyncio.create_task(self.checker_task()),
            asyncio.create_task(self.stats_task()),
        ]

        logger.info("=" * 50)
        logger.info("🟢 BOT IS RUNNING - FIXED VERSION")
        logger.info("✅ Telegram polling active - will receive new posts automatically")
        logger.info(f"Send interval: {settings.send_interval}s")
        logger.info(f"Check interval: {settings.status_check_interval}s")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 50)

        # Wait for tasks
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Tasks cancelled")

    async def stop(self):
        """Stop the bot"""
        logger.info("Stopping bot...")
        self.running = False

        # Stop polling
        await self.poller.stop()

        # Close connections
        await self.sender.nakrutka.close()
        await self.db.close()

        # Final log
        uptime = (datetime.utcnow() - self.start_time).total_seconds() / 3600
        await self.queries.log(
            'info',
            'Bot stopped',
            {'uptime_hours': uptime}
        )

        logger.info("🔴 BOT STOPPED")


async def main():
    """Main entry point"""
    logger.info("=" * 50)
    logger.info("🚀 TELEGRAM SMM BOT - FIXED VERSION")
    logger.info("=" * 50)

    # Check environment
    if not settings.database_url:
        logger.error("❌ DATABASE_URL not set")
        sys.exit(1)

    if not settings.nakrutka_api_key:
        logger.error("❌ NAKRUTKA_API_KEY not set")
        sys.exit(1)

    if not settings.telegram_bot_token:
        logger.error("❌ TELEGRAM_BOT_TOKEN not set")
        sys.exit(1)

    bot = TelegramSMMBot()

    # Setup
    try:
        await bot.setup()
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        sys.exit(1)

    # Handle signals
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}")
        asyncio.create_task(bot.stop())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start bot
    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
    finally:
        await bot.stop()


if __name__ == "__main__":
    # Windows compatibility
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Failed to run: {e}")
        sys.exit(1)