#!/usr/bin/env python3
"""
Telegram SMM Bot - Simplified Main
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
from src.services.monitor import ChannelMonitor
from src.services.sender import OrderSender
from src.services.checker import StatusChecker
from src.database.queries import Queries


class TelegramSMMBot:
    """Main bot application - simplified"""

    def __init__(self):
        self.db = db
        self.queries = Queries()
        self.monitor = ChannelMonitor()
        self.sender = OrderSender()
        self.checker = StatusChecker()
        self.running = False
        self.start_time = datetime.utcnow()

    async def setup(self):
        """Initialize database connection"""
        try:
            logger.info("Initializing database...")
            await self.db.init()

            # Test connection
            result = await self.db.fetchval("SELECT 1")
            if result == 1:
                logger.info("✅ Database connected")
            else:
                raise Exception("Database test failed")

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

    async def monitor_task(self):
        """Task for monitoring channels"""
        while self.running:
            try:
                await self.monitor.check_channels()
            except Exception as e:
                logger.error(f"Monitor error: {e}")

            await asyncio.sleep(settings.check_interval)

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

                logger.info(
                    f"📊 Stats - "
                    f"Uptime: {uptime:.1f}h | "
                    f"Channels: {stats.get('active_channels', 0)} | "
                    f"Pending: {stats.get('pending_orders', 0)} | "
                    f"Sent: {stats.get('sent_orders', 0)} | "
                    f"24h orders: {stats.get('orders_24h', 0)}"
                )

            except Exception as e:
                logger.error(f"Stats error: {e}")

            await asyncio.sleep(300)  # Every 5 minutes

    async def cleanup_task(self):
        """Task for cleanup"""
        while self.running:
            try:
                # Clean old logs
                deleted = await self.queries.cleanup_logs(days=7)
                if deleted > 0:
                    logger.info(f"Cleaned {deleted} old logs")

            except Exception as e:
                logger.error(f"Cleanup error: {e}")

            await asyncio.sleep(3600)  # Every hour

    async def start(self):
        """Start the bot"""
        self.running = True

        logger.info("Starting bot tasks...")

        # Create tasks
        tasks = [
            asyncio.create_task(self.monitor_task()),
            asyncio.create_task(self.sender_task()),
            asyncio.create_task(self.checker_task()),
            asyncio.create_task(self.stats_task()),
            asyncio.create_task(self.cleanup_task()),
        ]

        logger.info("=" * 50)
        logger.info("🟢 BOT IS RUNNING")
        logger.info(f"Monitor interval: {settings.check_interval}s")
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
    logger.info("🚀 TELEGRAM SMM BOT (Simplified)")
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