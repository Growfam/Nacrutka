"""
Telegram channel monitoring service
"""
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import asyncio

from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError

from src.config import settings
from src.database.connection import DatabaseConnection
from src.database.queries import Queries
from src.database.models import Channel, Post
from src.utils.logger import get_logger, DatabaseLogger

logger = get_logger(__name__)


class TelegramMonitor:
    """Monitors Telegram channels for new posts"""

    def __init__(self, db: DatabaseConnection):
        self.db = db
        self.queries = Queries(db)
        self.db_logger = DatabaseLogger(db)
        self.bot = Bot(token=settings.telegram_bot_token)
        self.app: Optional[Application] = None

        # Cache for channel posts
        self._channel_cache: Dict[int, List[int]] = {}
        self._cache_ttl = 300  # 5 minutes
        self._cache_updated: Dict[int, datetime] = {}

    async def setup_bot(self):
        """Setup telegram bot handlers"""
        self.app = Application.builder().token(settings.telegram_bot_token).build()

        # Add command handlers
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("status", self.cmd_status))
        self.app.add_handler(CommandHandler("stats", self.cmd_stats))

        logger.info("Telegram bot handlers configured")

    async def start_bot(self):
        """Start telegram bot (if needed for commands)"""
        if self.app:
            await self.app.initialize()
            await self.app.start()
            logger.info("Telegram bot started")

    async def stop_bot(self):
        """Stop telegram bot"""
        if self.app:
            await self.app.stop()
            logger.info("Telegram bot stopped")

    async def check_channels(self):
        """Check all active channels for new posts"""
        try:
            channels = await self.queries.get_active_channels()

            if not channels:
                logger.debug("No active channels to monitor")
                return

            logger.info(f"Checking {len(channels)} active channels")

            # Check each channel
            tasks = [self.check_channel(channel) for channel in channels]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Count results
            new_posts = sum(1 for r in results if r and not isinstance(r, Exception))
            errors = sum(1 for r in results if isinstance(r, Exception))

            if new_posts > 0:
                logger.info(f"Found {new_posts} new posts")
                await self.db_logger.info(
                    "New posts found",
                    count=new_posts,
                    channels_checked=len(channels)
                )

            if errors > 0:
                logger.warning(f"Errors checking {errors} channels")

        except Exception as e:
            logger.error("Failed to check channels", error=str(e))
            await self.db_logger.error(
                "Channel check failed",
                error=str(e)
            )

    async def check_channel(self, channel: Channel) -> Optional[int]:
        """Check single channel for new posts"""
        try:
            # Get recent posts from channel
            recent_posts = await self.get_channel_posts(
                channel.channel_username,
                channel.channel_id
            )

            if not recent_posts:
                return None

            # Get existing posts from DB
            existing_posts = await self.queries.get_channel_posts(
                channel.id,
                limit=200
            )
            existing_set = set(existing_posts)

            # Find new posts
            new_posts = [
                post_id for post_id in recent_posts
                if post_id not in existing_set
            ]

            if not new_posts:
                return None

            logger.info(
                f"Found {len(new_posts)} new posts in {channel.channel_username}",
                post_ids=new_posts[:5]  # Log first 5
            )

            # Save new posts to database
            created_count = 0
            for post_id in new_posts:
                post_url = f"https://t.me/{channel.channel_username}/{post_id}"

                result = await self.queries.create_post(
                    channel_id=channel.id,
                    post_id=post_id,
                    post_url=post_url
                )

                if result:
                    created_count += 1

            return created_count

        except Exception as e:
            logger.error(
                f"Failed to check channel {channel.channel_username}",
                error=str(e),
                channel_id=channel.id
            )
            return None

    async def get_channel_posts(
            self,
            username: str,
            channel_id: int,
            limit: int = 20
    ) -> List[int]:
        """Get recent post IDs from channel"""
        # Check cache first
        cache_key = channel_id
        if cache_key in self._channel_cache:
            cache_age = datetime.now() - self._cache_updated.get(cache_key, datetime.min)
            if cache_age.total_seconds() < self._cache_ttl:
                return self._channel_cache[cache_key]

        try:
            # Get channel messages
            posts = []

            # Try to get channel history
            # Note: Bot must be admin in channel or channel must be public
            try:
                # For public channels, we can try to get updates
                updates = await self.bot.get_updates(
                    allowed_updates=["channel_post"],
                    limit=limit
                )

                for update in updates:
                    if update.channel_post and update.channel_post.chat.id == channel_id:
                        posts.append(update.channel_post.message_id)

            except Exception:
                # Alternative method - parse from web if needed
                # This would require additional implementation
                pass

            # Update cache
            self._channel_cache[cache_key] = posts[-limit:]
            self._cache_updated[cache_key] = datetime.now()

            return posts

        except TelegramError as e:
            logger.error(
                f"Telegram API error for {username}",
                error=str(e),
                error_code=e.message
            )
            return []

    async def get_post_info(self, channel_id: int, post_id: int) -> Dict[str, Any]:
        """Get detailed post information"""
        try:
            # This would get post details like views, reactions etc
            # Implementation depends on bot permissions
            return {
                'post_id': post_id,
                'channel_id': channel_id,
                'views': 0,  # Would need to parse from message
                'reactions': {},
                'forwards': 0
            }
        except Exception as e:
            logger.error("Failed to get post info", error=str(e))
            return {}

    # ============ BOT COMMANDS ============

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        if not self._is_admin(update.effective_user.id):
            return

        await update.message.reply_text(
            "🤖 Telegram SMM Bot\n\n"
            "Commands:\n"
            "/status - Bot status\n"
            "/stats - Statistics\n"
        )

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command"""
        if not self._is_admin(update.effective_user.id):
            return

        try:
            # Get active channels count
            channels = await self.queries.get_active_channels()

            # Get today's stats
            today_costs = await self.queries.get_today_costs()
            total_cost = sum(today_costs.values())

            # Get active orders
            active_orders = await self.queries.get_active_orders()

            message = (
                "📊 Bot Status\n\n"
                f"✅ Active channels: {len(channels)}\n"
                f"📝 Active orders: {len(active_orders)}\n"
                f"💰 Today's cost: ${total_cost:.2f}\n"
            )

            await update.message.reply_text(message)

        except Exception as e:
            logger.error("Failed to get status", error=str(e))
            await update.message.reply_text("❌ Failed to get status")

    async def cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command"""
        if not self._is_admin(update.effective_user.id):
            return

        try:
            channels = await self.queries.get_active_channels()

            message = "📈 Channel Statistics\n\n"

            for channel in channels[:10]:  # Limit to 10 channels
                stats = await self.queries.get_channel_stats(channel.id)

                message += (
                    f"📢 {channel.channel_username}\n"
                    f"  Total: {stats['total_posts']}\n"
                    f"  Completed: {stats['completed_posts']}\n"
                    f"  Processing: {stats['processing_posts']}\n"
                    f"  Failed: {stats['failed_posts']}\n\n"
                )

            await update.message.reply_text(message)

        except Exception as e:
            logger.error("Failed to get stats", error=str(e))
            await update.message.reply_text("❌ Failed to get statistics")

    def _is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        return settings.admin_telegram_id and user_id == settings.admin_telegram_id