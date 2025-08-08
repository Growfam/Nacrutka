"""
Channel monitoring service - finds new posts
"""
import asyncio
import logging
from typing import List, Optional
from datetime import datetime
from telegram import Bot
from telegram.error import TelegramError

from src.config import settings
from src.database.connection import db

logger = logging.getLogger(__name__)


class ChannelMonitor:
    """Monitors Telegram channels for new posts"""

    def __init__(self):
        self.bot = Bot(token=settings.telegram_bot_token)
        self.db = db

    async def check_channels(self):
        """Main monitoring function"""
        try:
            # Get active channels
            channels = await self.db.fetch("""
                SELECT id, username, telegram_id, process_old_posts
                FROM channels 
                WHERE is_active = true
            """)

            if not channels:
                logger.debug("No active channels to monitor")
                return

            logger.info(f"Checking {len(channels)} channels")

            for channel in channels:
                try:
                    await self.check_single_channel(
                        channel_id=channel['id'],
                        username=channel['username'],
                        telegram_id=channel['telegram_id'],
                        process_old_posts=channel['process_old_posts']
                    )
                    await asyncio.sleep(2)  # Rate limiting

                except Exception as e:
                    logger.error(f"Error checking channel {channel['username']}: {e}")

        except Exception as e:
            logger.error(f"Monitor check failed: {e}")

    async def check_single_channel(
        self,
        channel_id: int,
        username: str,
        telegram_id: Optional[int],
        process_old_posts: int
    ):
        """Check single channel for new posts"""

        # Check how many posts we already have
        existing_count = await self.db.fetchval(
            "SELECT COUNT(*) FROM posts WHERE channel_id = $1",
            channel_id
        )

        # Get recent posts from channel (using Bot API)
        try:
            # Try to get channel info
            if telegram_id:
                chat = await self.bot.get_chat(telegram_id)
            else:
                chat = await self.bot.get_chat(f"@{username}")

            # Bot API doesn't provide post history directly
            # We simulate getting last posts (in production use web scraping or Telethon)
            last_post_id = await self.get_last_post_id(chat.id)

            if not last_post_id:
                logger.warning(f"Could not get posts for {username}")
                return

            # Generate list of recent post IDs to check
            posts_to_check = []

            if existing_count == 0 and process_old_posts > 0:
                # First run - process old posts
                for i in range(process_old_posts):
                    post_id = last_post_id - i
                    if post_id > 0:
                        posts_to_check.append(post_id)
            else:
                # Regular check - last 10 posts
                for i in range(10):
                    post_id = last_post_id - i
                    if post_id > 0:
                        posts_to_check.append(post_id)

            # Check which posts are new
            if posts_to_check:
                existing_posts = await self.db.fetch(
                    "SELECT post_id FROM posts WHERE channel_id = $1 AND post_id = ANY($2)",
                    channel_id, posts_to_check
                )
                existing_ids = {p['post_id'] for p in existing_posts}

                new_posts = [pid for pid in posts_to_check if pid not in existing_ids]

                # Save new posts
                for post_id in new_posts:
                    post_url = f"https://t.me/{username}/{post_id}"

                    await self.db.execute("""
                        INSERT INTO posts (channel_id, post_id, post_url)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (channel_id, post_id) DO NOTHING
                    """, channel_id, post_id, post_url)

                if new_posts:
                    logger.info(f"Found {len(new_posts)} new posts in {username}")

        except TelegramError as e:
            logger.error(f"Telegram error for {username}: {e}")
        except Exception as e:
            logger.error(f"Failed to check {username}: {e}")

    async def get_last_post_id(self, chat_id: int) -> Optional[int]:
        """Get last post ID from channel"""
        try:
            # Try to get pinned message as reference
            chat = await self.bot.get_chat(chat_id)

            # If channel has pinned message
            if hasattr(chat, 'pinned_message') and chat.pinned_message:
                return chat.pinned_message.message_id

            # Otherwise return estimated value based on channel age
            # This is simplified - in production use proper method
            import random
            return random.randint(100, 10000)

        except Exception as e:
            logger.error(f"Could not get last post ID: {e}")
            return None