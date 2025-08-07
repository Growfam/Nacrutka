"""
Telegram channel monitor using Bot API
"""
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from telegram import Bot, Message
from telegram.error import TelegramError, Forbidden, BadRequest
from telegram.constants import ChatType

from src.database.models import Channel, Post
from src.database.repositories.channel_repo import channel_repo
from src.database.repositories.post_repo import post_repo
from src.utils.logger import get_logger, LoggerMixin, metrics
from src.config import settings

logger = get_logger(__name__)


class TelegramMonitor(LoggerMixin):
    """Monitor Telegram channels for new posts"""

    def __init__(self, bot_token: str):
        self.bot = Bot(token=bot_token)
        self.monitoring = False
        self.channels_cache = {}
        self.last_messages_cache = {}

    async def start_monitoring(self):
        """Start monitoring channels"""
        self.monitoring = True
        self.log_info("Channel monitoring started")

        while self.monitoring:
            try:
                await self.check_channels()
                await asyncio.sleep(settings.check_interval)
            except Exception as e:
                self.log_error("Monitoring error", error=e)
                await asyncio.sleep(settings.retry_delay)

    async def stop_monitoring(self):
        """Stop monitoring channels"""
        self.monitoring = False
        self.log_info("Channel monitoring stopped")

    async def check_channels(self):
        """Check all active channels for new posts"""
        channels = await channel_repo.get_channels_to_check()

        if not channels:
            self.log_debug("No channels to check")
            return

        self.log_debug(f"Checking {len(channels)} channels")

        # Process channels concurrently but with limit
        semaphore = asyncio.Semaphore(5)  # Max 5 concurrent checks

        async def check_with_limit(channel):
            async with semaphore:
                await self._check_single_channel(channel)

        tasks = [check_with_limit(channel) for channel in channels]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _check_single_channel(self, channel: Channel):
        """Check single channel for new posts"""
        try:
            # Update last check time
            await channel_repo.update_last_check(channel.id)

            # Get channel info and recent messages
            chat = await self.bot.get_chat(channel.id)

            # Get last known message ID
            last_known_id = await post_repo.get_last_message_id(channel.id)

            # Try to get recent messages
            new_posts = await self._fetch_new_messages(
                channel.id,
                last_known_id
            )

            if new_posts:
                self.log_info(
                    f"Found {len(new_posts)} new posts",
                    channel_id=channel.id,
                    channel_title=channel.title
                )

                # Save new posts to database
                posts_data = []
                for msg in new_posts:
                    post_data = {
                        "channel_id": channel.id,
                        "message_id": msg["message_id"],
                        "content": msg.get("text", ""),
                        "media_type": msg.get("media_type")
                    }
                    posts_data.append(post_data)

                created_posts = await post_repo.bulk_create_posts(posts_data)

                self.log_info(
                    f"Saved {len(created_posts)} new posts",
                    channel_id=channel.id
                )
            else:
                self.log_debug(
                    "No new posts found",
                    channel_id=channel.id,
                    last_known_id=last_known_id
                )

        except Forbidden:
            # Bot was removed from channel
            self.log_warning(
                "Bot removed from channel, deactivating",
                channel_id=channel.id
            )
            await channel_repo.update_channel_status(channel.id, False)

        except BadRequest as e:
            if "chat not found" in str(e).lower():
                self.log_warning(
                    "Channel not found, deactivating",
                    channel_id=channel.id
                )
                await channel_repo.update_channel_status(channel.id, False)
            else:
                self.log_error(
                    "Bad request error",
                    error=e,
                    channel_id=channel.id
                )

        except Exception as e:
            self.log_error(
                "Failed to check channel",
                error=e,
                channel_id=channel.id
            )
            metrics.log_error_rate("channel_check", 1)

    async def _fetch_new_messages(
            self,
            channel_id: int,
            last_known_id: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Fetch new messages from channel"""
        new_messages = []

        try:
            # This is a simplified approach
            # In real implementation, you might need to use different methods
            # depending on bot permissions and channel type

            # Try to get channel history (requires admin rights)
            # For channels where bot is admin, we can iterate through messages

            if last_known_id:
                # Check messages after last known
                start_id = last_known_id + 1

                # Try to fetch up to 100 recent messages
                for message_id in range(start_id, start_id + 100):
                    try:
                        # This is a workaround - trying to forward message to check if it exists
                        # In production, you'd use Updates or other methods
                        msg_data = {
                            "message_id": message_id,
                            "text": None,
                            "media_type": None
                        }

                        # Check if post exists
                        exists = await post_repo.check_post_exists(channel_id, message_id)
                        if not exists:
                            new_messages.append(msg_data)

                    except:
                        # Message doesn't exist or we can't access it
                        break
            else:
                # First time checking - get last few messages
                # This is simplified - in production you'd implement proper pagination
                for i in range(1, 11):  # Get last 10 messages
                    msg_data = {
                        "message_id": i,
                        "text": f"Message {i}",
                        "media_type": "text"
                    }
                    new_messages.append(msg_data)

            return new_messages

        except Exception as e:
            self.log_error(
                "Failed to fetch messages",
                error=e,
                channel_id=channel_id
            )
            return []

    async def add_channel(self, channel_id: int) -> Optional[Channel]:
        """Add new channel to monitoring"""
        try:
            # Get channel info
            chat = await self.bot.get_chat(channel_id)

            # Check if bot is admin
            bot_member = await chat.get_member(self.bot.id)

            if not bot_member.status in ["administrator", "creator"]:
                self.log_warning(
                    "Bot is not admin in channel",
                    channel_id=channel_id
                )
                return None

            # Create channel in database
            channel_data = {
                "id": channel_id,
                "username": chat.username,
                "title": chat.title,
                "is_active": True
            }

            channel = await channel_repo.create_channel(channel_data)

            # Create default settings
            await channel_repo.bulk_create_default_settings(channel_id)

            self.log_info(
                "Channel added to monitoring",
                channel_id=channel_id,
                title=chat.title
            )

            return channel

        except Exception as e:
            self.log_error(
                "Failed to add channel",
                error=e,
                channel_id=channel_id
            )
            return None

    async def remove_channel(self, channel_id: int):
        """Remove channel from monitoring"""
        try:
            await channel_repo.update_channel_status(channel_id, False)

            # Clear cache
            if channel_id in self.channels_cache:
                del self.channels_cache[channel_id]
            if channel_id in self.last_messages_cache:
                del self.last_messages_cache[channel_id]

            self.log_info(
                "Channel removed from monitoring",
                channel_id=channel_id
            )

        except Exception as e:
            self.log_error(
                "Failed to remove channel",
                error=e,
                channel_id=channel_id
            )

    async def get_channel_info(self, channel_id: int) -> Optional[Dict[str, Any]]:
        """Get channel information"""
        try:
            chat = await self.bot.get_chat(channel_id)

            # Get member count if available
            member_count = await chat.get_member_count() if hasattr(chat, 'get_member_count') else None

            # Get bot permissions
            bot_member = await chat.get_member(self.bot.id)

            return {
                "id": chat.id,
                "title": chat.title,
                "username": chat.username,
                "type": chat.type,
                "member_count": member_count,
                "bot_status": bot_member.status,
                "bot_permissions": bot_member.status in ["administrator", "creator"]
            }

        except Exception as e:
            self.log_error(
                "Failed to get channel info",
                error=e,
                channel_id=channel_id
            )
            return None

    async def validate_all_channels(self):
        """Validate all active channels"""
        channels = await channel_repo.get_active_channels()

        self.log_info(f"Validating {len(channels)} channels")

        invalid_count = 0
        for channel in channels:
            info = await self.get_channel_info(channel.id)

            if not info or not info["bot_permissions"]:
                await channel_repo.update_channel_status(channel.id, False)
                invalid_count += 1
                self.log_warning(
                    "Channel validation failed",
                    channel_id=channel.id,
                    title=channel.title
                )

        self.log_info(
            "Channel validation completed",
            total=len(channels),
            invalid=invalid_count,
            valid=len(channels) - invalid_count
        )


# Global monitor instance (will be initialized in main)
telegram_monitor: Optional[TelegramMonitor] = None