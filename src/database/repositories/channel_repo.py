"""
Channel repository for database operations
"""
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import json

from src.database.connection import db
from src.database.models import Channel, ChannelSettings, ServiceType
from src.utils.logger import get_logger, LoggerMixin

logger = get_logger(__name__)


class ChannelRepository(LoggerMixin):
    """Repository for channel operations"""

    async def create_channel(self, channel_data: Dict[str, Any]) -> Channel:
        """Create new channel"""
        query = """
            INSERT INTO channels (id, username, title, is_active, monitoring_interval)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO UPDATE SET
                username = EXCLUDED.username,
                title = EXCLUDED.title,
                updated_at = NOW()
            RETURNING *
        """

        row = await db.fetchrow(
            query,
            channel_data["id"],
            channel_data.get("username"),
            channel_data["title"],
            channel_data.get("is_active", True),
            channel_data.get("monitoring_interval", 30)
        )

        self.log_info("Channel created/updated", channel_id=row["id"], title=row["title"])
        return self._row_to_channel(row)

    async def get_channel(self, channel_id: int) -> Optional[Channel]:
        """Get channel by ID"""
        query = "SELECT * FROM channels WHERE id = $1"
        row = await db.fetchrow(query, channel_id)

        if row:
            return self._row_to_channel(row)
        return None

    async def get_channel_with_username(self, channel_id: int) -> Optional[Channel]:
        """Get channel ensuring username is loaded"""
        channel = await self.get_channel(channel_id)

        if channel and not channel.username:
            # Try to get username from Telegram
            try:
                from src.bot.telegram_monitor import telegram_monitor
                if telegram_monitor:
                    info = await telegram_monitor.get_channel_info(channel_id)
                    if info and info.get("username"):
                        await self.update_channel_username(channel_id, info["username"])
                        channel.username = info["username"]
            except Exception as e:
                self.log_error(f"Failed to get channel username: {e}")

        return channel

    async def get_active_channels(self) -> List[Channel]:
        """Get all active channels"""
        query = """
            SELECT * FROM channels
            WHERE is_active = true
            ORDER BY created_at DESC
        """
        rows = await db.fetch(query)

        channels = [self._row_to_channel(row) for row in rows]
        self.log_debug(f"Found {len(channels)} active channels")
        return channels

    async def get_channels_to_check(self) -> List[Channel]:
        """Get channels that need checking"""
        query = """
            SELECT * FROM channels
            WHERE is_active = true
              AND (last_check_at IS NULL
                OR last_check_at < NOW() - INTERVAL '1 second' * monitoring_interval)
            ORDER BY last_check_at ASC NULLS FIRST
        """
        rows = await db.fetch(query)

        channels = [self._row_to_channel(row) for row in rows]
        self.log_debug(f"Found {len(channels)} channels to check")
        return channels

    async def update_last_check(self, channel_id: int):
        """Update last check timestamp"""
        query = """
            UPDATE channels
            SET last_check_at = NOW()
            WHERE id = $1
        """
        await db.execute(query, channel_id)

    async def update_channel_status(self, channel_id: int, is_active: bool):
        """Update channel active status"""
        query = """
            UPDATE channels
            SET is_active = $2,
                updated_at = NOW()
            WHERE id = $1
        """
        await db.execute(query, channel_id, is_active)

        self.log_info("Channel status updated", channel_id=channel_id, is_active=is_active)

    async def update_channel_username(self, channel_id: int, username: str):
        """Update channel username"""
        query = """
            UPDATE channels
            SET username = $2,
                updated_at = NOW()
            WHERE id = $1
        """
        await db.execute(query, channel_id, username)

        # Also update username in posts for correct links
        query_posts = """
            UPDATE posts
            SET channel_username = $2
            WHERE channel_id = $1
        """
        await db.execute(query_posts, channel_id, username)

        self.log_info(
            "Channel username updated",
            channel_id=channel_id,
            username=username
        )

    async def ensure_all_usernames(self):
        """Ensure all channels have usernames where possible"""
        channels = await self.get_active_channels()
        updated = 0

        for channel in channels:
            if not channel.username:
                try:
                    from src.bot.telegram_monitor import telegram_monitor
                    if telegram_monitor:
                        info = await telegram_monitor.get_channel_info(channel.id)
                        if info and info.get("username"):
                            await self.update_channel_username(channel.id, info["username"])
                            updated += 1
                            self.log_info(
                                f"Username updated for channel {channel.id}: {info['username']}"
                            )
                except Exception as e:
                    self.log_error(f"Failed to update username for {channel.id}: {e}")

        self.log_info(f"Updated usernames for {updated} channels")
        return updated

    async def delete_channel(self, channel_id: int):
        """Delete channel and all related data"""
        query = "DELETE FROM channels WHERE id = $1"
        await db.execute(query, channel_id)

        self.log_info("Channel deleted", channel_id=channel_id)

    # ========== Channel Settings Methods ==========

    async def create_settings(self, settings_data: Dict[str, Any]) -> ChannelSettings:
        """Create or update channel settings"""
        query = """
            INSERT INTO channel_settings (
                channel_id, service_type, base_quantity, randomization_percent,
                is_enabled, portions_count, fast_delivery_percent,
                reaction_distribution, repost_delay_minutes,
                drops_per_run, run_interval, twiboost_service_ids
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (channel_id, service_type) DO UPDATE SET
                base_quantity = EXCLUDED.base_quantity,
                randomization_percent = EXCLUDED.randomization_percent,
                is_enabled = EXCLUDED.is_enabled,
                portions_count = EXCLUDED.portions_count,
                fast_delivery_percent = EXCLUDED.fast_delivery_percent,
                reaction_distribution = EXCLUDED.reaction_distribution,
                repost_delay_minutes = EXCLUDED.repost_delay_minutes,
                drops_per_run = EXCLUDED.drops_per_run,
                run_interval = EXCLUDED.run_interval,
                twiboost_service_ids = EXCLUDED.twiboost_service_ids,
                updated_at = NOW()
            RETURNING *
        """

        # Convert dicts to JSON
        reaction_dist = json.dumps(settings_data.get("reaction_distribution")) if settings_data.get(
            "reaction_distribution") else None
        service_ids = json.dumps(settings_data.get("twiboost_service_ids")) if settings_data.get(
            "twiboost_service_ids") else None

        row = await db.fetchrow(
            query,
            settings_data["channel_id"],
            settings_data["service_type"],
            settings_data.get("base_quantity", 100),
            settings_data.get("randomization_percent", 0),
            settings_data.get("is_enabled", True),
            settings_data.get("portions_count", 5),
            settings_data.get("fast_delivery_percent", 70),
            reaction_dist,
            settings_data.get("repost_delay_minutes", 5),
            settings_data.get("drops_per_run", 5),
            settings_data.get("run_interval", 30),
            service_ids
        )

        self.log_info(
            "Settings created/updated",
            channel_id=settings_data["channel_id"],
            service_type=settings_data["service_type"]
        )

        return self._row_to_settings(row)

    async def get_channel_settings(
            self,
            channel_id: int,
            service_type: Optional[ServiceType] = None
    ) -> List[ChannelSettings]:
        """Get channel settings"""
        if service_type:
            query = """
                SELECT * FROM channel_settings
                WHERE channel_id = $1
                  AND service_type = $2
                  AND is_enabled = true
            """
            rows = await db.fetch(query, channel_id, service_type)
        else:
            query = """
                SELECT * FROM channel_settings
                WHERE channel_id = $1
                  AND is_enabled = true
                ORDER BY service_type
            """
            rows = await db.fetch(query, channel_id)

        return [self._row_to_settings(row) for row in rows]

    async def get_all_settings(self) -> List[ChannelSettings]:
        """Get all channel settings"""
        query = """
            SELECT cs.*
            FROM channel_settings cs
            JOIN channels c ON cs.channel_id = c.id
            WHERE cs.is_enabled = true
              AND c.is_active = true
            ORDER BY cs.channel_id, cs.service_type
        """
        rows = await db.fetch(query)

        return [self._row_to_settings(row) for row in rows]

    async def update_service_ids(
            self,
            channel_id: int,
            service_type: str,
            service_ids: Dict[str, int]
    ):
        """Update Twiboost service IDs for channel"""
        query = """
            UPDATE channel_settings
            SET twiboost_service_ids = $3,
                updated_at = NOW()
            WHERE channel_id = $1
              AND service_type = $2
        """

        await db.execute(query, channel_id, service_type, json.dumps(service_ids))

        self.log_info(
            "Service IDs updated",
            channel_id=channel_id,
            service_type=service_type,
            service_ids=service_ids
        )

    async def bulk_create_default_settings(self, channel_id: int):
        """Create default settings for all service types"""
        default_settings = [
            {
                "channel_id": channel_id,
                "service_type": ServiceType.VIEWS,
                "base_quantity": 1000,
                "randomization_percent": 0,
                "portions_count": 5,
                "fast_delivery_percent": 70
            },
            {
                "channel_id": channel_id,
                "service_type": ServiceType.REACTIONS,
                "base_quantity": 100,
                "randomization_percent": 40,
                "reaction_distribution": {"👍": 45, "❤️": 30, "🔥": 25},
                "portions_count": 1,
                "drops_per_run": 5,
                "run_interval": 23
            },
            {
                "channel_id": channel_id,
                "service_type": ServiceType.REPOSTS,
                "base_quantity": 50,
                "randomization_percent": 40,
                "portions_count": 1,
                "repost_delay_minutes": 5,
                "drops_per_run": 3,
                "run_interval": 34
            }
        ]

        for settings in default_settings:
            await self.create_settings(settings)

        self.log_info("Default settings created", channel_id=channel_id)

    # ========== Helper Methods ==========

    def _row_to_channel(self, row) -> Channel:
        """Convert database row to Channel model"""
        return Channel(
            id=row["id"],
            username=row["username"],
            title=row["title"],
            is_active=row["is_active"],
            monitoring_interval=row["monitoring_interval"],
            created_at=row["created_at"],
            updated_at=row["updated_at"]
        )

    def _row_to_settings(self, row) -> ChannelSettings:
        """Convert database row to ChannelSettings model"""
        return ChannelSettings(
            id=row["id"],
            channel_id=row["channel_id"],
            service_type=ServiceType(row["service_type"]),
            base_quantity=row["base_quantity"],
            randomization_percent=row["randomization_percent"],
            portions_count=row["portions_count"],
            fast_delivery_percent=row["fast_delivery_percent"],
            reaction_distribution=json.loads(row["reaction_distribution"]) if row["reaction_distribution"] else None,
            repost_delay_minutes=row["repost_delay_minutes"],
            twiboost_service_ids=json.loads(row["twiboost_service_ids"]) if row["twiboost_service_ids"] else None,
            drops_per_run=row["drops_per_run"],
            run_interval=row["run_interval"],
            updated_at=row["updated_at"]
        )


# Global repository instance
channel_repo = ChannelRepository()