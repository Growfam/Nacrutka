"""
Channel repository for database operations with dual API support
"""
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import json

from src.database.connection import db
from src.database.models import Channel, ChannelSettings, ServiceType, APIProvider
from src.utils.logger import get_logger, LoggerMixin

logger = get_logger(__name__)


class ChannelRepository(LoggerMixin):
    """Repository for channel operations with dual API support"""

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
                drops_per_run, run_interval, twiboost_service_ids,
                nakrutochka_service_ids, api_preferences
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
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
                nakrutochka_service_ids = EXCLUDED.nakrutochka_service_ids,
                api_preferences = EXCLUDED.api_preferences,
                updated_at = NOW()
            RETURNING *
        """

        # Convert dicts to JSON
        reaction_dist = json.dumps(settings_data.get("reaction_distribution")) if settings_data.get(
            "reaction_distribution") else None
        twiboost_ids = json.dumps(settings_data.get("twiboost_service_ids")) if settings_data.get(
            "twiboost_service_ids") else None
        nakrutochka_ids = json.dumps(settings_data.get("nakrutochka_service_ids")) if settings_data.get(
            "nakrutochka_service_ids") else None
        api_prefs = json.dumps(settings_data.get("api_preferences")) if settings_data.get(
            "api_preferences") else None

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
            twiboost_ids,
            nakrutochka_ids,
            api_prefs
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

    # ========== Dual API Support Methods ==========

    async def update_api_preferences(self, channel_id: int, preferences: Dict[str, str]):
        """Update API preferences for channel"""
        query = """
            UPDATE channel_settings
            SET api_preferences = $2,
                updated_at = NOW()
            WHERE channel_id = $1
        """

        await db.execute(query, channel_id, json.dumps(preferences))

        self.log_info(
            "API preferences updated",
            channel_id=channel_id,
            preferences=preferences
        )

    async def update_api_preference(self, channel_id: int, service_type: str, api: str):
        """Update API preference for specific service type"""
        # Get current preferences
        query = """
            SELECT api_preferences FROM channel_settings
            WHERE channel_id = $1 AND service_type = $2
            LIMIT 1
        """
        row = await db.fetchrow(query, channel_id, service_type)

        if row and row["api_preferences"]:
            preferences = json.loads(row["api_preferences"])
        else:
            preferences = {}

        preferences[service_type] = api

        # Update
        update_query = """
            UPDATE channel_settings
            SET api_preferences = $3,
                updated_at = NOW()
            WHERE channel_id = $1 AND service_type = $2
        """

        await db.execute(update_query, channel_id, service_type, json.dumps(preferences))

        self.log_info(
            f"API preference updated for {service_type}",
            channel_id=channel_id,
            api=api
        )

    async def update_service_ids(
        self,
        channel_id: int,
        service_type: str,
        service_ids: Dict[str, int],
        api_provider: str = "twiboost"
    ):
        """Update service IDs for specific API"""
        if api_provider == "twiboost":
            column = "twiboost_service_ids"
        elif api_provider == "nakrutochka":
            column = "nakrutochka_service_ids"
        else:
            raise ValueError(f"Unknown API provider: {api_provider}")

        query = f"""
            UPDATE channel_settings
            SET {column} = $3,
                updated_at = NOW()
            WHERE channel_id = $1
              AND service_type = $2
        """

        await db.execute(query, channel_id, service_type, json.dumps(service_ids))

        self.log_info(
            f"{api_provider} service IDs updated",
            channel_id=channel_id,
            service_type=service_type,
            service_ids=service_ids
        )

    async def get_api_preference(
        self,
        channel_id: int,
        service_type: ServiceType
    ) -> APIProvider:
        """Get preferred API for service type"""
        query = """
            SELECT api_preferences
            FROM channel_settings
            WHERE channel_id = $1
              AND service_type = $2
            LIMIT 1
        """

        row = await db.fetchrow(query, channel_id, service_type)

        if row and row["api_preferences"]:
            preferences = json.loads(row["api_preferences"])
            provider = preferences.get(service_type)
            if provider:
                return APIProvider(provider)

        # Return defaults
        if service_type == ServiceType.VIEWS:
            return APIProvider.TWIBOOST
        elif service_type == ServiceType.REACTIONS:
            return APIProvider.NAKRUTOCHKA
        elif service_type == ServiceType.REPOSTS:
            return APIProvider.NAKRUTOCHKA

        return APIProvider.TWIBOOST

    async def get_api_preferences(self, channel_id: int) -> Dict[str, str]:
        """Get all API preferences for channel"""
        query = """
            SELECT service_type, api_preferences
            FROM channel_settings
            WHERE channel_id = $1
        """

        rows = await db.fetch(query, channel_id)

        result = {}
        for row in rows:
            if row["api_preferences"]:
                prefs = json.loads(row["api_preferences"])
                service_type = row["service_type"]
                if service_type in prefs:
                    result[service_type] = prefs[service_type]

        # Fill defaults
        if "views" not in result:
            result["views"] = "twiboost"
        if "reactions" not in result:
            result["reactions"] = "nakrutochka"
        if "reposts" not in result:
            result["reposts"] = "nakrutochka"

        return result

    async def get_service_ids_for_api(
        self,
        channel_id: int,
        service_type: str,
        api_provider: str
    ) -> Dict[str, int]:
        """Get service IDs for specific API"""
        if api_provider == "twiboost":
            column = "twiboost_service_ids"
        elif api_provider == "nakrutochka":
            column = "nakrutochka_service_ids"
        else:
            return {}

        query = f"""
            SELECT {column}
            FROM channel_settings
            WHERE channel_id = $1
              AND service_type = $2
            LIMIT 1
        """

        row = await db.fetchrow(query, channel_id, service_type)

        if row and row[column]:
            return json.loads(row[column])

        return {}

    async def bulk_create_default_settings(self, channel_id: int):
        """Create default settings for all service types with dual API support"""
        default_settings = [
            {
                "channel_id": channel_id,
                "service_type": ServiceType.VIEWS,
                "base_quantity": 1000,
                "randomization_percent": 0,
                "portions_count": 5,
                "fast_delivery_percent": 70,
                "api_preferences": {"views": "twiboost"}
            },
            {
                "channel_id": channel_id,
                "service_type": ServiceType.REACTIONS,
                "base_quantity": 100,
                "randomization_percent": 40,
                "reaction_distribution": {"👍": 45, "❤️": 30, "🔥": 25},
                "portions_count": 1,
                "drops_per_run": 5,
                "run_interval": 23,
                "api_preferences": {"reactions": "nakrutochka"}
            },
            {
                "channel_id": channel_id,
                "service_type": ServiceType.REPOSTS,
                "base_quantity": 50,
                "randomization_percent": 40,
                "portions_count": 1,
                "repost_delay_minutes": 5,
                "drops_per_run": 3,
                "run_interval": 34,
                "api_preferences": {"reposts": "nakrutochka"}
            }
        ]

        for settings in default_settings:
            await self.create_settings(settings)

        self.log_info("Default settings created with dual API support", channel_id=channel_id)

    async def migrate_to_dual_api(self):
        """Migrate existing channels to dual API support"""
        channels = await self.get_active_channels()

        for channel in channels:
            # Set default API preferences
            api_preferences = {
                "views": "twiboost",
                "reactions": "nakrutochka",
                "reposts": "nakrutochka"
            }

            await self.update_api_preferences(channel.id, api_preferences)

            self.log_info(
                f"Migrated channel {channel.id} to dual API support",
                channel_id=channel.id
            )

        self.log_info(f"Migrated {len(channels)} channels to dual API support")

    # ========== Statistics Methods ==========

    async def get_channel_statistics(self, channel_id: int) -> Dict[str, Any]:
        """Get detailed statistics for channel"""
        # Get post statistics
        post_query = """
            SELECT 
                COUNT(*) as total_posts,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_posts,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_posts,
                COUNT(CASE WHEN detected_at > NOW() - INTERVAL '24 hours' THEN 1 END) as posts_24h
            FROM posts
            WHERE channel_id = $1
        """
        post_stats = await db.fetchrow(post_query, channel_id)

        # Get order statistics by API
        order_query = """
            SELECT 
                o.api_provider,
                COUNT(*) as count,
                SUM(o.actual_quantity) as total_quantity,
                COUNT(CASE WHEN o.status = 'completed' THEN 1 END) as completed
            FROM orders o
            JOIN posts p ON o.post_id = p.id
            WHERE p.channel_id = $1
            GROUP BY o.api_provider
        """
        order_stats = await db.fetch(order_query, channel_id)

        api_stats = {}
        for row in order_stats:
            api_stats[row["api_provider"]] = {
                "count": row["count"],
                "total_quantity": row["total_quantity"],
                "completed": row["completed"]
            }

        return {
            "posts": {
                "total": post_stats["total_posts"],
                "completed": post_stats["completed_posts"],
                "failed": post_stats["failed_posts"],
                "last_24h": post_stats["posts_24h"]
            },
            "orders_by_api": api_stats
        }

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
            nakrutochka_service_ids=json.loads(row["nakrutochka_service_ids"]) if row["nakrutochka_service_ids"] else None,
            api_preferences=json.loads(row["api_preferences"]) if row["api_preferences"] else None,
            drops_per_run=row["drops_per_run"],
            run_interval=row["run_interval"],
            updated_at=row["updated_at"]
        )


# Global repository instance
channel_repo = ChannelRepository()