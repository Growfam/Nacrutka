"""
Database queries - SIMPLIFIED VERSION
"""
from typing import List, Optional, Dict, Any
from datetime import datetime
import json

from src.database.connection import DatabaseConnection
from src.database.models import *
from src.utils.logger import get_logger
from src.config import POST_STATUS, ORDER_STATUS, PORTION_STATUS

logger = get_logger(__name__)


class Queries:
    """Simplified database queries"""

    def __init__(self, db: DatabaseConnection):
        self.db = db

    # ============ CHANNELS ============

    async def get_active_channels(self) -> List[Channel]:
        """Get all active channels"""
        query = """
            SELECT * FROM channels
            WHERE is_active = true
            ORDER BY id
        """
        rows = await self.db.fetch(query)
        return [Channel.from_db_row(dict(row)) for row in rows]

    async def get_channel_by_id(self, channel_id: int) -> Optional[Channel]:
        """Get channel by ID"""
        query = "SELECT * FROM channels WHERE id = $1"
        row = await self.db.fetchrow(query, channel_id)
        return Channel.from_db_row(dict(row)) if row else None

    async def get_channel_settings(self, channel_id: int) -> Optional[ChannelSettings]:
        """Get channel settings"""
        query = "SELECT * FROM channel_settings WHERE channel_id = $1"
        row = await self.db.fetchrow(query, channel_id)

        if not row:
            return None

        settings = ChannelSettings.from_db_row(dict(row))

        # Load reaction services if needed
        if settings.reactions_target > 0:
            reaction_services = await self.get_reaction_services(channel_id)
            if reaction_services:
                settings._reaction_distribution = [
                    {
                        'service_id': rs.service_id,
                        'emoji': rs.emoji,
                        'quantity': rs.target_quantity
                    }
                    for rs in reaction_services
                ]

        return settings

    async def get_reaction_services(self, channel_id: int) -> List[ChannelReactionService]:
        """Get reaction services for channel"""
        query = """
            SELECT * FROM channel_reaction_services
            WHERE channel_id = $1
            ORDER BY id
        """
        rows = await self.db.fetch(query, channel_id)
        return [ChannelReactionService.from_db_row(dict(row)) for row in rows]

    async def get_channel_with_full_config(self, channel_id: int) -> Optional[Dict[str, Any]]:
        """Get complete channel configuration"""
        try:
            channel = await self.get_channel_by_id(channel_id)
            if not channel:
                return None

            settings = await self.get_channel_settings(channel_id)
            if not settings:
                return None

            reaction_services = await self.get_reaction_services(channel_id)
            templates = await self.get_all_portion_templates(channel_id)

            return {
                'channel': channel,
                'settings': settings,
                'reaction_services': reaction_services,
                'templates': templates,
                'channel_id': channel_id
            }

        except Exception as e:
            logger.error(f"Failed to get channel config: {e}")
            return None

    # ============ POSTS ============

    async def get_channel_posts(self, channel_id: int, limit: int = 100) -> List[int]:
        """Get recent post IDs for channel"""
        query = """
            SELECT post_id FROM posts
            WHERE channel_id = $1
            ORDER BY created_at DESC
            LIMIT $2
        """
        rows = await self.db.fetch(query, channel_id, limit)
        return [row['post_id'] for row in rows]

    async def create_post(
        self,
        channel_id: int,
        post_id: int,
        post_url: str
    ) -> Optional[int]:
        """Create new post record"""
        query = """
            INSERT INTO posts (channel_id, post_id, post_url, status)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (channel_id, post_id) DO NOTHING
            RETURNING id
        """
        result = await self.db.fetchval(
            query,
            channel_id,
            post_id,
            post_url,
            POST_STATUS["NEW"]
        )
        return result

    async def get_new_posts(self, limit: int = 10) -> List[Post]:
        """Get posts with 'new' status"""
        query = """
            SELECT p.* 
            FROM posts p
            JOIN channels c ON c.id = p.channel_id
            WHERE p.status = $1
            AND c.is_active = true
            ORDER BY p.created_at
            LIMIT $2
        """
        rows = await self.db.fetch(query, POST_STATUS["NEW"], limit)
        return [Post.from_db_row(dict(row)) for row in rows]

    async def update_post_status(
        self,
        post_id: int,
        status: str,
        processed_at: Optional[datetime] = None
    ):
        """Update post status"""
        if processed_at:
            query = """
                UPDATE posts 
                SET status = $1, processed_at = $2
                WHERE id = $3
            """
            await self.db.execute(query, status, processed_at, post_id)
        else:
            query = "UPDATE posts SET status = $1 WHERE id = $2"
            await self.db.execute(query, status, post_id)

    async def get_recent_failed_posts(self, hours: int = 24) -> List[Post]:
        """Get recently failed posts"""
        query = """
            SELECT * FROM posts
            WHERE status = 'failed'
            AND created_at > NOW() - INTERVAL '%s hours'
            AND published_at > NOW() - INTERVAL '48 hours'
            ORDER BY created_at DESC
        """
        rows = await self.db.fetch(query % hours)
        return [Post.from_db_row(dict(row)) for row in rows]

    # ============ ORDERS ============

    async def create_order(
        self,
        post_id: int,
        service_type: str,
        service_id: int,
        total_quantity: int,
        start_delay_minutes: int = 0
    ) -> int:
        """Create new order"""
        query = """
            INSERT INTO orders (
                post_id, service_type, service_id, 
                total_quantity, start_delay_minutes, status
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
        """
        order_id = await self.db.fetchval(
            query,
            post_id,
            service_type,
            service_id,
            total_quantity,
            start_delay_minutes,
            ORDER_STATUS["PENDING"]
        )
        return order_id

    async def update_order_nakrutka_id(self, order_id: int, nakrutka_id: str) -> bool:
        """Update order with Nakrutka order ID"""
        query = """
            UPDATE orders 
            SET nakrutka_order_id = $1,
                status = $2,
                started_at = $3
            WHERE id = $4
        """
        result = await self.db.execute(
            query,
            nakrutka_id,
            ORDER_STATUS["IN_PROGRESS"],
            datetime.utcnow(),
            order_id
        )

        success = result == "UPDATE 1"
        if success:
            logger.info(f"Updated order {order_id} with nakrutka_id {nakrutka_id}")
        else:
            logger.error(f"Failed to update order {order_id}")

        return success

    async def get_active_orders(self) -> List[Order]:
        """Get orders in progress"""
        query = """
            SELECT * FROM orders
            WHERE status IN ($1, $2)
            ORDER BY created_at
        """
        rows = await self.db.fetch(
            query,
            ORDER_STATUS["PENDING"],
            ORDER_STATUS["IN_PROGRESS"]
        )
        return [Order.from_db_row(dict(row)) for row in rows]

    async def update_order_status(
        self,
        order_id: int,
        status: str,
        completed_at: Optional[datetime] = None
    ):
        """Update order status"""
        if completed_at:
            query = """
                UPDATE orders 
                SET status = $1, completed_at = $2
                WHERE id = $3
            """
            await self.db.execute(query, status, completed_at, order_id)
        else:
            query = "UPDATE orders SET status = $1 WHERE id = $2"
            await self.db.execute(query, status, order_id)

    # ============ PORTIONS (DON'T TOUCH!) ============

    async def create_portions(self, portions: List[Dict[str, Any]]):
        """Create multiple order portions"""
        if not portions:
            return

        query = """
            INSERT INTO order_portions (
                order_id, portion_number, quantity_per_run,
                runs, interval_minutes, status, scheduled_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """

        for p in portions:
            await self.db.execute(
                query,
                p['order_id'],
                p['portion_number'],
                p['quantity_per_run'],
                p['runs'],
                p['interval_minutes'],
                PORTION_STATUS["WAITING"],
                p.get('scheduled_at')
            )

    async def create_portions_batch(self, order_id: int, portions: List[Dict[str, Any]]):
        """Create portions for a single order"""
        if not portions:
            return

        for portion in portions:
            portion['order_id'] = order_id

        await self.create_portions(portions)

    async def get_order_portions(self, order_id: int) -> List[OrderPortion]:
        """Get portions for order"""
        query = """
            SELECT * FROM order_portions
            WHERE order_id = $1
            ORDER BY portion_number
        """
        rows = await self.db.fetch(query, order_id)
        return [OrderPortion.from_db_row(dict(row)) for row in rows]

    async def update_portion_status(
        self,
        portion_id: int,
        status: str,
        nakrutka_id: Optional[str] = None
    ):
        """Update portion status"""
        if nakrutka_id:
            query = """
                UPDATE order_portions 
                SET status = $1, nakrutka_portion_id = $2, started_at = $3
                WHERE id = $4
            """
            await self.db.execute(
                query, status, nakrutka_id, datetime.utcnow(), portion_id
            )
        else:
            if status == PORTION_STATUS["COMPLETED"]:
                query = """
                    UPDATE order_portions 
                    SET status = $1, completed_at = $2
                    WHERE id = $3
                """
                await self.db.execute(query, status, datetime.utcnow(), portion_id)
            else:
                query = "UPDATE order_portions SET status = $1 WHERE id = $2"
                await self.db.execute(query, status, portion_id)

    async def get_scheduled_portions(self) -> List[OrderPortion]:
        """Get portions ready to be executed"""
        query = """
            SELECT op.* 
            FROM order_portions op
            JOIN orders o ON o.id = op.order_id
            WHERE op.status = $1 
            AND (op.scheduled_at IS NULL OR op.scheduled_at <= $2)
            AND o.status IN ($3, $4)
            ORDER BY op.scheduled_at
            LIMIT 50
        """
        rows = await self.db.fetch(
            query,
            PORTION_STATUS["WAITING"],
            datetime.utcnow(),
            ORDER_STATUS["PENDING"],
            ORDER_STATUS["IN_PROGRESS"]
        )
        return [OrderPortion.from_db_row(dict(row)) for row in rows]

    # ============ SERVICES ============

    async def refresh_service_cache(self) -> Dict[int, Service]:
        """Get all services"""
        query = """
            SELECT * FROM services
            WHERE is_active = true
            ORDER BY service_type, price_per_1000
        """
        rows = await self.db.fetch(query)

        services = {}
        for row in rows:
            service = Service.from_db_row(dict(row))
            services[service.nakrutka_id] = service

        logger.info(f"Loaded {len(services)} services")
        return services

    async def get_service(self, service_id: int) -> Optional[Service]:
        """Get service by ID"""
        query = """
            SELECT * FROM services
            WHERE id = $1 OR nakrutka_id = $1
            LIMIT 1
        """
        row = await self.db.fetchrow(query, service_id)
        return Service.from_db_row(dict(row)) if row else None

    async def get_services_by_type(self, service_type: str) -> List[Service]:
        """Get all active services by type"""
        query = """
            SELECT * FROM services
            WHERE service_type = $1 AND is_active = true
            ORDER BY price_per_1000
        """
        rows = await self.db.fetch(query, service_type)
        return [Service.from_db_row(dict(row)) for row in rows]

    async def get_cheapest_service(self, service_type: str) -> Optional[Service]:
        """Get cheapest service for type"""
        query = """
            SELECT * FROM services
            WHERE service_type = $1 AND is_active = true
            ORDER BY price_per_1000
            LIMIT 1
        """
        row = await self.db.fetchrow(query, service_type)
        return Service.from_db_row(dict(row)) if row else None

    async def find_service_by_emoji(self, emoji: str) -> Optional[Service]:
        """Find reaction service by emoji"""
        query = """
            SELECT * FROM services
            WHERE service_type = 'reactions'
            AND service_name LIKE $1
            AND is_active = true
            LIMIT 1
        """
        row = await self.db.fetchrow(query, f'%{emoji}%')
        return Service.from_db_row(dict(row)) if row else None

    async def optimize_service_selection(
        self,
        service_type: str,
        quantity: int,
        channel_id: Optional[int] = None
    ) -> Optional[Service]:
        """Simple service selection"""
        # Just get cheapest that fits quantity
        query = """
            SELECT * FROM services
            WHERE service_type = $1 
            AND is_active = true
            AND min_quantity <= $2
            AND max_quantity >= $2
            ORDER BY price_per_1000
            LIMIT 1
        """
        row = await self.db.fetchrow(query, service_type, quantity)
        return Service.from_db_row(dict(row)) if row else None

    # ============ PORTION TEMPLATES ============

    async def get_portion_templates(
        self,
        channel_id: int,
        service_type: str
    ) -> List[PortionTemplate]:
        """Get portion templates for channel and service type"""
        query = """
            SELECT * FROM portion_templates
            WHERE channel_id = $1 AND service_type = $2
            ORDER BY portion_number
        """
        rows = await self.db.fetch(query, channel_id, service_type)
        return [PortionTemplate.from_db_row(dict(row)) for row in rows]

    async def get_all_portion_templates(self, channel_id: int) -> Dict[str, List[PortionTemplate]]:
        """Get all portion templates for channel"""
        query = """
            SELECT * FROM portion_templates
            WHERE channel_id = $1
            ORDER BY service_type, portion_number
        """
        rows = await self.db.fetch(query, channel_id)

        templates = {}
        for row in rows:
            template = PortionTemplate.from_db_row(dict(row))
            if template.service_type not in templates:
                templates[template.service_type] = []
            templates[template.service_type].append(template)

        return templates

    # ============ HELPERS ============

    async def calculate_random_quantity(
        self,
        base_quantity: int,
        randomize_percent: int
    ) -> int:
        """Calculate random quantity using DB function"""
        query = "SELECT calculate_random_quantity($1, $2)"
        return await self.db.fetchval(query, base_quantity, randomize_percent)

    async def calculate_portion_details(
        self,
        channel_id: int,
        service_type: str,
        total_quantity: int
    ) -> List[Dict[str, Any]]:
        """Calculate portion details using DB function"""
        query = """
            SELECT * FROM calculate_portion_details($1, $2, $3)
        """
        rows = await self.db.fetch(query, channel_id, service_type, total_quantity)
        return [dict(row) for row in rows]

    # ============ LOGS ============

    async def create_log(self, level: str, message: str, context: Dict[str, Any]):
        """Create log entry"""
        query = """
            INSERT INTO logs (level, message, context)
            VALUES ($1, $2, $3)
        """
        await self.db.execute(query, level, message, json.dumps(context))

    async def cleanup_old_logs(self, days: int = 7) -> int:
        """Delete old log entries"""
        query = """
            DELETE FROM logs 
            WHERE created_at < NOW() - INTERVAL '%s days'
            RETURNING id
        """
        result = await self.db.fetch(query % days)
        return len(result)

    # ============ BASIC STATS ============

    async def get_monitoring_stats(self) -> Dict[str, Any]:
        """Get basic monitoring statistics"""
        query = """
            SELECT 
                (SELECT COUNT(*) FROM channels WHERE is_active = true) as active_channels,
                (SELECT COUNT(*) FROM posts WHERE created_at > NOW() - INTERVAL '24 hours') as posts_24h,
                (SELECT COUNT(*) FROM orders WHERE created_at > NOW() - INTERVAL '24 hours') as orders_24h,
                (SELECT COUNT(*) FROM orders WHERE status IN ('pending', 'in_progress')) as active_orders
        """
        row = await self.db.fetchrow(query)
        return dict(row) if row else {}