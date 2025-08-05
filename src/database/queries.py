"""
Database queries - Complete implementation
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
    """Database queries collection"""

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
        return [Channel(**dict(row)) for row in rows]

    async def get_channel_by_username(self, username: str) -> Optional[Channel]:
        """Get channel by username"""
        query = "SELECT * FROM channels WHERE channel_username = $1"
        row = await self.db.fetchrow(query, username)
        return Channel(**dict(row)) if row else None

    async def get_channel_by_id(self, channel_id: int) -> Optional[Channel]:
        """Get channel by ID"""
        query = "SELECT * FROM channels WHERE id = $1"
        row = await self.db.fetchrow(query, channel_id)
        return Channel(**dict(row)) if row else None

    # ============ CHANNEL SETTINGS ============

    async def get_channel_settings(self, channel_id: int) -> Optional[ChannelSettings]:
        """Get channel settings"""
        query = "SELECT * FROM channel_settings WHERE channel_id = $1"
        row = await self.db.fetchrow(query, channel_id)
        if row:
            # Convert row to dict and handle JSON fields
            data = dict(row)
            if isinstance(data.get('reaction_types'), str):
                data['reaction_types'] = json.loads(data['reaction_types'])
            return ChannelSettings(**data)
        return None

    async def get_reaction_services(self, channel_id: int) -> List[Dict[str, Any]]:
        """Get reaction services for channel"""
        query = """
            SELECT * FROM channel_reaction_services
            WHERE channel_id = $1
            ORDER BY id
        """
        rows = await self.db.fetch(query, channel_id)
        return [dict(row) for row in rows]

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
            SELECT * FROM posts
            WHERE status = $1
            ORDER BY created_at
            LIMIT $2
        """
        rows = await self.db.fetch(query, POST_STATUS["NEW"], limit)
        return [Post(**dict(row)) for row in rows]

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

    async def get_post_by_id(self, post_id: int) -> Optional[Post]:
        """Get post by ID"""
        query = "SELECT * FROM posts WHERE id = $1"
        row = await self.db.fetchrow(query, post_id)
        return Post(**dict(row)) if row else None

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

    async def update_order_nakrutka_id(self, order_id: int, nakrutka_id: str):
        """Update order with Nakrutka order ID"""
        query = """
            UPDATE orders 
            SET nakrutka_order_id = $1,
                status = $2,
                started_at = $3
            WHERE id = $4
        """
        await self.db.execute(
            query,
            nakrutka_id,
            ORDER_STATUS["IN_PROGRESS"],
            datetime.utcnow(),
            order_id
        )

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
        return [Order(**dict(row)) for row in rows]

    async def get_order_by_id(self, order_id: int) -> Optional[Order]:
        """Get order by ID"""
        query = "SELECT * FROM orders WHERE id = $1"
        row = await self.db.fetchrow(query, order_id)
        return Order(**dict(row)) if row else None

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

    # ============ PORTIONS ============

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

        # Prepare data for bulk insert
        data = [
            (
                p['order_id'],
                p['portion_number'],
                p['quantity_per_run'],
                p['runs'],
                p['interval_minutes'],
                PORTION_STATUS["WAITING"],
                p['scheduled_at']
            )
            for p in portions
        ]

        # Bulk insert
        await self.db.executemany(query, data)

    async def get_order_portions(self, order_id: int) -> List[OrderPortion]:
        """Get portions for order"""
        query = """
            SELECT * FROM order_portions
            WHERE order_id = $1
            ORDER BY portion_number
        """
        rows = await self.db.fetch(query, order_id)
        return [OrderPortion(**dict(row)) for row in rows]

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
            query = "UPDATE order_portions SET status = $1 WHERE id = $2"
            await self.db.execute(query, status, portion_id)

    async def get_scheduled_portions(self) -> List[OrderPortion]:
        """Get portions ready to be executed"""
        query = """
            SELECT * FROM order_portions
            WHERE status = $1 
            AND scheduled_at <= $2
            ORDER BY scheduled_at
            LIMIT 50
        """
        rows = await self.db.fetch(
            query,
            PORTION_STATUS["WAITING"],
            datetime.utcnow()
        )
        return [OrderPortion(**dict(row)) for row in rows]

    # ============ SERVICES ============

    async def get_service(self, service_id: int) -> Optional[Service]:
        """Get service by Nakrutka ID"""
        query = """
            SELECT * FROM services 
            WHERE nakrutka_id = $1 AND is_active = true
        """
        row = await self.db.fetchrow(query, service_id)
        return Service(**dict(row)) if row else None

    async def get_services_by_type(self, service_type: str) -> List[Service]:
        """Get all active services by type"""
        query = """
            SELECT * FROM services
            WHERE service_type = $1 AND is_active = true
            ORDER BY price_per_1000
        """
        rows = await self.db.fetch(query, service_type)
        return [Service(**dict(row)) for row in rows]

    async def get_service_by_name(self, service_name: str) -> Optional[Service]:
        """Get service by name"""
        query = """
            SELECT * FROM services 
            WHERE service_name = $1 AND is_active = true
            LIMIT 1
        """
        row = await self.db.fetchrow(query, service_name)
        return Service(**dict(row)) if row else None

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
        return [PortionTemplate(**dict(row)) for row in rows]

    # ============ API KEYS ============

    async def get_api_key(self, service_name: str) -> Optional[str]:
        """Get active API key for service"""
        query = """
            SELECT api_key FROM api_keys
            WHERE service_name = $1 AND is_active = true
            LIMIT 1
        """
        return await self.db.fetchval(query, service_name)

    async def update_api_key(self, service_name: str, api_key: str):
        """Update API key"""
        query = """
            UPDATE api_keys 
            SET api_key = $1, created_at = $2
            WHERE service_name = $3
        """
        await self.db.execute(query, api_key, datetime.utcnow(), service_name)

    # ============ LOGS ============

    async def create_log(self, level: str, message: str, context: Dict[str, Any]):
        """Create log entry"""
        query = """
            INSERT INTO logs (level, message, context)
            VALUES ($1, $2, $3)
        """
        await self.db.execute(query, level, message, json.dumps(context))

    async def get_recent_logs(
        self,
        level: Optional[str] = None,
        limit: int = 100
    ) -> List[Log]:
        """Get recent logs"""
        if level:
            query = """
                SELECT * FROM logs 
                WHERE level = $1
                ORDER BY created_at DESC
                LIMIT $2
            """
            rows = await self.db.fetch(query, level, limit)
        else:
            query = """
                SELECT * FROM logs 
                ORDER BY created_at DESC
                LIMIT $1
            """
            rows = await self.db.fetch(query, limit)

        logs = []
        for row in rows:
            data = dict(row)
            if isinstance(data.get('context'), str):
                data['context'] = json.loads(data['context'])
            logs.append(Log(**data))
        return logs

    # ============ STATISTICS ============

    async def get_channel_stats(self, channel_id: int) -> Dict[str, Any]:
        """Get channel statistics"""
        query = """
            SELECT 
                COUNT(*) as total_posts,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_posts,
                COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing_posts,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_posts
            FROM posts
            WHERE channel_id = $1
        """
        row = await self.db.fetchrow(query, channel_id)
        return dict(row)

    async def get_today_costs(self) -> Dict[str, float]:
        """Get today's costs by service type"""
        query = """
            SELECT 
                o.service_type,
                SUM(o.total_quantity * s.price_per_1000 / 1000) as total_cost
            FROM orders o
            JOIN services s ON s.nakrutka_id = o.service_id
            WHERE DATE(o.created_at) = CURRENT_DATE
            GROUP BY o.service_type
        """
        rows = await self.db.fetch(query)
        return {row['service_type']: float(row['total_cost']) for row in rows}

    async def get_channel_costs(
        self,
        channel_id: int,
        days: int = 30
    ) -> Dict[str, Any]:
        """Get channel costs for period"""
        query = """
            SELECT 
                o.service_type,
                COUNT(o.id) as order_count,
                SUM(o.total_quantity) as total_quantity,
                SUM(o.total_quantity * s.price_per_1000 / 1000) as total_cost
            FROM orders o
            JOIN posts p ON p.id = o.post_id
            JOIN services s ON s.nakrutka_id = o.service_id
            WHERE p.channel_id = $1
            AND o.created_at > NOW() - INTERVAL '%s days'
            GROUP BY o.service_type
        """
        rows = await self.db.fetch(query % days, channel_id)

        result = {
            'by_type': {row['service_type']: {
                'count': row['order_count'],
                'quantity': row['total_quantity'],
                'cost': float(row['total_cost'])
            } for row in rows},
            'total_cost': sum(float(row['total_cost']) for row in rows)
        }
        return result

    # ============ HELPER FUNCTIONS ============

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

    # ============ MONITORING ============

    async def get_monitoring_stats(self) -> Dict[str, Any]:
        """Get overall monitoring statistics"""
        query = """
            WITH channel_stats AS (
                SELECT 
                    c.id,
                    c.channel_username,
                    c.is_active,
                    COUNT(DISTINCT p.id) as post_count,
                    COUNT(DISTINCT CASE WHEN p.created_at > NOW() - INTERVAL '24 hours' 
                          THEN p.id END) as posts_24h,
                    COUNT(DISTINCT CASE WHEN p.created_at > NOW() - INTERVAL '7 days' 
                          THEN p.id END) as posts_7d
                FROM channels c
                LEFT JOIN posts p ON p.channel_id = c.id
                GROUP BY c.id, c.channel_username, c.is_active
            ),
            order_stats AS (
                SELECT 
                    COUNT(*) as total_orders,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_orders,
                    COUNT(CASE WHEN created_at > NOW() - INTERVAL '24 hours' 
                          THEN 1 END) as orders_24h
                FROM orders
            ),
            cost_stats AS (
                SELECT 
                    SUM(o.total_quantity * s.price_per_1000 / 1000) as total_cost_today
                FROM orders o
                JOIN services s ON s.nakrutka_id = o.service_id
                WHERE DATE(o.created_at) = CURRENT_DATE
            )
            SELECT 
                (SELECT COUNT(*) FROM channels WHERE is_active = true) as active_channels,
                (SELECT COUNT(*) FROM channel_stats) as total_channels,
                (SELECT SUM(post_count) FROM channel_stats) as total_posts,
                (SELECT SUM(posts_24h) FROM channel_stats) as posts_24h,
                (SELECT SUM(posts_7d) FROM channel_stats) as posts_7d,
                (SELECT total_orders FROM order_stats) as total_orders,
                (SELECT completed_orders FROM order_stats) as completed_orders,
                (SELECT orders_24h FROM order_stats) as orders_24h,
                (SELECT COALESCE(total_cost_today, 0) FROM cost_stats) as cost_today
        """
        row = await self.db.fetchrow(query)
        return dict(row)

    # ============ MAINTENANCE ============

    async def cleanup_old_logs(self, days: int = 30):
        """Delete old log entries"""
        query = """
            DELETE FROM logs 
            WHERE created_at < NOW() - INTERVAL '%s days'
        """
        deleted = await self.db.execute(query % days)
        logger.info(f"Deleted {deleted} old log entries")
        return deleted

    async def get_failed_orders(self, hours: int = 24) -> List[Order]:
        """Get recent failed orders"""
        query = """
            SELECT * FROM orders
            WHERE status = 'failed'
            AND created_at > NOW() - INTERVAL '%s hours'
            ORDER BY created_at DESC
        """
        rows = await self.db.fetch(query % hours)
        return [Order(**dict(row)) for row in rows]