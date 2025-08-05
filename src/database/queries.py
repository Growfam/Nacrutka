"""
Database queries - Enhanced with dynamic service loading
"""
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
import json
from decimal import Decimal

from src.database.connection import DatabaseConnection
from src.database.models import *
from src.utils.logger import get_logger
from src.config import POST_STATUS, ORDER_STATUS, PORTION_STATUS

logger = get_logger(__name__)


class Queries:
    """Enhanced database queries with caching and dynamic services"""

    def __init__(self, db: DatabaseConnection):
        self.db = db
        self._service_cache = ServiceCache()

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

    async def get_channel_by_username(self, username: str) -> Optional[Channel]:
        """Get channel by username"""
        query = "SELECT * FROM channels WHERE channel_username = $1"
        row = await self.db.fetchrow(query, username)
        return Channel.from_db_row(dict(row)) if row else None

    async def get_channel_by_id(self, channel_id: int) -> Optional[Channel]:
        """Get channel by ID"""
        query = "SELECT * FROM channels WHERE id = $1"
        row = await self.db.fetchrow(query, channel_id)
        return Channel.from_db_row(dict(row)) if row else None

    async def get_channel_with_full_config(self, channel_id: int) -> Optional[Dict[str, Any]]:
        """Get channel with all related configuration"""
        channel = await self.get_channel_by_id(channel_id)
        if not channel:
            return None

        settings = await self.get_channel_settings(channel_id)
        reaction_services = await self.get_reaction_services(channel_id)
        templates = await self.get_all_portion_templates(channel_id)

        return {
            'channel': channel,
            'settings': settings,
            'reaction_services': reaction_services,
            'templates': templates
        }

    # ============ CHANNEL SETTINGS ============

    async def get_channel_settings(self, channel_id: int) -> Optional[ChannelSettings]:
        """Get channel settings with proper model"""
        query = "SELECT * FROM channel_settings WHERE channel_id = $1"
        row = await self.db.fetchrow(query, channel_id)
        return ChannelSettings.from_db_row(dict(row)) if row else None

    async def get_reaction_services(self, channel_id: int) -> List[ChannelReactionService]:
        """Get reaction services for channel"""
        query = """
            SELECT crs.*, s.service_name 
            FROM channel_reaction_services crs
            LEFT JOIN services s ON s.nakrutka_id = crs.service_id
            WHERE crs.channel_id = $1
            ORDER BY crs.id
        """
        rows = await self.db.fetch(query, channel_id)
        return [ChannelReactionService.from_db_row(dict(row)) for row in rows]

    async def update_channel_settings(
        self,
        channel_id: int,
        **kwargs
    ) -> bool:
        """Update channel settings dynamically"""
        if not kwargs:
            return False

        # Build update query
        set_clauses = []
        values = []
        for i, (key, value) in enumerate(kwargs.items(), 1):
            set_clauses.append(f"{key} = ${i}")
            values.append(value)

        query = f"""
            UPDATE channel_settings 
            SET {', '.join(set_clauses)}
            WHERE channel_id = ${len(values) + 1}
        """
        values.append(channel_id)

        try:
            await self.db.execute(query, *values)
            return True
        except Exception as e:
            logger.error(f"Failed to update channel settings: {e}")
            return False

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
            SELECT p.*, c.channel_username 
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

    async def get_post_by_id(self, post_id: int) -> Optional[Post]:
        """Get post by ID"""
        query = "SELECT * FROM posts WHERE id = $1"
        row = await self.db.fetchrow(query, post_id)
        return Post.from_db_row(dict(row)) if row else None

    async def get_recent_failed_posts(self, hours: int = 24) -> List[Post]:
        """Get recently failed posts"""
        query = """
            SELECT * FROM posts
            WHERE status = 'failed'
            AND created_at > NOW() - INTERVAL '%s hours'
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
        return [Order.from_db_row(dict(row)) for row in rows]

    async def get_order_by_id(self, order_id: int) -> Optional[Order]:
        """Get order by ID"""
        query = "SELECT * FROM orders WHERE id = $1"
        row = await self.db.fetchrow(query, order_id)
        return Order.from_db_row(dict(row)) if row else None

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

    async def get_orders_for_post(self, post_id: int) -> List[Order]:
        """Get all orders for a post"""
        query = """
            SELECT * FROM orders
            WHERE post_id = $1
            ORDER BY service_type
        """
        rows = await self.db.fetch(query, post_id)
        return [Order.from_db_row(dict(row)) for row in rows]

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
        return [OrderPortion.from_db_row(dict(row)) for row in rows]

    # ============ SERVICES (DYNAMIC) ============

    async def refresh_service_cache(self) -> Dict[int, Service]:
        """Refresh service cache from database"""
        if not self._service_cache.is_stale():
            return self._service_cache.services

        query = """
            SELECT * FROM services 
            WHERE is_active = true
            ORDER BY service_type, price_per_1000
        """
        rows = await self.db.fetch(query)

        self._service_cache.clear()
        for row in rows:
            service = Service.from_db_row(dict(row))
            self._service_cache.add_service(service)

        logger.info(f"Refreshed service cache with {len(rows)} services")
        return self._service_cache.services

    async def get_service(self, service_id: int) -> Optional[Service]:
        """Get service by Nakrutka ID with caching"""
        # Try cache first
        service = self._service_cache.get_service(service_id)
        if service:
            return service

        # Refresh cache and try again
        await self.refresh_service_cache()
        return self._service_cache.get_service(service_id)

    async def get_services_by_type(self, service_type: str) -> List[Service]:
        """Get all active services by type"""
        await self.refresh_service_cache()
        return [
            s for s in self._service_cache.services.values()
            if s.service_type == service_type
        ]

    async def get_cheapest_service(self, service_type: str) -> Optional[Service]:
        """Get cheapest service for type"""
        services = await self.get_services_by_type(service_type)
        if not services:
            return None
        return min(services, key=lambda s: float(s.price_per_1000))

    async def find_service_by_emoji(self, emoji: str) -> Optional[Service]:
        """Find reaction service by emoji"""
        await self.refresh_service_cache()

        for service in self._service_cache.services.values():
            if service.service_type != 'reactions':
                continue

            # Check if emoji is in service name
            if emoji in service.service_name:
                return service

        return None

    async def get_service_by_name_pattern(self, pattern: str) -> Optional[Service]:
        """Find service by name pattern"""
        await self.refresh_service_cache()

        pattern_lower = pattern.lower()
        for service in self._service_cache.services.values():
            if pattern_lower in service.service_name.lower():
                return service

        return None

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
        """Get all portion templates for channel grouped by type"""
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

    async def create_default_portion_templates(self, channel_id: int):
        """Create default portion templates for new channel"""
        templates = [
            # Views - 70/30 distribution
            ('views', 1, 29.13, 'quantity/134', 15, 0),
            ('views', 2, 22.50, 'quantity/115', 18, 0),
            ('views', 3, 17.04, 'quantity/98', 22, 0),
            ('views', 4, 13.09, 'quantity/86', 26, 0),
            ('views', 5, 18.24, 'quantity/23', 15, 156),

            # Reactions
            ('reactions', 1, 45.45, 'quantity/6', 12, 0),
            ('reactions', 2, 18.18, 'quantity/4', 15, 0),
            ('reactions', 3, 13.64, 'quantity/3', 18, 0),
            ('reactions', 4, 6.06, 'quantity/2', 20, 0),
            ('reactions', 5, 16.67, 'quantity/1', 15, 45),

            # Reposts
            ('reposts', 1, 26.09, 'quantity/2', 10, 5),
            ('reposts', 2, 17.39, 'quantity/2', 12, 5),
            ('reposts', 3, 13.04, 'quantity/1', 15, 5),
            ('reposts', 4, 8.70, 'quantity/1', 18, 5),
            ('reposts', 5, 34.78, 'quantity/2', 15, 45),
        ]

        query = """
            INSERT INTO portion_templates (
                channel_id, service_type, portion_number,
                quantity_percent, runs_formula, interval_minutes,
                start_delay_minutes
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING
        """

        for template in templates:
            await self.db.execute(query, channel_id, *template)

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

        return [Log.from_db_row(dict(row)) for row in rows]

    async def cleanup_old_logs(self, days: int = 7) -> int:
        """Delete old log entries"""
        query = """
            DELETE FROM logs 
            WHERE created_at < NOW() - INTERVAL '%s days'
            RETURNING id
        """
        result = await self.db.fetch(query % days)
        return len(result)

    # ============ STATISTICS ============

    async def get_channel_stats(self, channel_id: int) -> Dict[str, Any]:
        """Get channel statistics"""
        query = """
            WITH post_stats AS (
                SELECT 
                    COUNT(*) as total_posts,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_posts,
                    COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing_posts,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_posts,
                    COUNT(CASE WHEN created_at > NOW() - INTERVAL '24 hours' THEN 1 END) as posts_24h,
                    COUNT(CASE WHEN created_at > NOW() - INTERVAL '7 days' THEN 1 END) as posts_7d
                FROM posts
                WHERE channel_id = $1
            ),
            order_stats AS (
                SELECT 
                    COUNT(DISTINCT o.id) as total_orders,
                    SUM(o.total_quantity) FILTER (WHERE o.service_type = 'views') as total_views,
                    SUM(o.total_quantity) FILTER (WHERE o.service_type = 'reactions') as total_reactions,
                    SUM(o.total_quantity) FILTER (WHERE o.service_type = 'reposts') as total_reposts
                FROM orders o
                JOIN posts p ON p.id = o.post_id
                WHERE p.channel_id = $1
            )
            SELECT * FROM post_stats, order_stats
        """
        row = await self.db.fetchrow(query, channel_id)
        return dict(row) if row else {}

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

    async def get_service_performance(
        self,
        service_id: int,
        days: int = 7
    ) -> Dict[str, Any]:
        """Get performance metrics for a service"""
        query = """
            SELECT 
                COUNT(*) as total_orders,
                COUNT(*) FILTER (WHERE status = 'completed') as completed_orders,
                COUNT(*) FILTER (WHERE status = 'failed') as failed_orders,
                AVG(total_quantity) as avg_quantity,
                SUM(total_quantity) as total_quantity
            FROM orders
            WHERE service_id = $1
            AND created_at > NOW() - INTERVAL '%s days'
        """
        row = await self.db.fetchrow(query % days, service_id)

        if not row:
            return {}

        result = dict(row)
        total = result['total_orders']
        if total > 0:
            result['success_rate'] = result['completed_orders'] / total
            result['failure_rate'] = result['failed_orders'] / total
        else:
            result['success_rate'] = 0
            result['failure_rate'] = 0

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
                    COUNT(*) FILTER (WHERE is_active) as active_channels,
                    COUNT(*) as total_channels
                FROM channels
            ),
            post_stats AS (
                SELECT 
                    COUNT(*) as total_posts,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') as posts_24h,
                    COUNT(*) FILTER (WHERE status = 'processing') as processing_posts
                FROM posts
            ),
            order_stats AS (
                SELECT 
                    COUNT(*) as total_orders,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') as orders_24h,
                    COUNT(*) FILTER (WHERE status IN ('pending', 'in_progress')) as active_orders
                FROM orders
            ),
            cost_stats AS (
                SELECT 
                    COALESCE(SUM(o.total_quantity * s.price_per_1000 / 1000), 0) as cost_today
                FROM orders o
                JOIN services s ON s.nakrutka_id = o.service_id
                WHERE DATE(o.created_at) = CURRENT_DATE
            ),
            service_stats AS (
                SELECT COUNT(*) as active_services
                FROM services
                WHERE is_active = true
            )
            SELECT 
                cs.active_channels,
                cs.total_channels,
                ps.total_posts,
                ps.posts_24h,
                ps.processing_posts,
                os.total_orders,
                os.orders_24h,
                os.active_orders,
                cos.cost_today,
                ss.active_services
            FROM channel_stats cs, post_stats ps, order_stats os, cost_stats cos, service_stats ss
        """
        row = await self.db.fetchrow(query)
        return dict(row) if row else {}

    async def get_failed_orders(self, hours: int = 24) -> List[Order]:
        """Get recent failed orders"""
        query = """
            SELECT o.*, p.post_url, c.channel_username
            FROM orders o
            JOIN posts p ON p.id = o.post_id
            JOIN channels c ON c.id = p.channel_id
            WHERE o.status = 'failed'
            AND o.created_at > NOW() - INTERVAL '%s hours'
            ORDER BY o.created_at DESC
        """
        rows = await self.db.fetch(query % hours)
        return [Order.from_db_row(dict(row)) for row in rows]

    # ============ ADVANCED QUERIES ============

    async def find_best_performing_services(
        self,
        service_type: str,
        limit: int = 5
    ) -> List[Tuple[Service, Dict[str, Any]]]:
        """Find best performing services by success rate"""
        query = """
            WITH service_performance AS (
                SELECT 
                    s.*,
                    COUNT(o.id) as total_orders,
                    COUNT(o.id) FILTER (WHERE o.status = 'completed') as completed_orders,
                    AVG(o.total_quantity) as avg_quantity
                FROM services s
                LEFT JOIN orders o ON o.service_id = s.nakrutka_id
                WHERE s.service_type = $1
                AND s.is_active = true
                GROUP BY s.id
                HAVING COUNT(o.id) > 0
            )
            SELECT *
            FROM service_performance
            ORDER BY (completed_orders::float / total_orders) DESC
            LIMIT $2
        """
        rows = await self.db.fetch(query, service_type, limit)

        results = []
        for row in rows:
            data = dict(row)
            service_data = {k: v for k, v in data.items() if k in Service.__annotations__}
            service = Service.from_db_row(service_data)

            performance = {
                'total_orders': data['total_orders'],
                'completed_orders': data['completed_orders'],
                'success_rate': data['completed_orders'] / data['total_orders'],
                'avg_quantity': data['avg_quantity']
            }

            results.append((service, performance))

        return results

    async def get_channel_reaction_distribution(self, channel_id: int) -> Dict[str, int]:
        """Get actual reaction distribution for channel"""
        query = """
            SELECT 
                crs.emoji,
                crs.service_id,
                SUM(o.total_quantity) as total_used
            FROM channel_reaction_services crs
            JOIN orders o ON o.service_id = crs.service_id
            JOIN posts p ON p.id = o.post_id
            WHERE p.channel_id = $1
            AND o.service_type = 'reactions'
            AND o.status = 'completed'
            GROUP BY crs.emoji, crs.service_id
        """
        rows = await self.db.fetch(query, channel_id)
        return {row['emoji']: row['total_used'] for row in rows}

    async def optimize_service_selection(
        self,
        service_type: str,
        quantity: int
    ) -> Optional[Service]:
        """Select optimal service based on performance and price"""
        # Get services with performance
        services_perf = await self.find_best_performing_services(service_type, 10)

        if not services_perf:
            # Fallback to cheapest
            return await self.get_cheapest_service(service_type)

        # Score services by success rate and price
        best_service = None
        best_score = -1

        for service, performance in services_perf:
            # Skip if quantity out of bounds
            if not service.validate_quantity(quantity):
                continue

            # Calculate score (higher success rate, lower price = better)
            success_weight = 0.7
            price_weight = 0.3

            # Normalize price (inverse - lower is better)
            max_price = max(s[0].price_per_1000 for s in services_perf)
            price_score = 1 - (float(service.price_per_1000) / float(max_price))

            score = (
                success_weight * performance['success_rate'] +
                price_weight * price_score
            )

            if score > best_score:
                best_score = score
                best_service = service

        return best_service