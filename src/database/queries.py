"""
Database queries - Enhanced with proper reaction services handling
"""
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
import json
from decimal import Decimal
import asyncio

from src.database.connection import DatabaseConnection
from src.database.models import *
from src.utils.logger import get_logger
from src.config import POST_STATUS, ORDER_STATUS, PORTION_STATUS

logger = get_logger(__name__)


class Queries:
    """Enhanced database queries with universal channel support"""

    def __init__(self, db: DatabaseConnection):
        self.db = db
        self._service_cache = ServiceCache()
        self._channel_config_cache: Dict[int, ChannelConfig] = {}
        self._cache_ttl = 300  # 5 minutes

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

    async def get_channel_config(self, channel_id: int, use_cache: bool = True) -> Optional[ChannelConfig]:
        """Get complete channel configuration with caching"""
        # Check cache
        if use_cache and channel_id in self._channel_config_cache:
            cached_config = self._channel_config_cache[channel_id]
            # Simple TTL check could be added here
            return cached_config

        # Get channel
        channel = await self.get_channel_by_id(channel_id)
        if not channel:
            return None

        # Get all related data in parallel
        settings_task = self.get_channel_settings(channel_id)
        reaction_services_task = self.get_reaction_services_with_details(channel_id)
        templates_task = self.get_all_portion_templates(channel_id)

        settings, reaction_services, templates = await asyncio.gather(
            settings_task,
            reaction_services_task,
            templates_task
        )

        if not settings:
            logger.warning(f"No settings found for channel {channel_id}")
            return None

        # Create config object
        config = ChannelConfig(
            channel=channel,
            settings=settings,
            reaction_services=reaction_services,
            portion_templates=templates
        )

        # Cache it
        self._channel_config_cache[channel_id] = config

        return config

    async def get_channel_with_full_config(self, channel_id: int) -> Optional[Dict[str, Any]]:
        """
        Get complete channel configuration including all related data

        Returns:
            Dict with channel, settings, reaction_services, templates
        """
        try:
            # Get channel
            channel = await self.get_channel_by_id(channel_id)
            if not channel:
                return None

            # Get settings
            settings = await self.get_channel_settings(channel_id)
            if not settings:
                logger.warning(f"No settings found for channel {channel_id}")
                return None

            # Get reaction services
            reaction_services = await self.get_reaction_services_with_details(channel_id)

            # Get templates
            templates = await self.get_all_portion_templates(channel_id)

            # Build complete config
            config = {
                'channel': channel,
                'settings': settings,
                'reaction_services': reaction_services,
                'templates': templates,
                'channel_id': channel_id
            }

            return config

        except Exception as e:
            logger.error(
                f"Failed to get full channel config",
                channel_id=channel_id,
                error=str(e),
                exc_info=True
            )
            return None

    async def invalidate_channel_cache(self, channel_id: int):
        """Invalidate channel configuration cache"""
        self._channel_config_cache.pop(channel_id, None)

    # ============ CHANNEL SETTINGS ============

    async def get_channel_settings(self, channel_id: int) -> Optional[ChannelSettings]:
        """Get channel settings with proper model"""
        query = "SELECT * FROM channel_settings WHERE channel_id = $1"
        row = await self.db.fetchrow(query, channel_id)

        if not row:
            return None

        settings = ChannelSettings.from_db_row(dict(row))

        # Load reaction distribution from channel_reaction_services if needed
        if settings.reactions_target > 0 and not settings._reaction_distribution:
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

    async def get_reaction_services_with_details(self, channel_id: int) -> List[ChannelReactionService]:
        """Get reaction services with service details"""
        query = """
            SELECT 
                crs.*,
                s.service_name,
                s.price_per_1000,
                s.min_quantity,
                s.max_quantity,
                s.is_active as service_active
            FROM channel_reaction_services crs
            LEFT JOIN services s ON s.nakrutka_id = crs.service_id
            WHERE crs.channel_id = $1
            ORDER BY crs.id
        """
        rows = await self.db.fetch(query, channel_id)

        reaction_services = []
        for row in rows:
            rs_data = {k: v for k, v in dict(row).items()
                      if k in ChannelReactionService.__annotations__}
            rs = ChannelReactionService.from_db_row(rs_data)

            # Set service details if available
            if row.get('service_name'):
                service_data = {
                    'id': 0,  # Placeholder
                    'nakrutka_id': rs.service_id,
                    'service_name': row['service_name'],
                    'service_type': 'reactions',
                    'price_per_1000': row['price_per_1000'],
                    'min_quantity': row['min_quantity'],
                    'max_quantity': row['max_quantity'],
                    'is_active': row['service_active'],
                    'updated_at': datetime.utcnow()
                }
                service = Service.from_db_row(service_data)
                rs.set_service(service)

            reaction_services.append(rs)

        return reaction_services

    async def update_channel_settings(self, channel_id: int, **kwargs) -> bool:
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
            await self.invalidate_channel_cache(channel_id)
            return True
        except Exception as e:
            logger.error(f"Failed to update channel settings: {e}")
            return False

    async def update_reaction_services(
        self,
        channel_id: int,
        reaction_distribution: List[Dict[str, Any]]
    ) -> bool:
        """Update reaction services for channel"""
        try:
            async with self.db.transaction() as conn:
                # Delete existing
                await conn.execute(
                    "DELETE FROM channel_reaction_services WHERE channel_id = $1",
                    channel_id
                )

                # Insert new
                for dist in reaction_distribution:
                    await conn.execute(
                        """
                        INSERT INTO channel_reaction_services 
                        (channel_id, service_id, emoji, target_quantity)
                        VALUES ($1, $2, $3, $4)
                        """,
                        channel_id,
                        dist['service_id'],
                        dist.get('emoji', 'unknown'),
                        dist['quantity']
                    )

                await self.invalidate_channel_cache(channel_id)
                return True

        except Exception as e:
            logger.error(f"Failed to update reaction services: {e}")
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
        """Get posts with 'new' status from active channels"""
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

    async def get_post_by_id(self, post_id: int) -> Optional[Post]:
        """Get post by ID"""
        query = "SELECT * FROM posts WHERE id = $1"
        row = await self.db.fetchrow(query, post_id)
        return Post.from_db_row(dict(row)) if row else None

    async def get_recent_failed_posts(self, hours: int = 24) -> List[Post]:
        """Get recently failed posts that can be retried"""
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

    async def create_order_with_validation(
        self,
        request: OrderRequest
    ) -> Optional[int]:
        """Create order with validation"""
        # Validate request
        if not request.is_valid():
            logger.error(
                "Invalid order request",
                errors=request.get_validation_errors()
            )
            return None

        # Use adjusted quantity if available
        quantity = request.get_final_quantity()

        # Create order
        order_id = await self.create_order(
            post_id=request.post_id,
            service_type=request.service_type.value,
            service_id=request.service_id,
            total_quantity=quantity,
            start_delay_minutes=request.start_delay_minutes
        )

        # Create portions
        if order_id and request.portions:
            await self.create_portions_batch(order_id, request.portions)

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
        completed_at: Optional[datetime] = None,
        actual_quantity: Optional[int] = None,
        cost: Optional[Decimal] = None
    ):
        """Update order status with additional info"""
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

        # Store additional metadata if needed
        # Could add columns for actual_quantity and cost

    async def get_orders_for_post(self, post_id: int) -> List[Order]:
        """Get all orders for a post"""
        query = """
            SELECT * FROM orders
            WHERE post_id = $1
            ORDER BY service_type
        """
        rows = await self.db.fetch(query, post_id)
        return [Order.from_db_row(dict(row)) for row in rows]

    async def get_order_history(
        self,
        channel_id: int,
        days: int = 30,
        service_type: Optional[str] = None
    ) -> List[Order]:
        """Get order history for channel"""
        query = """
            SELECT o.* 
            FROM orders o
            JOIN posts p ON p.id = o.post_id
            WHERE p.channel_id = $1
            AND o.created_at > NOW() - INTERVAL '%s days'
        """

        if service_type:
            query += " AND o.service_type = $2"
            rows = await self.db.fetch(query % days, channel_id, service_type)
        else:
            rows = await self.db.fetch(query % days, channel_id)

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
                p.get('scheduled_at')
            )
            for p in portions
        ]

        # Bulk insert
        await self.db.executemany(query, data)

    async def create_portions_batch(self, order_id: int, portions: List[Dict[str, Any]]):
        """Create portions for a single order"""
        if not portions:
            return

        # Add order_id to each portion
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

    # ============ SERVICES (ENHANCED) ============

    async def refresh_service_cache(self) -> Dict[int, Service]:
        """Refresh service cache from database"""
        if not self._service_cache.is_stale():
            return self._service_cache.services

        query = """
            WITH service_stats AS (
                SELECT 
                    service_id,
                    COUNT(*) FILTER (WHERE status = 'completed') as completed_count,
                    COUNT(*) as total_count
                FROM orders
                WHERE created_at > NOW() - INTERVAL '30 days'
                GROUP BY service_id
            )
            SELECT 
                s.*,
                COALESCE(ss.completed_count, 0) as completed_orders,
                COALESCE(ss.total_count, 0) as total_orders
            FROM services s
            LEFT JOIN service_stats ss ON ss.service_id = s.nakrutka_id
            WHERE s.is_active = true
            ORDER BY s.service_type, s.price_per_1000
        """
        rows = await self.db.fetch(query)

        self._service_cache.clear()
        for row in rows:
            service = Service.from_db_row(dict(row))

            # Calculate performance score
            if row['total_orders'] > 0:
                success_rate = row['completed_orders'] / row['total_orders']
                service.set_performance_score(success_rate)

            self._service_cache.add_service(service)

        logger.info(f"Refreshed service cache with {len(rows)} services")
        return self._service_cache.services

    async def get_service(self, service_id: int) -> Optional[Service]:
        """Get service by ID (works with both id and nakrutka_id)"""
        # Спочатку шукаємо в кеші
        service = self._service_cache.get_service(service_id)
        if service:
            return service

        # Якщо не знайшли - шукаємо в БД
        query = """
                SELECT * \
                FROM services
                WHERE id = $1 \
                   OR nakrutka_id = $1 LIMIT 1 \
                """
        row = await self.db.fetchrow(query, service_id)

        if row:
            service = Service.from_db_row(dict(row))
            # Додаємо в кеш
            self._service_cache.add_service(service)
            return service

        # Оновлюємо кеш і пробуємо ще раз
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

    async def find_best_service(
        self,
        service_type: str,
        quantity: int,
        preferred_features: Optional[List[str]] = None
    ) -> Optional[Service]:
        """Find best service based on multiple criteria"""
        services = await self.get_services_by_type(service_type)
        if not services:
            return None

        # Filter by quantity limits
        valid_services = [
            s for s in services
            if s.validate_quantity(quantity)
        ]

        if not valid_services:
            # Adjust quantity and try again
            valid_services = [
                s for s in services
                if quantity >= s.min_quantity
            ]

        if not valid_services:
            return None

        # Score services
        scored_services = []
        for service in valid_services:
            score = service.get_performance_score() * 0.5  # Base performance

            # Price score (lower is better)
            min_price = min(s.price_per_1000 for s in valid_services)
            price_score = float(min_price) / float(service.price_per_1000)
            score += price_score * 0.3

            # Feature matching
            if preferred_features:
                feature_score = 0
                for feature in preferred_features:
                    if feature.lower() in service.service_name.lower():
                        feature_score += 0.1
                score += min(feature_score, 0.2)

            scored_services.append((service, score))

        # Sort by score
        scored_services.sort(key=lambda x: x[1], reverse=True)

        return scored_services[0][0] if scored_services else None

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

    async def update_portion_template(
        self,
        channel_id: int,
        service_type: str,
        portion_number: int,
        **updates
    ) -> bool:
        """Update specific portion template"""
        if not updates:
            return False

        set_clauses = []
        values = []
        for i, (key, value) in enumerate(updates.items(), 1):
            set_clauses.append(f"{key} = ${i}")
            values.append(value)

        query = f"""
            UPDATE portion_templates
            SET {', '.join(set_clauses)}
            WHERE channel_id = ${len(values) + 1}
            AND service_type = ${len(values) + 2}
            AND portion_number = ${len(values) + 3}
        """
        values.extend([channel_id, service_type, portion_number])

        try:
            result = await self.db.execute(query, *values)
            await self.invalidate_channel_cache(channel_id)
            return result == "UPDATE 1"
        except Exception as e:
            logger.error(f"Failed to update portion template: {e}")
            return False

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
            INSERT INTO api_keys (service_name, api_key, is_active)
            VALUES ($1, $2, true)
            ON CONFLICT (service_name) 
            DO UPDATE SET api_key = $2, created_at = $3
        """
        await self.db.execute(query, service_name, api_key, datetime.utcnow())

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
        limit: int = 100,
        channel_id: Optional[int] = None
    ) -> List[Log]:
        """Get recent logs with filters"""
        query = "SELECT * FROM logs WHERE 1=1"
        params = []
        param_count = 0

        if level:
            param_count += 1
            query += f" AND level = ${param_count}"
            params.append(level)

        if channel_id:
            param_count += 1
            query += f" AND context->>'channel_id' = ${param_count}::text"
            params.append(str(channel_id))

        query += f" ORDER BY created_at DESC LIMIT ${param_count + 1}"
        params.append(limit)

        rows = await self.db.fetch(query, *params)
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

    # ============ STATISTICS (ENHANCED) ============

    async def get_channel_stats(self, channel_id: int) -> ChannelStats:
        """Get comprehensive channel statistics"""
        query = """
            WITH post_stats AS (
                SELECT 
                    COUNT(*) as total_posts,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_posts,
                    COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing_posts,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_posts,
                    COUNT(CASE WHEN created_at > NOW() - INTERVAL '24 hours' THEN 1 END) as posts_24h,
                    COUNT(CASE WHEN created_at > NOW() - INTERVAL '7 days' THEN 1 END) as posts_7d,
                    MAX(created_at) as last_post_date
                FROM posts
                WHERE channel_id = $1
            ),
            order_stats AS (
                SELECT 
                    COUNT(DISTINCT o.id) as total_orders,
                    SUM(o.total_quantity) FILTER (WHERE o.service_type = 'views') as total_views,
                    SUM(o.total_quantity) FILTER (WHERE o.service_type = 'reactions') as total_reactions,
                    SUM(o.total_quantity) FILTER (WHERE o.service_type = 'reposts') as total_reposts,
                    SUM(o.total_quantity * s.price_per_1000 / 1000) as total_cost,
                    AVG(EXTRACT(EPOCH FROM (o.completed_at - o.started_at))/60) 
                        FILTER (WHERE o.completed_at IS NOT NULL) as avg_processing_time
                FROM orders o
                JOIN posts p ON p.id = o.post_id
                LEFT JOIN services s ON s.nakrutka_id = o.service_id
                WHERE p.channel_id = $1
            ),
            channel_info AS (
                SELECT channel_username FROM channels WHERE id = $1
            )
            SELECT 
                $1 as channel_id,
                ci.channel_username,
                COALESCE(ps.total_posts, 0) as total_posts,
                COALESCE(ps.completed_posts, 0) as completed_posts,
                COALESCE(ps.processing_posts, 0) as processing_posts,
                COALESCE(ps.failed_posts, 0) as failed_posts,
                COALESCE(os.total_views, 0) as total_views,
                COALESCE(os.total_reactions, 0) as total_reactions,
                COALESCE(os.total_reposts, 0) as total_reposts,
                COALESCE(os.total_cost, 0) as total_cost,
                COALESCE(os.avg_processing_time, 0) as avg_processing_time,
                ps.last_post_date
            FROM channel_info ci, post_stats ps, order_stats os
        """

        row = await self.db.fetchrow(query, channel_id)
        if not row:
            # Return empty stats
            return ChannelStats(
                channel_id=channel_id,
                channel_username='unknown'
            )

        return ChannelStats.from_db_row(dict(row))

    async def get_service_stats(
        self,
        service_id: int,
        days: int = 30
    ) -> ServiceStats:
        """Get service performance statistics"""
        service = await self.get_service(service_id)
        if not service:
            raise ValueError(f"Service {service_id} not found")

        query = """
            SELECT 
                COUNT(*) as total_orders,
                COUNT(*) FILTER (WHERE status = 'completed') as completed_orders,
                COUNT(*) FILTER (WHERE status IN ('failed', 'cancelled')) as failed_orders,
                SUM(total_quantity) as total_quantity,
                SUM(total_quantity * $2 / 1000) as total_cost,
                AVG(EXTRACT(EPOCH FROM (completed_at - started_at))/60) 
                    FILTER (WHERE completed_at IS NOT NULL) as avg_completion_time,
                MAX(created_at) as last_used
            FROM orders
            WHERE service_id = $1
            AND created_at > NOW() - INTERVAL '%s days'
        """

        row = await self.db.fetchrow(query % days, service_id, float(service.price_per_1000))

        if not row:
            return ServiceStats(
                service_id=service_id,
                service_name=service.service_name,
                service_type=ServiceType(service.service_type)
            )

        stats_data = dict(row)

        # Calculate success rate
        if stats_data['total_orders'] > 0:
            stats_data['success_rate'] = (
                stats_data['completed_orders'] / stats_data['total_orders'] * 100
            )
        else:
            stats_data['success_rate'] = 0.0

        return ServiceStats.from_db_data(service, stats_data)

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
                SUM(total_quantity) as total_quantity,
                MIN(created_at) as first_order,
                MAX(created_at) as last_order
            FROM orders
            WHERE service_id = $1
            AND created_at > NOW() - INTERVAL '%s days'
        """
        row = await self.db.fetchrow(query % days, service_id)

        if not row or row['total_orders'] == 0:
            return {
                'total_orders': 0,
                'success_rate': 0,
                'failure_rate': 0
            }

        result = dict(row)
        total = result['total_orders']
        result['success_rate'] = result['completed_orders'] / total
        result['failure_rate'] = result['failed_orders'] / total

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

    async def get_failed_orders(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent failed orders with details"""
        query = """
            SELECT 
                o.*,
                p.post_url,
                c.channel_username,
                s.service_name
            FROM orders o
            JOIN posts p ON p.id = o.post_id
            JOIN channels c ON c.id = p.channel_id
            LEFT JOIN services s ON s.nakrutka_id = o.service_id
            WHERE o.status IN ('failed', 'cancelled')
            AND o.created_at > NOW() - INTERVAL '%s hours'
            ORDER BY o.created_at DESC
        """
        rows = await self.db.fetch(query % hours)

        results = []
        for row in rows:
            order = Order.from_db_row(dict(row))
            results.append({
                'order': order,
                'post_url': row['post_url'],
                'channel_username': row['channel_username'],
                'service_name': row['service_name']
            })

        return results

    # ============ ADVANCED QUERIES ============

    async def find_best_performing_services(
        self,
        service_type: str,
        limit: int = 5
    ) -> List[Tuple[Service, ServiceStats]]:
        """Find best performing services by success rate"""
        query = """
            WITH service_performance AS (
                SELECT 
                    s.*,
                    COUNT(o.id) as total_orders,
                    COUNT(o.id) FILTER (WHERE o.status = 'completed') as completed_orders,
                    COUNT(o.id) FILTER (WHERE o.status IN ('failed', 'cancelled')) as failed_orders,
                    SUM(o.total_quantity) as total_quantity,
                    SUM(o.total_quantity * s.price_per_1000 / 1000) as total_cost,
                    AVG(EXTRACT(EPOCH FROM (o.completed_at - o.started_at))/60) 
                        FILTER (WHERE o.completed_at IS NOT NULL) as avg_completion_time,
                    MAX(o.created_at) as last_used
                FROM services s
                LEFT JOIN orders o ON o.service_id = s.nakrutka_id
                    AND o.created_at > NOW() - INTERVAL '30 days'
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

            # Extract service data
            service_data = {
                k: v for k, v in data.items()
                if k in Service.__annotations__
            }
            service = Service.from_db_row(service_data)

            # Create stats
            stats_data = {
                'total_orders': data['total_orders'],
                'completed_orders': data['completed_orders'],
                'failed_orders': data['failed_orders'],
                'total_quantity': data['total_quantity'],
                'total_cost': data['total_cost'],
                'avg_completion_time': data['avg_completion_time'] or 0,
                'last_used': data['last_used'],
                'success_rate': (
                    data['completed_orders'] / data['total_orders'] * 100
                    if data['total_orders'] > 0 else 0
                )
            }

            stats = ServiceStats.from_db_data(service, stats_data)
            results.append((service, stats))

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
        quantity: int,
        channel_id: Optional[int] = None
    ) -> Optional[Service]:
        """Select optimal service based on performance and price"""
        # Get channel preferences if available
        preferred_features = []
        if channel_id:
            config = await self.get_channel_config(channel_id)
            if config and config.settings:
                # Could add preferred features based on channel settings
                pass

        # Find best service
        return await self.find_best_service(
            service_type=service_type,
            quantity=quantity,
            preferred_features=preferred_features
        )

    async def get_processing_queue_status(self) -> Dict[str, Any]:
        """Get current processing queue status"""
        query = """
            WITH queue_stats AS (
                SELECT 
                    COUNT(*) FILTER (WHERE status = 'new') as new_posts,
                    COUNT(*) FILTER (WHERE status = 'processing') as processing_posts,
                    MIN(created_at) FILTER (WHERE status = 'new') as oldest_new_post
                FROM posts
                WHERE status IN ('new', 'processing')
            ),
            active_portions AS (
                SELECT COUNT(*) as active_portions
                FROM order_portions
                WHERE status IN ('waiting', 'running')
            )
            SELECT 
                qs.*,
                ap.active_portions
            FROM queue_stats qs, active_portions ap
        """

        row = await self.db.fetchrow(query)
        if not row:
            return {
                'new_posts': 0,
                'processing_posts': 0,
                'active_portions': 0,
                'queue_age_minutes': 0
            }

        result = dict(row)

        # Calculate queue age
        if result['oldest_new_post']:
            age = datetime.utcnow() - result['oldest_new_post']
            result['queue_age_minutes'] = int(age.total_seconds() / 60)
        else:
            result['queue_age_minutes'] = 0

        return result

    # ============ CACHE MANAGEMENT ============

    def clear_all_caches(self):
        """Clear all internal caches"""
        self._service_cache.clear()
        self._channel_config_cache.clear()
        logger.info("All caches cleared")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'service_cache_size': len(self._service_cache.services),
            'channel_config_cache_size': len(self._channel_config_cache),
            'service_cache_age': (
                (datetime.utcnow() - self._service_cache.last_updated).total_seconds()
                if self._service_cache.last_updated else 0
            )
        }


# ============ CACHE MODELS ============

@dataclass
class ServiceCache:
    """Cache for services to avoid frequent DB queries"""
    services: Dict[NakrutkaServiceID, Service] = field(default_factory=dict)
    last_updated: datetime = field(default_factory=datetime.utcnow)

    def add_service(self, service: Service):
        """Add service to cache"""
        self.services[service.nakrutka_id] = service

    def get_service(self, nakrutka_id: NakrutkaServiceID) -> Optional[Service]:
        """Get service from cache"""
        return self.services.get(nakrutka_id)

    def is_stale(self, max_age_minutes: int = 60) -> bool:
        """Check if cache is stale"""
        age = datetime.utcnow() - self.last_updated
        return age.total_seconds() > max_age_minutes * 60

    def clear(self):
        """Clear cache"""
        self.services.clear()
        self.last_updated = datetime.utcnow()