"""
Post processing service - SIMPLIFIED VERSION
"""
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import asyncio
from decimal import Decimal

from src.database.connection import DatabaseConnection
from src.database.queries import Queries
from src.database.models import *
from src.services.nakrutka import NakrutkaClient, NakrutkaError
from src.services.portion_calculator import PortionCalculator, ReactionDistributor
from src.config import POST_STATUS, ORDER_STATUS, PORTION_STATUS
from src.utils.logger import get_logger, DatabaseLogger
from src.utils.validators import DataValidator

logger = get_logger(__name__)


class PostProcessor:
    """Simplified post processor"""

    def __init__(self, db: DatabaseConnection, nakrutka: NakrutkaClient):
        self.db = db
        self.nakrutka = nakrutka
        self.queries = Queries(db)
        self.db_logger = DatabaseLogger(db)
        self._processing_lock = asyncio.Lock()
        self._channel_configs_cache = {}
        self._cache_ttl = 300  # 5 minutes

    async def process_new_posts(self):
        """Process all posts with 'new' status"""
        async with self._processing_lock:
            try:
                logger.info("Starting process_new_posts")

                # Get new posts
                new_posts = await self.queries.get_new_posts(limit=10)
                if not new_posts:
                    logger.debug("No new posts to process")
                    return

                logger.info(f"Processing {len(new_posts)} new posts")

                # Process each post
                for post in new_posts:
                    try:
                        # Get channel config
                        config = await self._get_channel_config(post.channel_id)
                        if not config:
                            logger.error(f"No configuration for channel {post.channel_id}")
                            await self.queries.update_post_status(post.id, POST_STATUS["FAILED"])
                            continue

                        # Process post
                        await self._process_single_post(post, config)
                        await asyncio.sleep(2)  # Rate limiting

                    except Exception as e:
                        logger.error(f"Failed to process post {post.id}: {e}")
                        await self.queries.update_post_status(post.id, POST_STATUS["FAILED"])
                        await self.db_logger.error(
                            "Post processing failed",
                            post_id=post.id,
                            error=str(e)
                        )

            except Exception as e:
                logger.error(f"Post processing job failed: {e}")

    async def _get_channel_config(self, channel_id: int) -> Optional[Dict[str, Any]]:
        """Get channel configuration with caching"""
        # Check cache
        if channel_id in self._channel_configs_cache:
            cached_time, config = self._channel_configs_cache[channel_id]
            if (datetime.utcnow() - cached_time).total_seconds() < self._cache_ttl:
                return config

        # Load from database
        config = await self.queries.get_channel_with_full_config(channel_id)

        if config:
            # Cache it
            self._channel_configs_cache[channel_id] = (datetime.utcnow(), config)

        return config

    async def _process_single_post(self, post: Post, config: Dict[str, Any]):
        """Process single post"""
        logger.info(f"Processing post {post.post_id} from channel {post.channel_id}")

        # Extract configuration
        channel = config['channel']
        settings = config['settings']
        reaction_services = config.get('reaction_services', [])
        templates = config['templates']

        # Basic validation
        if not settings:
            raise Exception("No settings for channel")

        # Calculate quantities with randomization
        quantities = await self._calculate_quantities(settings)

        # Log calculated quantities
        await self.db_logger.info(
            "Calculated quantities for post",
            post_id=post.id,
            channel=channel.channel_username,
            views=quantities['views'],
            reactions=quantities['reactions'],
            reposts=quantities['reposts']
        )

        # Update post status to processing
        await self.db.execute(
            "UPDATE posts SET status = $1, processed_at = $2 WHERE id = $3",
            POST_STATUS["PROCESSING"],
            datetime.utcnow(),
            post.id
        )

        # Process each service type
        results = await self._process_all_service_types(
            post=post,
            config=config,
            quantities=quantities
        )

        # Update post status based on results
        success_count = sum(1 for r in results.values() if r and r.get('success'))

        if success_count > 0:
            status = POST_STATUS["COMPLETED"]
        else:
            status = POST_STATUS["FAILED"]

        await self.db.execute(
            "UPDATE posts SET status = $1 WHERE id = $2",
            status,
            post.id
        )

    async def _calculate_quantities(self, settings: ChannelSettings) -> Dict[str, int]:
        """Calculate quantities with randomization"""
        # Views - no randomization
        views = settings.views_target

        # Reactions - with randomization if enabled
        if settings.randomize_reactions and settings.reactions_target > 0:
            reactions = await self.queries.calculate_random_quantity(
                settings.reactions_target,
                settings.randomize_percent
            )
        else:
            reactions = settings.reactions_target

        # Reposts - with randomization if enabled
        if settings.randomize_reposts and settings.reposts_target > 0:
            reposts = await self.queries.calculate_random_quantity(
                settings.reposts_target,
                settings.randomize_percent
            )
        else:
            reposts = settings.reposts_target

        return {
            'views': views,
            'reactions': reactions,
            'reposts': reposts
        }

    async def _process_all_service_types(
        self,
        post: Post,
        config: Dict[str, Any],
        quantities: Dict[str, int]
    ) -> Dict[str, Any]:
        """Process all service types for a post"""
        settings = config['settings']
        templates = config['templates']
        reaction_services = config.get('reaction_services', [])

        results = {}

        # Process views
        if quantities['views'] > 0 and templates.get('views'):
            logger.info(f"Processing views: {quantities['views']}")
            results['views'] = await self._create_service_order(
                post=post,
                service_type='views',
                quantity=quantities['views'],
                templates=templates['views'],
                service_id=settings.views_service_id
            )

        # Process reactions
        if quantities['reactions'] > 0:
            logger.info(f"Processing reactions: {quantities['reactions']}")
            if reaction_services and len(reaction_services) > 1:
                # Multiple reaction types - distribute
                results['reactions'] = await self._create_distributed_reactions(
                    post=post,
                    total_quantity=quantities['reactions'],
                    reaction_services=reaction_services,
                    templates=templates.get('reactions', [])
                )
            else:
                # Single reaction service
                results['reactions'] = await self._create_service_order(
                    post=post,
                    service_type='reactions',
                    quantity=quantities['reactions'],
                    templates=templates.get('reactions', []),
                    service_id=settings.reactions_service_id
                )

        # Process reposts - SINGLE PORTION ONLY
        if quantities['reposts'] > 0:
            logger.info(f"Processing reposts: {quantities['reposts']} (single portion)")
            results['reposts'] = await self._create_service_order(
                post=post,
                service_type='reposts',
                quantity=quantities['reposts'],
                templates=None,  # Force single portion
                service_id=settings.reposts_service_id,
                force_single_portion=True
            )

        return results

    async def _create_service_order(
        self,
        post: Post,
        service_type: str,
        quantity: int,
        templates: List[PortionTemplate],
        service_id: Optional[int] = None,
        delay_minutes: int = 0,
        force_single_portion: bool = False
    ) -> Dict[str, Any]:
        """Create order for a service type"""
        if quantity <= 0:
            return {'success': True, 'skipped': True}

        try:
            logger.info(f"Creating {service_type} order, quantity={quantity}")

            # Get service
            if not service_id:
                service = await self.queries.get_cheapest_service(service_type)
                if not service:
                    raise Exception(f"No service found for {service_type}")
            else:
                service = await self.queries.get_service(service_id)
                if not service:
                    service = await self.queries.get_cheapest_service(service_type)
                    if not service:
                        raise Exception(f"Service {service_id} not found")

            # Adjust quantity for service limits
            if quantity < service.min_quantity:
                quantity = service.min_quantity
            elif quantity > service.max_quantity:
                quantity = service.max_quantity

            # Create order record
            order_id = await self.queries.create_order(
                post_id=post.id,
                service_type=service_type,
                service_id=service.nakrutka_id,
                total_quantity=quantity,
                start_delay_minutes=delay_minutes
            )

            logger.info(f"Created order record: {order_id}")

            # Calculate portions
            if force_single_portion or service_type == 'reposts':
                portions = [{
                    'portion_number': 1,
                    'quantity_per_run': quantity,
                    'runs': 1,
                    'interval_minutes': 0,
                    'scheduled_at': datetime.utcnow() + timedelta(minutes=delay_minutes),
                    'total_quantity': quantity
                }]
            else:
                calculator = PortionCalculator(templates) if templates else PortionCalculator()
                portions = calculator.calculate_portions(
                    total_quantity=quantity,
                    service=service,
                    start_time=datetime.utcnow() + timedelta(minutes=delay_minutes)
                )

            # Send to Nakrutka - EACH PORTION SEPARATELY
            nakrutka_results = await self._send_portions_to_nakrutka(
                order_id=order_id,
                service=service,
                link=post.post_url,
                portions=portions
            )

            # Calculate cost
            cost = float(service.calculate_cost(quantity))

            logger.info(
                f"{service_type} order completed",
                order_id=order_id,
                quantity=quantity,
                cost=f"${cost:.2f}"
            )

            return {
                'success': True,
                'order_id': order_id,
                'quantity': quantity,
                'cost': cost
            }

        except Exception as e:
            logger.error(f"Failed to create {service_type} order: {e}")
            return {'success': False, 'error': str(e)}

    async def _send_portions_to_nakrutka(
        self,
        order_id: int,
        service: Service,
        link: str,
        portions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Send each portion as SEPARATE order to Nakrutka"""
        logger.info(f"Sending {len(portions)} portions to Nakrutka")

        results = {
            'successful_portions': 0,
            'failed_portions': 0
        }

        for portion in portions:
            try:
                # Create order in Nakrutka
                if portion['runs'] > 1:
                    # Drip-feed order
                    nakrutka_result = await self.nakrutka.create_order(
                        service_id=service.nakrutka_id,
                        link=link,
                        quantity=portion['quantity_per_run'],
                        runs=portion['runs'],
                        interval=portion['interval_minutes']
                    )
                else:
                    # Simple order
                    nakrutka_result = await self.nakrutka.create_order(
                        service_id=service.nakrutka_id,
                        link=link,
                        quantity=portion['quantity_per_run'] * portion['runs']
                    )

                nakrutka_order_id = str(nakrutka_result.get('order'))

                if not nakrutka_order_id or nakrutka_order_id == 'None':
                    raise Exception(f"No order ID in response")

                # Save portion to DB
                await self.db.execute(
                    """
                    INSERT INTO order_portions 
                    (order_id, portion_number, quantity_per_run, runs, 
                     interval_minutes, nakrutka_portion_id, status, 
                     scheduled_at, started_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    order_id,
                    portion['portion_number'],
                    portion['quantity_per_run'],
                    portion['runs'],
                    portion['interval_minutes'],
                    nakrutka_order_id,
                    PORTION_STATUS["RUNNING"],
                    portion.get('scheduled_at'),
                    datetime.utcnow()
                )

                results['successful_portions'] += 1
                await asyncio.sleep(1)  # Rate limit

            except Exception as e:
                logger.error(f"Failed to create portion {portion['portion_number']}: {e}")
                results['failed_portions'] += 1

        # Update order status
        if results['successful_portions'] > 0:
            await self.db.execute(
                "UPDATE orders SET status = $1, started_at = $2 WHERE id = $3",
                ORDER_STATUS["IN_PROGRESS"],
                datetime.utcnow(),
                order_id
            )
        else:
            await self.db.execute(
                "UPDATE orders SET status = $1 WHERE id = $2",
                ORDER_STATUS["FAILED"],
                order_id
            )

        return results

    async def _create_distributed_reactions(
        self,
        post: Post,
        total_quantity: int,
        reaction_services: List[ChannelReactionService],
        templates: List[PortionTemplate]
    ) -> Dict[str, Any]:
        """Create multiple reaction orders based on distribution"""
        if total_quantity <= 0:
            return {'success': True, 'skipped': True}

        try:
            # Calculate distribution
            distributor = ReactionDistributor(reaction_services)
            distributions = distributor.distribute_reactions(total_quantity)

            all_results = []
            total_cost = Decimal(0)

            # Create order for each reaction type
            for dist in distributions:
                if dist['quantity'] <= 0:
                    continue

                result = await self._create_service_order(
                    post=post,
                    service_type='reactions',
                    quantity=dist['quantity'],
                    templates=templates,
                    service_id=dist['service_id']
                )

                if result['success']:
                    total_cost += Decimal(str(result.get('cost', 0)))

                all_results.append(result)

            success_count = sum(1 for r in all_results if r['success'])

            return {
                'success': success_count > 0,
                'orders': all_results,
                'total_cost': float(total_cost)
            }

        except Exception as e:
            logger.error(f"Failed to create distributed reactions: {e}")
            return {'success': False, 'error': str(e)}

    async def check_order_status(self):
        """Check status of active orders"""
        try:
            logger.info("Checking order statuses")

            # Get active portions
            active_portions = await self.db.fetch("""
                SELECT 
                    op.*,
                    o.service_type
                FROM order_portions op
                JOIN orders o ON o.id = op.order_id
                WHERE op.status IN ('waiting', 'running')
                AND op.nakrutka_portion_id IS NOT NULL
                LIMIT 100
            """)

            if not active_portions:
                return

            # Check status for each portion
            for portion in active_portions:
                try:
                    status_info = await self.nakrutka.get_order_status(
                        portion['nakrutka_portion_id']
                    )

                    nakrutka_status = status_info.get('status', '').lower()

                    if nakrutka_status == 'completed':
                        await self.db.execute(
                            "UPDATE order_portions SET status = $1, completed_at = $2 WHERE id = $3",
                            PORTION_STATUS["COMPLETED"],
                            datetime.utcnow(),
                            portion['id']
                        )
                    elif nakrutka_status in ['canceled', 'cancelled']:
                        await self.db.execute(
                            "UPDATE order_portions SET status = $1 WHERE id = $2",
                            PORTION_STATUS["FAILED"],
                            portion['id']
                        )

                except Exception as e:
                    logger.error(f"Failed to check portion status: {e}")

            # Check if orders are complete
            await self._check_order_completion()

        except Exception as e:
            logger.error(f"Status check failed: {e}")

    async def _check_order_completion(self):
        """Check if all portions of orders are completed"""
        orders = await self.db.fetch("""
            SELECT 
                o.id,
                COUNT(op.id) as total_portions,
                COUNT(CASE WHEN op.status = 'completed' THEN 1 END) as completed_portions,
                COUNT(CASE WHEN op.status = 'failed' THEN 1 END) as failed_portions
            FROM orders o
            JOIN order_portions op ON op.order_id = o.id
            WHERE o.status = 'in_progress'
            GROUP BY o.id
        """)

        for order in orders:
            if order['completed_portions'] + order['failed_portions'] == order['total_portions']:
                # All portions done
                if order['failed_portions'] == 0:
                    status = ORDER_STATUS["COMPLETED"]
                else:
                    status = ORDER_STATUS["PARTIAL"]

                await self.db.execute(
                    "UPDATE orders SET status = $1, completed_at = $2 WHERE id = $3",
                    status,
                    datetime.utcnow(),
                    order['id']
                )

    async def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        try:
            stats = await self.queries.get_monitoring_stats()
            return {
                'posts_processing': stats.get('processing_posts', 0),
                'active_orders': stats.get('active_orders', 0),
                'posts_24h': stats.get('posts_24h', 0),
                'orders_24h': stats.get('orders_24h', 0),
                'active_channels': stats.get('active_channels', 0),
                'cache_size': len(self._channel_configs_cache)
            }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {}

    def clear_cache(self):
        """Clear channel configuration cache"""
        self._channel_configs_cache.clear()
        logger.info("Cache cleared")