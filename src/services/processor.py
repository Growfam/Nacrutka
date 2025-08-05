"""
Post processing service - Universal implementation for all channels
"""
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import asyncio
from decimal import Decimal

from src.database.connection import DatabaseConnection
from src.database.queries import Queries
from src.database.models import *
from src.services.nakrutka import NakrutkaClient
from src.services.portion_calculator import PortionCalculator, ReactionDistributor
from src.config import POST_STATUS, ORDER_STATUS, PORTION_STATUS, SERVICE_TYPES
from src.utils.logger import get_logger, DatabaseLogger
from src.utils.helpers import calculate_cost
from src.utils.validators import DataValidator, OrderValidator, validate_before_processing

logger = get_logger(__name__)


class PostProcessor:
    """Universal post processor that adapts to any channel configuration"""

    def __init__(self, db: DatabaseConnection, nakrutka: NakrutkaClient):
        self.db = db
        self.nakrutka = nakrutka
        self.queries = Queries(db)
        self.db_logger = DatabaseLogger(db)
        self._processing_lock = asyncio.Lock()

    async def process_new_posts(self):
        """Process all posts with 'new' status"""
        async with self._processing_lock:
            try:
                # Get new posts
                new_posts = await self.queries.get_new_posts(limit=10)

                if not new_posts:
                    logger.debug("No new posts to process")
                    return

                logger.info(f"Processing {len(new_posts)} new posts")

                # Process each post
                for post in new_posts:
                    try:
                        await self.process_single_post(post)
                        # Small delay between posts to avoid rate limits
                        await asyncio.sleep(2)

                    except Exception as e:
                        logger.error(
                            f"Failed to process post {post.id}",
                            error=str(e),
                            post_id=post.post_id,
                            channel_id=post.channel_id,
                            exc_info=True
                        )

                        # Mark as failed
                        await self.queries.update_post_status(
                            post.id,
                            POST_STATUS["FAILED"]
                        )

                        await self.db_logger.error(
                            "Post processing failed",
                            post_id=post.id,
                            post_url=post.post_url,
                            error=str(e),
                            error_type=type(e).__name__
                        )

            except Exception as e:
                logger.error("Post processing job failed", error=str(e), exc_info=True)

    async def process_single_post(self, post: Post):
        """Process single post with universal logic"""
        logger.info(
            f"Processing post {post.post_id}",
            post_id=post.id,
            channel_id=post.channel_id,
            url=post.post_url
        )

        # Start transaction
        async with self.db.transaction() as conn:
            # Mark as processing
            await conn.execute(
                """
                UPDATE posts 
                SET status = $1, processed_at = $2
                WHERE id = $3
                """,
                POST_STATUS["PROCESSING"],
                datetime.utcnow(),
                post.id
            )

            # Get full channel configuration
            config = await self.queries.get_channel_with_full_config(post.channel_id)
            if not config:
                raise Exception(f"No configuration found for channel {post.channel_id}")

            channel = config['channel']
            settings = config['settings']
            reaction_services = config['reaction_services']
            templates = config['templates']

            # Validate configuration
            errors = validate_before_processing(post, settings, templates)
            if errors:
                raise Exception(f"Validation failed: {'; '.join(errors)}")

            # Validate channel
            valid, error = DataValidator.validate_channel(channel)
            if not valid:
                raise Exception(f"Invalid channel: {error}")

            # Validate settings
            valid, error = DataValidator.validate_channel_settings(settings)
            if not valid:
                raise Exception(f"Invalid settings: {error}")

            # Calculate quantities with randomization
            quantities = await self._calculate_quantities(settings)

            logger.info(
                "Calculated quantities",
                channel=channel.channel_username,
                views=quantities['views'],
                reactions=quantities['reactions'],
                reposts=quantities['reposts'],
                reactions_randomized=settings.randomize_reactions,
                reposts_randomized=settings.randomize_reposts
            )

            # Load services dynamically
            await self.queries.refresh_service_cache()

            # Create orders for each service type
            results = {
                'views': None,
                'reactions': None,
                'reposts': None
            }

            # Process each service type
            if quantities['views'] > 0 and templates.get('views'):
                results['views'] = await self._create_universal_order(
                    post=post,
                    settings=settings,
                    service_type='views',
                    quantity=quantities['views'],
                    templates=templates['views'],
                    delay_minutes=0
                )

            if quantities['reactions'] > 0 and templates.get('reactions'):
                # Use reaction distribution if available
                if reaction_services:
                    results['reactions'] = await self._create_distributed_reactions_order(
                        post=post,
                        settings=settings,
                        quantity=quantities['reactions'],
                        templates=templates['reactions'],
                        reaction_services=reaction_services
                    )
                else:
                    # Fallback to single service
                    results['reactions'] = await self._create_universal_order(
                        post=post,
                        settings=settings,
                        service_type='reactions',
                        quantity=quantities['reactions'],
                        templates=templates['reactions'],
                        delay_minutes=0
                    )

            if quantities['reposts'] > 0 and templates.get('reposts'):
                results['reposts'] = await self._create_universal_order(
                    post=post,
                    settings=settings,
                    service_type='reposts',
                    quantity=quantities['reposts'],
                    templates=templates['reposts'],
                    delay_minutes=5  # Default delay for reposts
                )

            # Check if all orders were successful
            success_count = sum(
                1 for r in results.values()
                if r and (r.get('success', False) or r.get('skipped', False))
            )

            # Update post status
            if success_count > 0:
                await conn.execute(
                    "UPDATE posts SET status = $1 WHERE id = $2",
                    POST_STATUS["COMPLETED"],
                    post.id
                )

                # Calculate total cost
                total_cost = sum(
                    r.get('total_cost', 0) for r in results.values()
                    if r and r.get('success', False)
                )

                # Log success
                await self.db_logger.info(
                    "Post processed successfully",
                    post_id=post.id,
                    channel=channel.channel_username,
                    post_url=post.post_url,
                    views=quantities['views'],
                    reactions=quantities['reactions'],
                    reposts=quantities['reposts'],
                    orders_created=success_count,
                    total_cost=f"${total_cost:.2f}"
                )
            else:
                # All orders failed
                await conn.execute(
                    "UPDATE posts SET status = $1 WHERE id = $2",
                    POST_STATUS["FAILED"],
                    post.id
                )

                failures = [k for k, v in results.items() if v and not v.get('success', False)]
                logger.error(
                    "All orders failed",
                    post_id=post.id,
                    failed_services=failures
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

    async def _create_universal_order(
        self,
        post: Post,
        settings: ChannelSettings,
        service_type: str,
        quantity: int,
        templates: List[PortionTemplate],
        delay_minutes: int = 0
    ) -> Dict[str, Any]:
        """Create order with universal logic for any service type"""
        if quantity <= 0:
            return {'success': True, 'skipped': True}

        try:
            # Get service ID based on type
            service_id = None
            if service_type == 'views':
                service_id = settings.views_service_id
            elif service_type == 'reactions':
                service_id = settings.reactions_service_id
            elif service_type == 'reposts':
                service_id = settings.reposts_service_id

            if not service_id:
                # Try to find best service dynamically
                service = await self.queries.optimize_service_selection(service_type, quantity)
                if not service:
                    service = await self.queries.get_cheapest_service(service_type)
                if not service:
                    raise Exception(f"No service found for {service_type}")
                service_id = service.nakrutka_id
            else:
                # Get service info
                service = await self.queries.get_service(service_id)
                if not service:
                    raise Exception(f"Service {service_id} not found")

            # Validate and adjust quantity
            if quantity < service.min_quantity:
                logger.warning(
                    f"{service_type} quantity {quantity} below minimum {service.min_quantity}",
                    adjusting=True
                )
                quantity = service.min_quantity
            elif quantity > service.max_quantity:
                logger.warning(
                    f"{service_type} quantity {quantity} above maximum {service.max_quantity}",
                    adjusting=True
                )
                quantity = service.max_quantity

            # Create order record
            order_id = await self.queries.create_order(
                post_id=post.id,
                service_type=service_type,
                service_id=service_id,
                total_quantity=quantity,
                start_delay_minutes=delay_minutes
            )

            # Calculate portions using templates
            calculator = PortionCalculator(templates)
            portions = calculator.calculate_portions(
                total_quantity=quantity,
                service=service,
                start_time=datetime.utcnow() + timedelta(minutes=delay_minutes)
            )

            # Validate portions
            valid, error = DataValidator.validate_portions(portions, quantity)
            if not valid:
                logger.warning(f"Portion validation: {error}")

            # Convert to DB format
            db_portions = [
                {
                    'order_id': order_id,
                    'portion_number': p['portion_number'],
                    'quantity_per_run': p['quantity_per_run'],
                    'runs': p['runs'],
                    'interval_minutes': p['interval_minutes'],
                    'scheduled_at': p['scheduled_at']
                }
                for p in portions
            ]

            # Save portions to database
            await self.queries.create_portions(db_portions)

            # Create orders in Nakrutka
            nakrutka_results = await self._send_portions_to_nakrutka(
                service_id=service_id,
                link=post.post_url,
                portions=portions
            )

            # Update order with Nakrutka ID
            if nakrutka_results['success'] and nakrutka_results['orders']:
                await self.queries.update_order_nakrutka_id(
                    order_id,
                    nakrutka_results['orders'][0]['order_id']
                )

                # Update portion IDs
                for i, result in enumerate(nakrutka_results['orders']):
                    if result['success']:
                        await self.db.execute(
                            """
                            UPDATE order_portions 
                            SET nakrutka_portion_id = $1, status = $2
                            WHERE order_id = $3 AND portion_number = $4
                            """,
                            result['order_id'],
                            PORTION_STATUS["RUNNING"],
                            order_id,
                            i + 1
                        )

            # Calculate cost
            cost = float(service.calculate_cost(quantity))

            logger.info(
                f"{service_type.capitalize()} order created",
                order_id=order_id,
                service_id=service_id,
                service_name=service.service_name,
                quantity=quantity,
                portions=len(portions),
                cost=f"${cost:.2f}",
                nakrutka_success=nakrutka_results['success']
            )

            return {
                'success': True,
                'order_id': order_id,
                'service_id': service_id,
                'quantity': quantity,
                'cost': cost,
                'nakrutka_results': nakrutka_results
            }

        except Exception as e:
            logger.error(f"Failed to create {service_type} order", error=str(e), exc_info=True)
            return {'success': False, 'error': str(e)}

    async def _create_distributed_reactions_order(
        self,
        post: Post,
        settings: ChannelSettings,
        quantity: int,
        templates: List[PortionTemplate],
        reaction_services: List[ChannelReactionService]
    ) -> Dict[str, Any]:
        """Create multiple reaction orders based on distribution"""
        if quantity <= 0:
            return {'success': True, 'skipped': True}

        try:
            # Convert to distributor format
            distributor_services = [
                {
                    'service_id': rs.service_id,
                    'emoji': rs.emoji,
                    'target_quantity': rs.target_quantity
                }
                for rs in reaction_services
            ]

            # Calculate distribution
            distributor = ReactionDistributor(distributor_services)
            distributions = distributor.distribute_reactions(
                total_quantity=quantity,
                randomized=settings.randomize_reactions
            )

            all_results = []
            total_cost = 0

            # Create separate order for each reaction type
            for dist in distributions:
                if dist['quantity'] <= 0:
                    continue

                logger.info(
                    f"Creating reaction order for {dist['emoji']}",
                    service_id=dist['service_id'],
                    quantity=dist['quantity'],
                    proportion=f"{dist['proportion']*100:.1f}%"
                )

                # Get service info
                service = await self.queries.get_service(dist['service_id'])
                if not service:
                    logger.warning(f"Service {dist['service_id']} not found, skipping")
                    continue

                # Adjust quantity to service limits
                service_quantity = dist['quantity']
                if service_quantity < service.min_quantity:
                    service_quantity = service.min_quantity
                elif service_quantity > service.max_quantity:
                    service_quantity = service.max_quantity

                # Create order
                order_id = await self.queries.create_order(
                    post_id=post.id,
                    service_type=SERVICE_TYPES["REACTIONS"],
                    service_id=dist['service_id'],
                    total_quantity=service_quantity,
                    start_delay_minutes=0
                )

                # Calculate portions
                calculator = PortionCalculator(templates)
                portions = calculator.calculate_portions(
                    total_quantity=service_quantity,
                    service=service
                )

                # Save portions
                db_portions = [
                    {
                        'order_id': order_id,
                        'portion_number': p['portion_number'],
                        'quantity_per_run': p['quantity_per_run'],
                        'runs': p['runs'],
                        'interval_minutes': p['interval_minutes'],
                        'scheduled_at': p['scheduled_at']
                    }
                    for p in portions
                ]
                await self.queries.create_portions(db_portions)

                # Send to Nakrutka
                nakrutka_results = await self._send_portions_to_nakrutka(
                    service_id=dist['service_id'],
                    link=post.post_url,
                    portions=portions
                )

                # Update order status
                if nakrutka_results['success'] and nakrutka_results['orders']:
                    await self.queries.update_order_nakrutka_id(
                        order_id,
                        nakrutka_results['orders'][0]['order_id']
                    )

                # Calculate cost
                cost = float(service.calculate_cost(service_quantity))
                total_cost += cost

                all_results.append({
                    'service_id': dist['service_id'],
                    'emoji': dist['emoji'],
                    'quantity': service_quantity,
                    'order_id': order_id,
                    'cost': cost,
                    'success': nakrutka_results['success']
                })

            logger.info(
                "Reaction orders created",
                total_orders=len(all_results),
                total_quantity=quantity,
                total_cost=f"${total_cost:.2f}"
            )

            return {
                'success': all(r['success'] for r in all_results) if all_results else False,
                'orders': all_results,
                'total_cost': total_cost
            }

        except Exception as e:
            logger.error("Failed to create distributed reactions", error=str(e), exc_info=True)
            return {'success': False, 'error': str(e)}

    async def _send_portions_to_nakrutka(
        self,
        service_id: int,
        link: str,
        portions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Send portions to Nakrutka API"""
        results = []
        all_success = True

        # Get service for validation
        service = await self.queries.get_service(service_id)
        if not service:
            return {
                'success': False,
                'error': f"Service {service_id} not found",
                'orders': []
            }

        # Create validator
        validator = OrderValidator({service_id: service})

        for portion in portions:
            try:
                # Validate order parameters
                valid, error = validator.validate_order_params(
                    service_id=service_id,
                    quantity=portion['quantity_per_run'] * portion['runs'],
                    link=link
                )
                if not valid:
                    raise Exception(error)

                # Create order in Nakrutka
                result = await self.nakrutka.create_order(
                    service_id=service_id,
                    link=link,
                    quantity=portion['quantity_per_run'],
                    runs=portion['runs'],
                    interval=portion['interval_minutes']
                )

                results.append({
                    'portion_number': portion['portion_number'],
                    'order_id': result.get('order'),
                    'success': True,
                    'charge': result.get('charge'),
                    'currency': result.get('currency'),
                    'total_quantity': portion['total_quantity']
                })

            except Exception as e:
                logger.error(
                    f"Failed to create portion {portion['portion_number']}",
                    error=str(e),
                    service_id=service_id,
                    quantity=portion['quantity_per_run'],
                    runs=portion['runs']
                )
                results.append({
                    'portion_number': portion['portion_number'],
                    'success': False,
                    'error': str(e)
                })
                all_success = False

        return {
            'success': all_success,
            'orders': results
        }

    async def check_order_status(self):
        """Check status of active orders"""
        try:
            active_orders = await self.queries.get_active_orders()

            if not active_orders:
                return

            logger.info(f"Checking status of {len(active_orders)} orders")

            # Group by Nakrutka order ID
            nakrutka_ids = [
                order.nakrutka_order_id
                for order in active_orders
                if order.nakrutka_order_id
            ]

            if not nakrutka_ids:
                return

            # Get statuses from Nakrutka in batches
            batch_size = 100
            for i in range(0, len(nakrutka_ids), batch_size):
                batch = nakrutka_ids[i:i + batch_size]

                try:
                    statuses = await self.nakrutka.get_multiple_status(batch)

                    # Update order statuses
                    for order in active_orders:
                        if not order.nakrutka_order_id or order.nakrutka_order_id not in statuses:
                            continue

                        status_info = statuses.get(order.nakrutka_order_id, {})
                        nakrutka_status = status_info.get('status', '').lower()

                        if nakrutka_status == 'completed':
                            await self.queries.update_order_status(
                                order.id,
                                ORDER_STATUS["COMPLETED"],
                                datetime.utcnow()
                            )

                            logger.info(
                                f"Order {order.id} completed",
                                nakrutka_id=order.nakrutka_order_id,
                                charge=status_info.get('charge'),
                                start_count=status_info.get('start_count'),
                                remains=status_info.get('remains')
                            )

                        elif nakrutka_status in ['canceled', 'cancelled']:
                            await self.queries.update_order_status(
                                order.id,
                                ORDER_STATUS["CANCELLED"],
                                datetime.utcnow()
                            )

                            logger.warning(
                                f"Order {order.id} cancelled",
                                nakrutka_id=order.nakrutka_order_id
                            )

                            await self.db_logger.warning(
                                "Order cancelled by Nakrutka",
                                order_id=order.id,
                                nakrutka_id=order.nakrutka_order_id,
                                reason=status_info.get('error', 'Unknown')
                            )

                        elif nakrutka_status == 'partial':
                            # Partial completion
                            logger.warning(
                                f"Order {order.id} partially completed",
                                nakrutka_id=order.nakrutka_order_id,
                                remains=status_info.get('remains')
                            )

                except Exception as e:
                    logger.error(
                        f"Failed to check batch of {len(batch)} orders",
                        error=str(e)
                    )

            # Check portion statuses
            await self._check_portion_statuses()

        except Exception as e:
            logger.error("Status check failed", error=str(e), exc_info=True)

    async def _check_portion_statuses(self):
        """Check and update portion statuses"""
        try:
            # Get running portions
            running_portions = await self.db.fetch(
                """
                SELECT op.*, o.nakrutka_order_id 
                FROM order_portions op
                JOIN orders o ON o.id = op.order_id
                WHERE op.status = $1 
                AND op.nakrutka_portion_id IS NOT NULL
                """,
                PORTION_STATUS["RUNNING"]
            )

            if not running_portions:
                return

            # Check each portion
            for portion in running_portions:
                try:
                    status = await self.nakrutka.get_order_status(
                        portion['nakrutka_portion_id']
                    )

                    if status.get('status', '').lower() == 'completed':
                        await self.db.execute(
                            """
                            UPDATE order_portions 
                            SET status = $1, completed_at = $2
                            WHERE id = $3
                            """,
                            PORTION_STATUS["COMPLETED"],
                            datetime.utcnow(),
                            portion['id']
                        )

                        logger.debug(
                            f"Portion {portion['id']} completed",
                            portion_number=portion['portion_number']
                        )

                except Exception as e:
                    logger.error(
                        f"Failed to check portion {portion['id']}",
                        error=str(e)
                    )

        except Exception as e:
            logger.error("Portion status check failed", error=str(e))

    async def retry_failed_orders(self, hours: int = 24):
        """Retry recently failed orders"""
        try:
            # Get failed posts
            failed_posts = await self.queries.get_recent_failed_posts(hours)

            if not failed_posts:
                logger.info("No failed posts to retry")
                return

            logger.info(f"Retrying {len(failed_posts)} failed posts")

            for post in failed_posts:
                try:
                    # Reset status to new
                    await self.queries.update_post_status(
                        post.id,
                        POST_STATUS["NEW"]
                    )

                    # Will be picked up by next processing cycle
                    logger.info(f"Reset post {post.id} for retry")

                except Exception as e:
                    logger.error(f"Failed to reset post {post.id}: {e}")

        except Exception as e:
            logger.error(f"Failed to retry orders: {e}")

    async def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        try:
            stats = await self.queries.get_monitoring_stats()

            # Add processing-specific stats
            processing_stats = {
                'posts_processing': stats.get('processing_posts', 0),
                'active_orders': stats.get('active_orders', 0),
                'posts_24h': stats.get('posts_24h', 0),
                'orders_24h': stats.get('orders_24h', 0),
                'cost_today': stats.get('cost_today', 0),
                'active_channels': stats.get('active_channels', 0)
            }

            return processing_stats

        except Exception as e:
            logger.error(f"Failed to get processing stats: {e}")
            return {}