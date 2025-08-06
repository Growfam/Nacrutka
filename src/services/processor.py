"""
Post processing service - Universal implementation for all channels
FIXED VERSION: Properly saves nakrutka_order_id and handles drip-feed correctly
"""
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import asyncio
from decimal import Decimal

from src.database.connection import DatabaseConnection
from src.database.queries import Queries
from src.database.models import *
from src.services.nakrutka import NakrutkaClient, NakrutkaError
from src.services.portion_calculator import (
    PortionCalculator,
    ReactionDistributor
)
from src.services.optimizer import DynamicPortionOptimizer
from src.config import POST_STATUS, ORDER_STATUS, PORTION_STATUS, SERVICE_TYPES
from src.utils.logger import get_logger, DatabaseLogger
from src.utils.helpers import calculate_cost, ErrorRecovery
from src.utils.validators import DataValidator, OrderValidator

logger = get_logger(__name__)


class ValidationError(Exception):
    """Validation error for processing"""
    pass


def validate_before_processing(
    post: Post,
    channel_settings: ChannelSettings,
    portion_templates: Dict[str, List[PortionTemplate]]
) -> List[str]:
    """Comprehensive validation before processing a post"""
    errors = []

    # Validate post
    valid, error = DataValidator.validate_post(post)
    if not valid:
        errors.append(f"Post validation: {error}")

    # Validate settings
    valid, error = DataValidator.validate_channel_settings(channel_settings)
    if not valid:
        errors.append(f"Settings validation: {error}")

    # Validate templates for each service type
    for service_type in ['views', 'reactions', 'reposts']:
        templates = portion_templates.get(service_type, [])

        # Skip if no target for this type
        if service_type == 'views' and channel_settings.views_target == 0:
            continue
        if service_type == 'reactions' and channel_settings.reactions_target == 0:
            continue
        if service_type == 'reposts' and channel_settings.reposts_target == 0:
            continue

        if templates:
            valid, error = DataValidator.validate_portion_templates(templates, service_type)
            if not valid:
                errors.append(f"Templates validation for {service_type}: {error}")

    return errors


class PostProcessor:
    """Universal post processor that adapts to any channel configuration"""

    def __init__(self, db: DatabaseConnection, nakrutka: NakrutkaClient):
        self.db = db
        self.nakrutka = nakrutka
        self.queries = Queries(db)
        self.db_logger = DatabaseLogger(db)
        self._processing_lock = asyncio.Lock()
        self._channel_configs_cache = {}
        self._cache_ttl = 300  # 5 minutes
        self._optimizer = DynamicPortionOptimizer()

    async def process_new_posts(self):
        """Process all posts with 'new' status"""
        async with self._processing_lock:
            try:
                # Ensure service cache is populated
                if not self.queries._service_cache.services:
                    logger.info("Service cache empty, refreshing...")
                    await self.queries.refresh_service_cache()

                # Get new posts
                new_posts = await self.queries.get_new_posts(limit=10)

                if not new_posts:
                    logger.debug("No new posts to process")
                    return

                logger.info(f"Processing {len(new_posts)} new posts")

                # Group posts by channel for optimization
                posts_by_channel = {}
                for post in new_posts:
                    if post.channel_id not in posts_by_channel:
                        posts_by_channel[post.channel_id] = []
                    posts_by_channel[post.channel_id].append(post)

                # Process each channel's posts
                for channel_id, channel_posts in posts_by_channel.items():
                    try:
                        # Load channel config once
                        config = await self._get_channel_config(channel_id)
                        if not config:
                            logger.error(f"No configuration for channel {channel_id}")
                            continue

                        # Process posts for this channel
                        for post in channel_posts:
                            try:
                                await self._process_single_post_with_config(post, config)
                                await asyncio.sleep(2)  # Rate limiting

                            except Exception as e:
                                await self._handle_post_error(post, e)

                    except Exception as e:
                        logger.error(
                            f"Failed to process channel {channel_id}",
                            error=str(e),
                            exc_info=True
                        )

            except Exception as e:
                logger.error("Post processing job failed", error=str(e), exc_info=True)

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

            # Validate configuration
            if not self._validate_channel_config(config):
                logger.error(f"Invalid configuration for channel {channel_id}")
                return None

        return config

    def _validate_channel_config(self, config: Dict[str, Any]) -> bool:
        """Validate channel configuration completeness"""
        required = ['channel', 'settings', 'templates']
        return all(key in config and config[key] for key in required)

    async def _process_single_post_with_config(
        self,
        post: Post,
        config: Dict[str, Any]
    ):
        """Process single post with pre-loaded configuration"""
        logger.info(
            f"Processing post {post.post_id}",
            post_id=post.id,
            channel_id=post.channel_id,
            url=post.post_url
        )

        # Extract configuration
        channel = config['channel']
        settings = config['settings']
        reaction_services = config.get('reaction_services', [])
        templates = config['templates']

        # Validate before processing
        errors = validate_before_processing(post, settings, templates)
        if errors:
            raise ValidationError(f"Validation failed: {'; '.join(errors)}")

        # Calculate quantities with randomization
        quantities = await self._calculate_quantities(settings)

        # Log calculated quantities
        await self.db_logger.info(
            "Calculated quantities for post",
            post_id=post.id,
            channel=channel.channel_username,
            views=quantities['views'],
            reactions=quantities['reactions'],
            reposts=quantities['reposts'],
            randomized={
                'reactions': settings.randomize_reactions,
                'reposts': settings.randomize_reposts
            }
        )

        # Ensure service cache is fresh
        await self.queries.refresh_service_cache()

        # First, update post status to processing
        await self.db.execute(
            """
            UPDATE posts 
            SET status = $1, processed_at = $2
            WHERE id = $3
            """,
            POST_STATUS["PROCESSING"],
            datetime.utcnow(),
            post.id
        )

        # Process each service type - NOW WITHOUT TRANSACTION
        results = await self._process_all_service_types(
            post=post,
            config=config,
            quantities=quantities
        )

        # Update post status based on results
        await self._update_final_post_status(post, results)

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

        results = {
            'views': None,
            'reactions': None,
            'reposts': None
        }

        # Process views
        if quantities['views'] > 0 and templates.get('views'):
            results['views'] = await self._create_service_order(
                post=post,
                service_type='views',
                quantity=quantities['views'],
                templates=templates['views'],
                service_id=settings.views_service_id,
                delay_minutes=0
            )

        # Process reactions - special handling for multiple services
        if quantities['reactions'] > 0:
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
                    service_id=settings.reactions_service_id,
                    delay_minutes=0
                )

        # Process reposts
        if quantities['reposts'] > 0 and templates.get('reposts'):
            results['reposts'] = await self._create_service_order(
                post=post,
                service_type='reposts',
                quantity=quantities['reposts'],
                templates=templates['reposts'],
                service_id=settings.reposts_service_id,
                delay_minutes=5  # Default delay for reposts
            )

        return results

    async def _create_service_order(
        self,
        post: Post,
        service_type: str,
        quantity: int,
        templates: List[PortionTemplate],
        service_id: Optional[int] = None,
        delay_minutes: int = 0
    ) -> Dict[str, Any]:
        """Create order for a service type"""
        if quantity <= 0:
            return {'success': True, 'skipped': True, 'reason': 'Zero quantity'}

        try:
            # Get or find service
            if not service_id:
                service = await self._find_best_service(service_type, quantity)
                if not service:
                    raise Exception(f"No suitable service found for {service_type}")
            else:
                service = await self.queries.get_service(service_id)
                if not service:
                    # Try to find alternative
                    logger.warning(f"Service {service_id} not found, finding alternative")
                    service = await self._find_best_service(service_type, quantity)
                    if not service:
                        raise Exception(f"Service {service_id} not found for {service_type}")

            # Validate and adjust quantity
            adjusted_quantity = self._adjust_quantity_for_service(quantity, service)

            if adjusted_quantity != quantity:
                logger.info(
                    f"Adjusted {service_type} quantity",
                    original=quantity,
                    adjusted=adjusted_quantity,
                    service_limits=(service.min_quantity, service.max_quantity)
                )

            # Create order record
            order_id = await self.queries.create_order(
                post_id=post.id,
                service_type=service_type,
                service_id=service.nakrutka_id,
                total_quantity=adjusted_quantity,
                start_delay_minutes=delay_minutes
            )

            # Calculate portions
            portions = await self._calculate_optimized_portions(
                quantity=adjusted_quantity,
                service=service,
                templates=templates,
                delay_minutes=delay_minutes
            )

            # Save portions to database
            await self._save_portions(order_id, portions)

            # Send to Nakrutka - NOW PROPERLY
            nakrutka_results = await self._send_to_nakrutka_drip_feed(
                order_id=order_id,
                service=service,
                link=post.post_url,
                portions=portions
            )

            # Calculate cost
            cost = float(service.calculate_cost(adjusted_quantity))

            logger.info(
                f"{service_type.capitalize()} order created",
                order_id=order_id,
                service_id=service.nakrutka_id,
                service_name=service.service_name,
                quantity=adjusted_quantity,
                portions=len(portions),
                cost=f"${cost:.2f}",
                nakrutka_success=nakrutka_results.get('success', False)
            )

            return {
                'success': True,
                'order_id': order_id,
                'service_id': service.nakrutka_id,
                'quantity': adjusted_quantity,
                'cost': cost,
                'portions': len(portions),
                'nakrutka_results': nakrutka_results
            }

        except Exception as e:
            logger.error(
                f"Failed to create {service_type} order",
                error=str(e),
                post_id=post.id,
                exc_info=True
            )
            return {
                'success': False,
                'error': str(e),
                'error_type': type(e).__name__
            }

    async def _send_to_nakrutka_drip_feed(
        self,
        order_id: int,
        service: Service,
        link: str,
        portions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Send to Nakrutka with PROPER drip-feed handling"""

        # Check if this is drip-feed (multiple portions)
        is_drip_feed = len(portions) > 1

        if is_drip_feed:
            # DRIP-FEED: Create ONE order with total runs

            # Calculate total quantity and runs
            total_quantity = sum(p['quantity_per_run'] * p['runs'] for p in portions)
            total_runs = sum(p['runs'] for p in portions)

            # Calculate quantity per run (should be consistent)
            quantity_per_run = total_quantity // total_runs if total_runs > 0 else total_quantity

            # Use average interval (or take from first portion)
            interval = portions[0]['interval_minutes'] if portions else 15

            logger.info(
                f"Creating SINGLE drip-feed order",
                order_id=order_id,
                total_quantity=total_quantity,
                quantity_per_run=quantity_per_run,
                total_runs=total_runs,
                interval=interval,
                portions_count=len(portions)
            )

            try:
                # Create ONE drip-feed order
                result = await self.nakrutka.create_order(
                    service_id=service.nakrutka_id,
                    link=link,
                    quantity=quantity_per_run,
                    runs=total_runs,
                    interval=interval
                )

                logger.info(f"Nakrutka API response: {result}")

                nakrutka_order_id = str(result.get('order'))
                if not nakrutka_order_id or nakrutka_order_id == 'None':
                    raise Exception(f"No order ID in response: {result}")

                # Update order with nakrutka_id - WITH VERIFICATION
                success = await self.queries.update_order_nakrutka_id(order_id, nakrutka_order_id)

                if not success:
                    raise Exception(f"Failed to update order {order_id} with nakrutka_id {nakrutka_order_id}")

                # Verify update
                verified_id = await self.queries.verify_order_update(order_id)
                if verified_id != nakrutka_order_id:
                    raise Exception(f"Verification failed: expected {nakrutka_order_id}, got {verified_id}")

                logger.info(f"Successfully verified order {order_id} with nakrutka_id {nakrutka_order_id}")

                # Update ALL portions with the SAME nakrutka_order_id
                await self.queries.update_all_portions_for_order(
                    order_id,
                    PORTION_STATUS["RUNNING"],
                    nakrutka_order_id
                )

                return {
                    'success': True,
                    'nakrutka_order_id': nakrutka_order_id,
                    'order_type': 'drip_feed',
                    'total_quantity': total_quantity,
                    'runs': total_runs,
                    'interval': interval
                }

            except Exception as e:
                logger.error(f"Failed to create drip-feed order: {e}")

                # Update status to failed
                await self.db.execute(
                    "UPDATE orders SET status = $1 WHERE id = $2",
                    ORDER_STATUS["FAILED"],
                    order_id
                )

                raise

        else:
            # SINGLE ORDER: One portion or simple order
            portion = portions[0] if portions else {'quantity_per_run': 0, 'runs': 1, 'interval_minutes': 0}

            try:
                # Check if this single portion has multiple runs
                if portion.get('runs', 1) > 1:
                    # Still use drip-feed for multiple runs
                    result = await self.nakrutka.create_order(
                        service_id=service.nakrutka_id,
                        link=link,
                        quantity=portion['quantity_per_run'],
                        runs=portion['runs'],
                        interval=portion.get('interval_minutes', 15)
                    )
                else:
                    # Simple order without drip-feed
                    total_qty = portion.get('total_quantity', portion['quantity_per_run'])
                    result = await self.nakrutka.create_order(
                        service_id=service.nakrutka_id,
                        link=link,
                        quantity=total_qty
                    )

                logger.info(f"Nakrutka API response: {result}")

                nakrutka_order_id = str(result.get('order'))
                if not nakrutka_order_id or nakrutka_order_id == 'None':
                    raise Exception(f"No order ID in response: {result}")

                # Update order with verification
                success = await self.queries.update_order_nakrutka_id(order_id, nakrutka_order_id)

                if not success:
                    raise Exception(f"Failed to update order {order_id}")

                # Update portion if exists
                if portions:
                    await self.db.execute(
                        """
                        UPDATE order_portions
                        SET nakrutka_portion_id = $1,
                            status = $2,
                            started_at = $3
                        WHERE order_id = $4 AND portion_number = $5
                        """,
                        nakrutka_order_id,
                        PORTION_STATUS["RUNNING"],
                        datetime.utcnow(),
                        order_id,
                        portion['portion_number']
                    )

                return {
                    'success': True,
                    'nakrutka_order_id': nakrutka_order_id,
                    'order_type': 'single',
                    'quantity': portion.get('total_quantity', portion['quantity_per_run'])
                }

            except Exception as e:
                logger.error(f"Failed to create single order: {e}")

                await self.db.execute(
                    "UPDATE orders SET status = $1 WHERE id = $2",
                    ORDER_STATUS["FAILED"],
                    order_id
                )

                raise

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
            total_actual_quantity = 0

            # Create order for each reaction type
            for dist in distributions:
                if dist['quantity'] <= 0:
                    continue

                logger.info(
                    f"Creating reaction order",
                    emoji=dist['emoji'],
                    service_id=dist['service_id'],
                    quantity=dist['quantity'],
                    proportion=f"{dist['proportion']*100:.1f}%"
                )

                # Get service
                service = await self.queries.get_service(dist['service_id'])
                if not service:
                    logger.warning(
                        f"Service {dist['service_id']} not found",
                        emoji=dist['emoji']
                    )
                    continue

                # Adjust quantity
                adjusted_quantity = self._adjust_quantity_for_service(
                    dist['quantity'],
                    service
                )

                # Create order
                result = await self._create_service_order(
                    post=post,
                    service_type='reactions',
                    quantity=adjusted_quantity,
                    templates=templates,
                    service_id=dist['service_id'],
                    delay_minutes=0
                )

                if result['success']:
                    total_cost += Decimal(str(result['cost']))
                    total_actual_quantity += result['quantity']

                all_results.append({
                    **result,
                    'emoji': dist['emoji'],
                    'target_quantity': dist['quantity']
                })

            # Summary
            success_count = sum(1 for r in all_results if r['success'])

            logger.info(
                "Reaction orders completed",
                total_orders=len(all_results),
                successful=success_count,
                total_quantity=total_actual_quantity,
                total_cost=f"${total_cost:.2f}"
            )

            return {
                'success': success_count > 0,
                'orders': all_results,
                'total_cost': float(total_cost),
                'total_quantity': total_actual_quantity,
                'distribution_count': len(all_results)
            }

        except Exception as e:
            logger.error(
                "Failed to create distributed reactions",
                error=str(e),
                post_id=post.id,
                exc_info=True
            )
            return {
                'success': False,
                'error': str(e)
            }

    async def _find_best_service(
        self,
        service_type: str,
        quantity: int
    ) -> Optional[Service]:
        """Find best service for given type and quantity"""
        # Try optimized selection first
        service = await self.queries.optimize_service_selection(service_type, quantity)

        if service:
            return service

        # Fallback to cheapest
        service = await self.queries.get_cheapest_service(service_type)

        if not service:
            logger.error(f"No services available for {service_type}")

        return service

    def _adjust_quantity_for_service(self, quantity: int, service: Service) -> int:
        """Adjust quantity to fit service limits"""
        if quantity < service.min_quantity:
            return service.min_quantity
        elif quantity > service.max_quantity:
            return service.max_quantity
        return quantity

    async def _calculate_optimized_portions(
        self,
        quantity: int,
        service: Service,
        templates: List[PortionTemplate],
        delay_minutes: int
    ) -> List[Dict[str, Any]]:
        """Calculate optimized portions"""
        if not templates:
            # Use default distribution
            return self._optimizer.calculate_default_portions(
                quantity=quantity,
                service=service,
                delay_minutes=delay_minutes
            )

        # Use templates with optimization
        calculator = PortionCalculator(templates)
        portions = calculator.calculate_portions(
            total_quantity=quantity,
            service=service,
            start_time=datetime.utcnow() + timedelta(minutes=delay_minutes)
        )

        # Optimize based on performance history
        channel_id = templates[0].channel_id if templates else None
        if channel_id:
            portions = self._optimizer.optimize_portions(
                portions=portions,
                channel_id=channel_id,
                service_type=service.service_type
            )

        return portions

    async def _save_portions(self, order_id: int, portions: List[Dict[str, Any]]):
        """Save portions to database"""
        db_portions = [
            {
                'order_id': order_id,
                'portion_number': p['portion_number'],
                'quantity_per_run': p['quantity_per_run'],
                'runs': p['runs'],
                'interval_minutes': p['interval_minutes'],
                'scheduled_at': p.get('scheduled_at')
            }
            for p in portions
        ]

        await self.queries.create_portions(db_portions)

    async def _update_final_post_status(
        self,
        post: Post,
        results: Dict[str, Any]
    ):
        """Update post status based on processing results"""
        # Count successes
        success_count = sum(
            1 for r in results.values()
            if r and (r.get('success') or r.get('skipped'))
        )

        # Calculate total cost
        total_cost = sum(
            r.get('cost', 0) for r in results.values()
            if r and r.get('success')
        )

        if success_count > 0:
            # At least partial success
            status = POST_STATUS["COMPLETED"]

            await self.db_logger.info(
                "Post processed",
                post_id=post.id,
                post_url=post.post_url,
                success_count=success_count,
                total_cost=f"${total_cost:.2f}",
                results={
                    k: {
                        'success': v.get('success', False),
                        'quantity': v.get('quantity', 0),
                        'cost': v.get('cost', 0),
                        'nakrutka_id': v.get('nakrutka_results', {}).get('nakrutka_order_id')
                    }
                    for k, v in results.items() if v
                }
            )
        else:
            # Complete failure
            status = POST_STATUS["FAILED"]

            await self.db_logger.error(
                "Post processing failed",
                post_id=post.id,
                errors={
                    k: v.get('error', 'Unknown error')
                    for k, v in results.items()
                    if v and not v.get('success')
                }
            )

        await self.db.execute(
            "UPDATE posts SET status = $1 WHERE id = $2",
            status,
            post.id
        )

    async def _handle_post_error(self, post: Post, error: Exception):
        """Handle post processing error"""
        error_category = ErrorRecovery.categorize_error(error)

        logger.error(
            f"Post processing error",
            post_id=post.id,
            error=str(error),
            category=error_category,
            exc_info=True
        )

        # Update post status
        await self.queries.update_post_status(post.id, POST_STATUS["FAILED"])

        # Log to database
        await self.db_logger.error(
            "Post processing failed",
            post_id=post.id,
            post_url=post.post_url,
            error=str(error),
            error_type=type(error).__name__,
            error_category=error_category
        )

        # Determine if we should retry later
        if ErrorRecovery.should_retry_error(error):
            # Schedule for retry
            await self._schedule_retry(post, error_category)

    async def _schedule_retry(self, post: Post, error_category: str):
        """Schedule post for retry"""
        # This would integrate with a retry queue
        # For now, just log
        logger.info(
            f"Post scheduled for retry",
            post_id=post.id,
            error_category=error_category
        )

    async def check_order_status(self):
        """Check status of active orders"""
        try:
            active_orders = await self.queries.get_active_orders()

            if not active_orders:
                return

            logger.info(f"Checking status of {len(active_orders)} orders")

            # Group by Nakrutka order ID for batch checking
            nakrutka_ids = [
                order.nakrutka_order_id
                for order in active_orders
                if order.nakrutka_order_id
            ]

            if not nakrutka_ids:
                logger.warning("No orders with nakrutka_order_id to check")
                return

            # Get statuses in batches
            batch_size = 100
            for i in range(0, len(nakrutka_ids), batch_size):
                batch = nakrutka_ids[i:i + batch_size]
                await self._check_batch_status(batch, active_orders)

            # Check portion statuses (they all have same nakrutka_order_id as main order)
            await self._check_portion_statuses()

        except Exception as e:
            logger.error("Status check failed", error=str(e), exc_info=True)

    async def _check_batch_status(
        self,
        batch: List[str],
        active_orders: List[Order]
    ):
        """Check status for a batch of orders"""
        try:
            statuses = await self.nakrutka.get_multiple_status(batch)

            for order in active_orders:
                if not order.nakrutka_order_id or order.nakrutka_order_id not in statuses:
                    continue

                status_info = statuses.get(order.nakrutka_order_id, {})
                await self._process_order_status_update(order, status_info)

        except Exception as e:
            logger.error(
                f"Failed to check batch status",
                error=str(e),
                batch_size=len(batch)
            )

    async def _process_order_status_update(
        self,
        order: Order,
        status_info: Dict[str, Any]
    ):
        """Process status update for an order"""
        nakrutka_status = status_info.get('status', '').lower()

        if nakrutka_status == 'completed':
            await self.queries.update_order_status(
                order.id,
                ORDER_STATUS["COMPLETED"],
                datetime.utcnow()
            )

            # Update all portions to completed
            await self.db.execute(
                """
                UPDATE order_portions
                SET status = $1, completed_at = $2
                WHERE order_id = $3
                """,
                PORTION_STATUS["COMPLETED"],
                datetime.utcnow(),
                order.id
            )

            # Record performance
            await self._record_order_performance(order, status_info)

        elif nakrutka_status in ['canceled', 'cancelled']:
            await self.queries.update_order_status(
                order.id,
                ORDER_STATUS["CANCELLED"],
                datetime.utcnow()
            )

            await self.db_logger.warning(
                "Order cancelled",
                order_id=order.id,
                nakrutka_id=order.nakrutka_order_id,
                reason=status_info.get('error', 'Unknown')
            )

        elif nakrutka_status == 'partial':
            logger.warning(
                f"Order partially completed",
                order_id=order.id,
                remains=status_info.get('remains')
            )

    async def _record_order_performance(
        self,
        order: Order,
        status_info: Dict[str, Any]
    ):
        """Record order performance for optimization"""
        try:
            # Get post and channel info
            post = await self.queries.get_post_by_id(order.post_id)
            if not post:
                return

            # Calculate completion time
            if order.started_at:
                completion_time = datetime.utcnow() - order.started_at

                # Record in optimizer
                self._optimizer.record_performance(
                    channel_id=post.channel_id,
                    service_type=order.service_type,
                    order_id=order.id,
                    completion_time=completion_time.total_seconds(),
                    success=True,
                    actual_quantity=order.total_quantity - status_info.get('remains', 0),
                    target_quantity=order.total_quantity,
                    service_id=order.service_id,
                    cost=status_info.get('charge', 0)
                )

        except Exception as e:
            logger.error(f"Failed to record performance: {e}")

    async def _check_portion_statuses(self):
        """Check and update portion statuses - they share nakrutka_order_id with main order"""
        try:
            # For drip-feed, portions share the same nakrutka_order_id as the main order
            # We check the main order status to update portions

            query = """
                SELECT DISTINCT o.id, o.nakrutka_order_id, o.status
                FROM orders o
                WHERE o.status = $1
                AND o.nakrutka_order_id IS NOT NULL
                AND EXISTS (
                    SELECT 1 FROM order_portions op
                    WHERE op.order_id = o.id
                    AND op.status = $2
                )
                LIMIT 50
            """

            orders = await self.db.fetch(
                query,
                ORDER_STATUS["IN_PROGRESS"],
                PORTION_STATUS["RUNNING"]
            )

            for order in orders:
                try:
                    # Get status from Nakrutka
                    status = await self.nakrutka.get_order_status(order['nakrutka_order_id'])

                    if status.get('status', '').lower() == 'completed':
                        # Update all portions for this order
                        await self.db.execute(
                            """
                            UPDATE order_portions
                            SET status = $1, completed_at = $2
                            WHERE order_id = $3
                            AND status = $4
                            """,
                            PORTION_STATUS["COMPLETED"],
                            datetime.utcnow(),
                            order['id'],
                            PORTION_STATUS["RUNNING"]
                        )

                        logger.info(f"Updated all portions for order {order['id']} to completed")

                except Exception as e:
                    logger.error(
                        f"Failed to check portions for order {order['id']}",
                        error=str(e)
                    )

        except Exception as e:
            logger.error("Portion status check failed", error=str(e))

    async def retry_failed_orders(self, hours: int = 24):
        """Retry recently failed orders"""
        try:
            failed_posts = await self.queries.get_recent_failed_posts(hours)

            if not failed_posts:
                logger.info("No failed posts to retry")
                return

            logger.info(f"Found {len(failed_posts)} failed posts to retry")

            retry_count = 0
            for post in failed_posts:
                # Check if should retry
                if await self._should_retry_post(post):
                    await self.queries.update_post_status(
                        post.id,
                        POST_STATUS["NEW"]
                    )
                    retry_count += 1

            logger.info(f"Reset {retry_count} posts for retry")

        except Exception as e:
            logger.error(f"Failed to retry orders: {e}")

    async def _should_retry_post(self, post: Post) -> bool:
        """Determine if post should be retried"""
        # Check post age
        age = datetime.utcnow() - post.created_at
        if age.days > 2:
            return False  # Too old

        # Check retry count (would need to track this)
        # For now, always retry if within age limit
        return True

    async def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        try:
            stats = await self.queries.get_monitoring_stats()

            # Add optimizer stats
            optimizer_stats = self._optimizer.get_stats()

            return {
                'posts_processing': stats.get('processing_posts', 0),
                'active_orders': stats.get('active_orders', 0),
                'posts_24h': stats.get('posts_24h', 0),
                'orders_24h': stats.get('orders_24h', 0),
                'cost_today': stats.get('cost_today', 0),
                'active_channels': stats.get('active_channels', 0),
                'cache_size': len(self._channel_configs_cache),
                'optimizer_stats': optimizer_stats
            }

        except Exception as e:
            logger.error(f"Failed to get processing stats: {e}")
            return {}

    def clear_cache(self):
        """Clear channel configuration cache"""
        self._channel_configs_cache.clear()
        logger.info("Channel configuration cache cleared")