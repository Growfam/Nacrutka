"""
Post processing service - Universal implementation for all channels
FIXED VERSION: Each portion creates SEPARATE order in Nakrutka
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
                logger.info("=== Starting process_new_posts ===")

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
            f"=== Processing post {post.post_id} ===",
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

        # Process each service type
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

        logger.info(f"Calculated quantities: views={views}, reactions={reactions}, reposts={reposts}")

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
            logger.info(f"Processing views: {quantities['views']}")
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
                    service_id=settings.reactions_service_id,
                    delay_minutes=0
                )

        # Process reposts - ONLY ONE PORTION!
        if quantities['reposts'] > 0 and templates.get('reposts'):
            logger.info(f"Processing reposts: {quantities['reposts']} (single portion only)")
            results['reposts'] = await self._create_service_order(
                post=post,
                service_type='reposts',
                quantity=quantities['reposts'],
                templates=None,  # Force single portion for reposts
                service_id=settings.reposts_service_id,
                delay_minutes=5,
                force_single_portion=True  # New parameter
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
            return {'success': True, 'skipped': True, 'reason': 'Zero quantity'}

        try:
            logger.info(
                f"=== Creating {service_type} order ===",
                quantity=quantity,
                service_id=service_id,
                force_single_portion=force_single_portion
            )

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

            logger.info(f"Created order record: {order_id}")

            # Calculate portions
            if force_single_portion or service_type == 'reposts':
                # Force single portion for reposts
                portions = [{
                    'portion_number': 1,
                    'quantity_per_run': adjusted_quantity,
                    'runs': 1,
                    'interval_minutes': 0,
                    'scheduled_at': datetime.utcnow() + timedelta(minutes=delay_minutes),
                    'total_quantity': adjusted_quantity
                }]
                logger.info(f"Using single portion for {service_type}")
            else:
                portions = await self._calculate_optimized_portions(
                    quantity=adjusted_quantity,
                    service=service,
                    templates=templates,
                    delay_minutes=delay_minutes
                )
                logger.info(f"Calculated {len(portions)} portions for {service_type}")

            # Send to Nakrutka - EACH PORTION SEPARATELY!
            nakrutka_results = await self._send_portions_to_nakrutka(
                order_id=order_id,
                service=service,
                link=post.post_url,
                portions=portions,
                service_type=service_type
            )

            # Calculate cost
            cost = float(service.calculate_cost(adjusted_quantity))

            logger.info(
                f"{service_type.capitalize()} order completed",
                order_id=order_id,
                service_id=service.nakrutka_id,
                service_name=service.service_name,
                quantity=adjusted_quantity,
                portions=len(portions),
                cost=f"${cost:.2f}",
                successful_portions=nakrutka_results['successful_portions']
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

    async def _send_portions_to_nakrutka(
        self,
        order_id: int,
        service: Service,
        link: str,
        portions: List[Dict[str, Any]],
        service_type: str
    ) -> Dict[str, Any]:
        """Send each portion as SEPARATE order to Nakrutka"""

        logger.info(
            f"=== Sending {len(portions)} portions to Nakrutka ===",
            order_id=order_id,
            service_type=service_type
        )

        results = {
            'successful_portions': 0,
            'failed_portions': 0,
            'portion_results': []
        }

        # Create SEPARATE order for EACH portion
        for portion in portions:
            try:
                portion_total = portion['quantity_per_run'] * portion['runs']

                logger.info(
                    f"Creating Nakrutka order for portion {portion['portion_number']}",
                    order_id=order_id,
                    portion_number=portion['portion_number'],
                    total_for_portion=portion_total,
                    quantity_per_run=portion['quantity_per_run'],
                    runs=portion['runs'],
                    interval=portion['interval_minutes']
                )

                # Create order in Nakrutka
                if portion['runs'] > 1:
                    # Drip-feed order for this portion
                    nakrutka_result = await self.nakrutka.create_order(
                        service_id=service.nakrutka_id,
                        link=link,
                        quantity=portion['quantity_per_run'],
                        runs=portion['runs'],
                        interval=portion['interval_minutes']
                    )
                else:
                    # Simple order (single run)
                    nakrutka_result = await self.nakrutka.create_order(
                        service_id=service.nakrutka_id,
                        link=link,
                        quantity=portion_total
                    )

                logger.info(
                    f"Nakrutka API response for portion {portion['portion_number']}",
                    response=nakrutka_result
                )

                nakrutka_order_id = str(nakrutka_result.get('order'))

                if not nakrutka_order_id or nakrutka_order_id == 'None':
                    raise Exception(f"No order ID in response: {nakrutka_result}")

                # Save portion to DB WITH its own nakrutka_order_id
                portion_db_id = await self.db.fetchval(
                    """
                    INSERT INTO order_portions 
                    (order_id, portion_number, quantity_per_run, runs, 
                     interval_minutes, nakrutka_portion_id, status, 
                     scheduled_at, started_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING id
                    """,
                    order_id,
                    portion['portion_number'],
                    portion['quantity_per_run'],
                    portion['runs'],
                    portion['interval_minutes'],
                    nakrutka_order_id,  # Unique for each portion!
                    PORTION_STATUS["RUNNING"],
                    portion.get('scheduled_at'),
                    datetime.utcnow()
                )

                logger.info(
                    f"✓ Portion {portion['portion_number']} saved",
                    portion_db_id=portion_db_id,
                    nakrutka_order_id=nakrutka_order_id
                )

                results['successful_portions'] += 1
                results['portion_results'].append({
                    'portion_number': portion['portion_number'],
                    'nakrutka_order_id': nakrutka_order_id,
                    'quantity': portion_total,
                    'success': True
                })

                # Small delay between creating orders
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(
                    f"Failed to create order for portion {portion['portion_number']}",
                    error=str(e),
                    order_id=order_id,
                    exc_info=True
                )

                results['failed_portions'] += 1
                results['portion_results'].append({
                    'portion_number': portion['portion_number'],
                    'error': str(e),
                    'success': False
                })

        # Update main order status
        if results['successful_portions'] > 0:
            # At least one portion succeeded
            await self.db.execute(
                "UPDATE orders SET status = $1, started_at = $2 WHERE id = $3",
                ORDER_STATUS["IN_PROGRESS"],
                datetime.utcnow(),
                order_id
            )
        else:
            # All portions failed
            await self.db.execute(
                "UPDATE orders SET status = $1 WHERE id = $2",
                ORDER_STATUS["FAILED"],
                order_id
            )

        logger.info(
            f"Nakrutka sending completed",
            order_id=order_id,
            successful=results['successful_portions'],
            failed=results['failed_portions']
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
            logger.info(
                f"=== Creating distributed reactions ===",
                total_quantity=total_quantity,
                reaction_types=len(reaction_services)
            )

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
                        'portions': v.get('nakrutka_results', {}).get('successful_portions', 0)
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
        """Check status of active orders and their portions"""
        try:
            logger.info("=== Checking order statuses ===")

            # Get all active PORTIONS with their own nakrutka_portion_id
            active_portions = await self.db.fetch("""
                SELECT 
                    op.id as portion_id,
                    op.order_id,
                    op.portion_number,
                    op.nakrutka_portion_id,
                    op.status as portion_status,
                    op.quantity_per_run,
                    op.runs,
                    o.id as order_id,
                    o.service_type,
                    o.total_quantity
                FROM order_portions op
                JOIN orders o ON o.id = op.order_id
                WHERE op.status IN ('waiting', 'running')
                AND op.nakrutka_portion_id IS NOT NULL
                ORDER BY op.order_id, op.portion_number
            """)

            if not active_portions:
                logger.info("No active portions to check")
                return

            logger.info(f"Checking {len(active_portions)} active portions")

            # Group by nakrutka IDs for batch checking
            nakrutka_ids = [p['nakrutka_portion_id'] for p in active_portions]

            # Get statuses in batches
            batch_size = 100
            for i in range(0, len(nakrutka_ids), batch_size):
                batch = nakrutka_ids[i:i + batch_size]

                try:
                    statuses = await self.nakrutka.get_multiple_status(batch)

                    # Process each portion
                    for portion in active_portions:
                        if portion['nakrutka_portion_id'] in statuses:
                            status_info = statuses[portion['nakrutka_portion_id']]
                            await self._process_portion_status_update(portion, status_info)

                except Exception as e:
                    logger.error(
                        f"Failed to check batch status",
                        error=str(e),
                        batch_size=len(batch)
                    )

            # Check if all portions of an order are completed
            await self._check_order_completion()

        except Exception as e:
            logger.error("Status check failed", error=str(e), exc_info=True)

    async def _process_portion_status_update(
        self,
        portion: Dict[str, Any],
        status_info: Dict[str, Any]
    ):
        """Process status update for a portion"""
        nakrutka_status = status_info.get('status', '').lower()

        logger.info(
            f"Portion {portion['portion_id']} status",
            nakrutka_id=portion['nakrutka_portion_id'],
            status=nakrutka_status,
            remains=status_info.get('remains', 0)
        )

        if nakrutka_status == 'completed':
            await self.db.execute(
                """
                UPDATE order_portions
                SET status = $1, completed_at = $2
                WHERE id = $3
                """,
                PORTION_STATUS["COMPLETED"],
                datetime.utcnow(),
                portion['portion_id']
            )

            # Record performance
            await self._record_portion_performance(portion, status_info)

        elif nakrutka_status in ['canceled', 'cancelled']:
            await self.db.execute(
                """
                UPDATE order_portions
                SET status = $1, completed_at = $2
                WHERE id = $3
                """,
                PORTION_STATUS["FAILED"],
                datetime.utcnow(),
                portion['portion_id']
            )

            await self.db_logger.warning(
                "Portion cancelled",
                portion_id=portion['portion_id'],
                nakrutka_id=portion['nakrutka_portion_id'],
                reason=status_info.get('error', 'Unknown')
            )

    async def _check_order_completion(self):
        """Check if all portions of orders are completed"""
        orders = await self.db.fetch("""
            SELECT 
                o.id,
                o.status,
                COUNT(op.id) as total_portions,
                COUNT(CASE WHEN op.status = 'completed' THEN 1 END) as completed_portions,
                COUNT(CASE WHEN op.status = 'failed' THEN 1 END) as failed_portions
            FROM orders o
            JOIN order_portions op ON op.order_id = o.id
            WHERE o.status = 'in_progress'
            GROUP BY o.id, o.status
        """)

        for order in orders:
            if order['completed_portions'] + order['failed_portions'] == order['total_portions']:
                # All portions are done
                if order['failed_portions'] == 0:
                    # All successful
                    await self.db.execute(
                        "UPDATE orders SET status = $1, completed_at = $2 WHERE id = $3",
                        ORDER_STATUS["COMPLETED"],
                        datetime.utcnow(),
                        order['id']
                    )
                    logger.info(f"Order {order['id']} completed successfully")
                else:
                    # Some failed
                    await self.db.execute(
                        "UPDATE orders SET status = $1, completed_at = $2 WHERE id = $3",
                        ORDER_STATUS["PARTIAL"],
                        datetime.utcnow(),
                        order['id']
                    )
                    logger.warning(
                        f"Order {order['id']} partially completed",
                        failed=order['failed_portions'],
                        total=order['total_portions']
                    )

    async def _record_portion_performance(
        self,
        portion: Dict[str, Any],
        status_info: Dict[str, Any]
    ):
        """Record portion performance for optimization"""
        try:
            # Calculate completion time
            started_at = await self.db.fetchval(
                "SELECT started_at FROM order_portions WHERE id = $1",
                portion['portion_id']
            )

            if started_at:
                completion_time = datetime.utcnow() - started_at

                # Get post info for channel_id
                post_info = await self.db.fetchrow("""
                    SELECT p.channel_id 
                    FROM posts p
                    JOIN orders o ON o.post_id = p.id
                    WHERE o.id = $1
                """, portion['order_id'])

                if post_info:
                    # Record in optimizer
                    self._optimizer.record_performance(
                        channel_id=post_info['channel_id'],
                        service_type=portion['service_type'],
                        order_id=portion['order_id'],
                        completion_time=completion_time.total_seconds(),
                        success=True,
                        actual_quantity=portion['quantity_per_run'] * portion['runs'] - status_info.get('remains', 0),
                        target_quantity=portion['quantity_per_run'] * portion['runs'],
                        service_id=None,  # Would need to get from order
                        cost=status_info.get('charge', 0)
                    )

        except Exception as e:
            logger.error(f"Failed to record performance: {e}")

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