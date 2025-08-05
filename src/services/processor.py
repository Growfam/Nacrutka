"""
Post processing service - Complete implementation
"""
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import asyncio
from decimal import Decimal

from src.database.connection import DatabaseConnection
from src.database.queries import Queries
from src.database.models import *
from src.services.nakrutka import NakrutkaClient
from src.config import POST_STATUS, ORDER_STATUS, PORTION_STATUS, SERVICE_TYPES
from src.utils.logger import get_logger, DatabaseLogger
from src.utils.helpers import calculate_cost

logger = get_logger(__name__)


class PostProcessor:
    """Processes new posts and creates promotion orders"""

    def __init__(self, db: DatabaseConnection, nakrutka: NakrutkaClient):
        self.db = db
        self.nakrutka = nakrutka
        self.queries = Queries(db)
        self.db_logger = DatabaseLogger(db)

    async def process_new_posts(self):
        """Process all posts with 'new' status"""
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
                        error=str(e)
                    )

        except Exception as e:
            logger.error("Post processing job failed", error=str(e), exc_info=True)

    async def process_single_post(self, post: Post):
        """Process single post with complete logic"""
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

            # Get channel info
            channel = await conn.fetchrow(
                "SELECT * FROM channels WHERE id = $1",
                post.channel_id
            )
            if not channel:
                raise Exception(f"Channel {post.channel_id} not found")

            # Get channel settings
            settings = await self.queries.get_channel_settings(post.channel_id)
            if not settings:
                raise Exception(f"Settings not found for channel {post.channel_id}")

            # Calculate quantities with randomization
            quantities = await self._calculate_quantities(settings)

            logger.info(
                "Calculated quantities",
                channel=channel['channel_username'],
                views=quantities['views'],
                reactions=quantities['reactions'],
                reposts=quantities['reposts'],
                reactions_randomized=settings.randomize_reactions,
                reposts_randomized=settings.randomize_reposts
            )

            # Create orders for each service type
            results = {
                'views': None,
                'reactions': None,
                'reposts': None
            }

            # Process views first (no delay)
            if quantities['views'] > 0:
                results['views'] = await self._create_views_order(
                    post, settings, quantities['views']
                )

            # Process reactions (with delay)
            if quantities['reactions'] > 0:
                results['reactions'] = await self._create_reactions_order(
                    post, settings, quantities['reactions']
                )

            # Process reposts (with delay)
            if quantities['reposts'] > 0:
                results['reposts'] = await self._create_reposts_order(
                    post, settings, quantities['reposts']
                )

            # Check if all orders were successful
            success_count = sum(
                1 for r in results.values()
                if r and r.get('success', False)
            )

            # Update post status
            if success_count == 3 or (success_count > 0 and all(
                r is None or r.get('success', False) or r.get('skipped', False)
                for r in results.values()
            )):
                await conn.execute(
                    "UPDATE posts SET status = $1 WHERE id = $2",
                    POST_STATUS["COMPLETED"],
                    post.id
                )

                # Log success
                await self.db_logger.info(
                    "Post processed successfully",
                    post_id=post.id,
                    channel=channel['channel_username'],
                    post_url=post.post_url,
                    views=quantities['views'],
                    reactions=quantities['reactions'],
                    reposts=quantities['reposts'],
                    orders_created=success_count
                )
            else:
                # Some orders failed
                failures = [k for k, v in results.items() if v and not v.get('success', False)]
                logger.error(
                    "Some orders failed",
                    post_id=post.id,
                    failed_services=failures
                )

                await conn.execute(
                    "UPDATE posts SET status = $1 WHERE id = $2",
                    POST_STATUS["FAILED"],
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

    async def _create_views_order(
        self,
        post: Post,
        settings: ChannelSettings,
        quantity: int
    ) -> Dict[str, Any]:
        """Create views order with portions"""
        if quantity <= 0:
            return {'success': True, 'skipped': True}

        try:
            # Get service info
            service = await self.queries.get_service(settings.views_service_id)
            if not service:
                raise Exception(f"Service {settings.views_service_id} not found")

            # Validate quantity
            if quantity < service.min_quantity:
                logger.warning(
                    f"Views quantity {quantity} below minimum {service.min_quantity}",
                    adjusting=True
                )
                quantity = service.min_quantity
            elif quantity > service.max_quantity:
                logger.warning(
                    f"Views quantity {quantity} above maximum {service.max_quantity}",
                    adjusting=True
                )
                quantity = service.max_quantity

            # Create order record
            order_id = await self.queries.create_order(
                post_id=post.id,
                service_type=SERVICE_TYPES["VIEWS"],
                service_id=settings.views_service_id,
                total_quantity=quantity,
                start_delay_minutes=0  # Views start immediately
            )

            # Get portion templates
            templates = await self.queries.get_portion_templates(
                settings.channel_id,
                SERVICE_TYPES["VIEWS"]
            )

            if not templates:
                raise Exception("No portion templates found for views")

            # Calculate portions based on templates
            portions_data = await self._calculate_portions_from_templates(
                templates, quantity, order_id
            )

            # Save portions to database
            await self.queries.create_portions(portions_data['db_portions'])

            # Create orders in Nakrutka
            nakrutka_results = await self._send_portions_to_nakrutka(
                service_id=settings.views_service_id,
                link=post.post_url,
                portions=portions_data['nakrutka_portions']
            )

            # Update order with main Nakrutka ID
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
            cost = calculate_cost(quantity, float(service.price_per_1000))

            logger.info(
                "Views order created",
                order_id=order_id,
                quantity=quantity,
                portions=len(portions_data['db_portions']),
                cost=f"${cost:.2f}",
                nakrutka_success=nakrutka_results['success']
            )

            return {
                'success': True,
                'order_id': order_id,
                'quantity': quantity,
                'cost': cost,
                'nakrutka_results': nakrutka_results
            }

        except Exception as e:
            logger.error("Failed to create views order", error=str(e), exc_info=True)
            return {'success': False, 'error': str(e)}

    async def _create_reactions_order(
        self,
        post: Post,
        settings: ChannelSettings,
        total_quantity: int
    ) -> Dict[str, Any]:
        """Create reactions orders with proper distribution"""
        if total_quantity <= 0:
            return {'success': True, 'skipped': True}

        try:
            # Get reaction services distribution
            reaction_services = await self.queries.get_reaction_services(
                settings.channel_id
            )

            if not reaction_services:
                # Use default service if no specific distribution
                reaction_services = [{
                    'service_id': settings.reactions_service_id,
                    'emoji': 'mix',
                    'target_quantity': total_quantity,
                    'channel_id': settings.channel_id,
                    'id': 0,
                    'created_at': datetime.utcnow()
                }]

            # Calculate proportional distribution
            total_target = sum(rs['target_quantity'] for rs in reaction_services)

            all_results = []
            total_cost = 0

            # Create separate order for each reaction type
            for rs in reaction_services:
                # Calculate proportional quantity
                proportion = rs['target_quantity'] / total_target if total_target > 0 else 1
                service_quantity = int(total_quantity * proportion)

                if service_quantity <= 0:
                    continue

                logger.info(
                    f"Creating reaction order for {rs['emoji']}",
                    service_id=rs['service_id'],
                    quantity=service_quantity,
                    proportion=f"{proportion*100:.1f}%"
                )

                # Get service info
                service = await self.queries.get_service(rs['service_id'])
                if not service:
                    logger.warning(f"Service {rs['service_id']} not found, skipping")
                    continue

                # Adjust quantity to service limits
                if service_quantity < service.min_quantity:
                    service_quantity = service.min_quantity
                elif service_quantity > service.max_quantity:
                    service_quantity = service.max_quantity

                # Create order
                order_id = await self.queries.create_order(
                    post_id=post.id,
                    service_type=SERVICE_TYPES["REACTIONS"],
                    service_id=rs['service_id'],
                    total_quantity=service_quantity,
                    start_delay_minutes=0  # Will be handled by portions
                )

                # Get portion templates
                templates = await self.queries.get_portion_templates(
                    settings.channel_id,
                    SERVICE_TYPES["REACTIONS"]
                )

                if not templates:
                    raise Exception("No portion templates found for reactions")

                # Calculate portions
                portions_data = await self._calculate_portions_from_templates(
                    templates, service_quantity, order_id
                )

                # Save portions
                await self.queries.create_portions(portions_data['db_portions'])

                # Send to Nakrutka
                nakrutka_results = await self._send_portions_to_nakrutka(
                    service_id=rs['service_id'],
                    link=post.post_url,
                    portions=portions_data['nakrutka_portions']
                )

                # Update order status
                if nakrutka_results['success'] and nakrutka_results['orders']:
                    await self.queries.update_order_nakrutka_id(
                        order_id,
                        nakrutka_results['orders'][0]['order_id']
                    )

                # Calculate cost
                cost = calculate_cost(service_quantity, float(service.price_per_1000))
                total_cost += cost

                all_results.append({
                    'service_id': rs['service_id'],
                    'emoji': rs['emoji'],
                    'quantity': service_quantity,
                    'order_id': order_id,
                    'cost': cost,
                    'success': nakrutka_results['success']
                })

            logger.info(
                "Reactions orders created",
                total_orders=len(all_results),
                total_quantity=total_quantity,
                total_cost=f"${total_cost:.2f}"
            )

            return {
                'success': all(r['success'] for r in all_results) if all_results else False,
                'orders': all_results,
                'total_cost': total_cost
            }

        except Exception as e:
            logger.error("Failed to create reactions order", error=str(e), exc_info=True)
            return {'success': False, 'error': str(e)}

    async def _create_reposts_order(
        self,
        post: Post,
        settings: ChannelSettings,
        quantity: int
    ) -> Dict[str, Any]:
        """Create reposts order with portions"""
        if quantity <= 0:
            return {'success': True, 'skipped': True}

        try:
            # Get service info
            service = await self.queries.get_service(settings.reposts_service_id)
            if not service:
                raise Exception(f"Service {settings.reposts_service_id} not found")

            # Adjust quantity to limits
            if quantity < service.min_quantity:
                quantity = service.min_quantity
            elif quantity > service.max_quantity:
                quantity = service.max_quantity

            # Create order
            order_id = await self.queries.create_order(
                post_id=post.id,
                service_type=SERVICE_TYPES["REPOSTS"],
                service_id=settings.reposts_service_id,
                total_quantity=quantity,
                start_delay_minutes=0  # Will be handled by portions
            )

            # Get portion templates
            templates = await self.queries.get_portion_templates(
                settings.channel_id,
                SERVICE_TYPES["REPOSTS"]
            )

            if not templates:
                raise Exception("No portion templates found for reposts")

            # Calculate portions
            portions_data = await self._calculate_portions_from_templates(
                templates, quantity, order_id
            )

            # Save portions
            await self.queries.create_portions(portions_data['db_portions'])

            # Send to Nakrutka
            nakrutka_results = await self._send_portions_to_nakrutka(
                service_id=settings.reposts_service_id,
                link=post.post_url,
                portions=portions_data['nakrutka_portions']
            )

            # Update order
            if nakrutka_results['success'] and nakrutka_results['orders']:
                await self.queries.update_order_nakrutka_id(
                    order_id,
                    nakrutka_results['orders'][0]['order_id']
                )

            # Calculate cost
            cost = calculate_cost(quantity, float(service.price_per_1000))

            logger.info(
                "Reposts order created",
                order_id=order_id,
                quantity=quantity,
                cost=f"${cost:.2f}",
                nakrutka_success=nakrutka_results['success']
            )

            return {
                'success': True,
                'order_id': order_id,
                'quantity': quantity,
                'cost': cost,
                'nakrutka_results': nakrutka_results
            }

        except Exception as e:
            logger.error("Failed to create reposts order", error=str(e), exc_info=True)
            return {'success': False, 'error': str(e)}

    async def _calculate_portions_from_templates(
        self,
        templates: List[PortionTemplate],
        total_quantity: int,
        order_id: int
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Calculate portions based on templates"""
        db_portions = []
        nakrutka_portions = []

        for template in templates:
            # Calculate quantity for this portion
            portion_quantity = int(total_quantity * float(template.quantity_percent) / 100)

            # Parse runs formula
            if template.runs_formula.startswith('quantity/'):
                divisor = int(template.runs_formula.replace('quantity/', ''))
                quantity_per_run = divisor
                runs = portion_quantity // divisor

                # Adjust for remainder
                if portion_quantity % divisor > 0:
                    runs += 1
            else:
                # Simple formula
                quantity_per_run = portion_quantity
                runs = 1

            # Calculate scheduled time
            scheduled_at = datetime.utcnow() + timedelta(
                minutes=template.start_delay_minutes
            )

            # Add to database portions
            db_portions.append({
                'order_id': order_id,
                'portion_number': template.portion_number,
                'quantity_per_run': quantity_per_run,
                'runs': runs,
                'interval_minutes': template.interval_minutes,
                'scheduled_at': scheduled_at
            })

            # Add to Nakrutka portions
            nakrutka_portions.append({
                'portion_number': template.portion_number,
                'quantity_per_run': quantity_per_run,
                'runs': runs,
                'interval': template.interval_minutes,
                'delay_minutes': template.start_delay_minutes
            })

        return {
            'db_portions': db_portions,
            'nakrutka_portions': nakrutka_portions
        }

    async def _send_portions_to_nakrutka(
        self,
        service_id: int,
        link: str,
        portions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Send portions to Nakrutka API"""
        results = []
        all_success = True

        for portion in portions:
            try:
                # Wait for scheduled time if needed
                if portion.get('delay_minutes', 0) > 0:
                    # For now, we'll let Nakrutka handle delays
                    # In production, might want to schedule these separately
                    pass

                # Create order in Nakrutka
                result = await self.nakrutka.create_order(
                    service_id=service_id,
                    link=link,
                    quantity=portion['quantity_per_run'],
                    runs=portion['runs'],
                    interval=portion['interval']
                )

                results.append({
                    'portion_number': portion['portion_number'],
                    'order_id': result.get('order'),
                    'success': True,
                    'charge': result.get('charge'),
                    'currency': result.get('currency')
                })

            except Exception as e:
                logger.error(
                    f"Failed to create portion {portion['portion_number']}",
                    error=str(e),
                    service_id=service_id
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
                            await self.db.execute(
                                """
                                UPDATE orders 
                                SET status = $1, completed_at = $2
                                WHERE id = $3
                                """,
                                ORDER_STATUS["COMPLETED"],
                                datetime.utcnow(),
                                order.id
                            )

                            logger.info(
                                f"Order {order.id} completed",
                                nakrutka_id=order.nakrutka_order_id,
                                charge=status_info.get('charge'),
                                start_count=status_info.get('start_count'),
                                remains=status_info.get('remains')
                            )

                        elif nakrutka_status in ['canceled', 'cancelled']:
                            await self.db.execute(
                                """
                                UPDATE orders 
                                SET status = $1, completed_at = $2
                                WHERE id = $3
                                """,
                                ORDER_STATUS["CANCELLED"],
                                datetime.utcnow(),
                                order.id
                            )

                            logger.warning(
                                f"Order {order.id} cancelled",
                                nakrutka_id=order.nakrutka_order_id
                            )

                            await self.db_logger.warning(
                                "Order cancelled by Nakrutka",
                                order_id=order.id,
                                nakrutka_id=order.nakrutka_order_id
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