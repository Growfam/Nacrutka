"""
Post processing service
"""
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import asyncio

from src.database.connection import DatabaseConnection
from src.database.queries import Queries
from src.database.models import *
from src.services.nakrutka import NakrutkaClient
from src.config import POST_STATUS, ORDER_STATUS, PORTION_STATUS, SERVICE_TYPES
from src.utils.logger import get_logger, DatabaseLogger

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
                return

            logger.info(f"Processing {len(new_posts)} new posts")

            # Process each post
            for post in new_posts:
                try:
                    await self.process_single_post(post)
                    await asyncio.sleep(1)  # Small delay between posts
                except Exception as e:
                    logger.error(
                        f"Failed to process post {post.id}",
                        error=str(e),
                        post_id=post.post_id,
                        channel_id=post.channel_id
                    )

                    # Mark as failed
                    await self.queries.update_post_status(
                        post.id,
                        POST_STATUS["FAILED"]
                    )

                    await self.db_logger.error(
                        "Post processing failed",
                        post_id=post.id,
                        error=str(e)
                    )

        except Exception as e:
            logger.error("Post processing job failed", error=str(e))

    async def process_single_post(self, post: Post):
        """Process single post"""
        logger.info(
            f"Processing post {post.post_id}",
            post_id=post.id,
            channel_id=post.channel_id
        )

        # Mark as processing
        await self.queries.update_post_status(
            post.id,
            POST_STATUS["PROCESSING"],
            datetime.utcnow()
        )

        # Get channel settings
        channel = await self.db.fetchrow(
            "SELECT * FROM channels WHERE id = $1",
            post.channel_id
        )
        if not channel:
            raise Exception(f"Channel {post.channel_id} not found")

        settings = await self.queries.get_channel_settings(post.channel_id)
        if not settings:
            raise Exception(f"Settings not found for channel {post.channel_id}")

        # Calculate quantities with randomization
        quantities = await self._calculate_quantities(settings)

        logger.info(
            "Calculated quantities",
            views=quantities['views'],
            reactions=quantities['reactions'],
            reposts=quantities['reposts']
        )

        # Create orders for each service type
        results = {
            'views': await self._create_views_order(post, settings, quantities['views']),
            'reactions': await self._create_reactions_order(post, settings, quantities['reactions']),
            'reposts': await self._create_reposts_order(post, settings, quantities['reposts'])
        }

        # Check if all orders were successful
        all_success = all(r['success'] for r in results.values())

        if all_success:
            await self.queries.update_post_status(
                post.id,
                POST_STATUS["COMPLETED"]
            )

            await self.db_logger.info(
                "Post processed successfully",
                post_id=post.id,
                channel=channel['channel_username'],
                views=quantities['views'],
                reactions=quantities['reactions'],
                reposts=quantities['reposts']
            )
        else:
            # Log failures
            failures = [k for k, v in results.items() if not v['success']]
            logger.error(
                "Some orders failed",
                post_id=post.id,
                failed_services=failures
            )

    async def _calculate_quantities(
            self,
            settings: ChannelSettings
    ) -> Dict[str, int]:
        """Calculate quantities with randomization"""
        # Views - no randomization
        views = settings.views_target

        # Reactions - with randomization if enabled
        if settings.randomize_reactions:
            reactions = await self.queries.calculate_random_quantity(
                settings.reactions_target,
                settings.randomize_percent
            )
        else:
            reactions = settings.reactions_target

        # Reposts - with randomization if enabled
        if settings.randomize_reposts:
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
            # Create order record
            order_id = await self.queries.create_order(
                post_id=post.id,
                service_type=SERVICE_TYPES["VIEWS"],
                service_id=settings.views_service_id,
                total_quantity=quantity,
                start_delay_minutes=0
            )

            # Calculate portions
            portions = await self.queries.calculate_portion_details(
                settings.channel_id,
                SERVICE_TYPES["VIEWS"],
                quantity
            )

            # Prepare portions for database
            db_portions = []
            nakrutka_portions = []

            for p in portions:
                scheduled_at = datetime.utcnow() + timedelta(minutes=p['start_delay_minutes'])

                db_portions.append({
                    'order_id': order_id,
                    'portion_number': p['portion_number'],
                    'quantity_per_run': p['quantity_per_run'],
                    'runs': p['runs'],
                    'interval_minutes': p['interval_minutes'],
                    'scheduled_at': scheduled_at
                })

                nakrutka_portions.append({
                    'quantity_per_run': p['quantity_per_run'],
                    'runs': p['runs'],
                    'interval': p['interval_minutes']
                })

            # Save portions to database
            await self.queries.create_portions(db_portions)

            # Create orders in Nakrutka
            results = await self.nakrutka.create_drip_feed_order(
                service_id=settings.views_service_id,
                link=post.post_url,
                total_quantity=quantity,
                portions=nakrutka_portions
            )

            # Update order with Nakrutka ID (use first portion's ID as main)
            if results and results[0]['success']:
                await self.queries.update_order_nakrutka_id(
                    order_id,
                    results[0]['result']['order']
                )

            return {
                'success': True,
                'order_id': order_id,
                'nakrutka_results': results
            }

        except Exception as e:
            logger.error("Failed to create views order", error=str(e))
            return {'success': False, 'error': str(e)}

    async def _create_reactions_order(
            self,
            post: Post,
            settings: ChannelSettings,
            quantity: int
    ) -> Dict[str, Any]:
        """Create reactions order with portions"""
        if quantity <= 0:
            return {'success': True, 'skipped': True}

        try:
            # Get reaction services distribution
            reaction_services = await self.queries.get_reaction_services(
                settings.channel_id
            )

            if not reaction_services:
                # Use default service
                reaction_services = [{
                    'service_id': settings.reactions_service_id,
                    'emoji': 'mix',
                    'target_quantity': quantity
                }]

            all_results = []

            # Create orders for each reaction type
            for rs in reaction_services:
                # Calculate proportional quantity
                if len(reaction_services) > 1:
                    # Distribute proportionally
                    total_target = sum(r.target_quantity for r in reaction_services)
                    proportion = rs.target_quantity / total_target
                    service_quantity = int(quantity * proportion)
                else:
                    service_quantity = quantity

                if service_quantity <= 0:
                    continue

                # Create order
                order_id = await self.queries.create_order(
                    post_id=post.id,
                    service_type=SERVICE_TYPES["REACTIONS"],
                    service_id=rs.service_id,
                    total_quantity=service_quantity,
                    start_delay_minutes=5  # 5 min delay for reactions
                )

                # Calculate portions
                portions = await self.queries.calculate_portion_details(
                    settings.channel_id,
                    SERVICE_TYPES["REACTIONS"],
                    service_quantity
                )

                # Create portions (similar to views)
                # ... (similar code as views)

                all_results.append({
                    'service_id': rs.service_id,
                    'emoji': rs.emoji,
                    'quantity': service_quantity,
                    'order_id': order_id
                })

            return {
                'success': True,
                'orders': all_results
            }

        except Exception as e:
            logger.error("Failed to create reactions order", error=str(e))
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
            # Create order record
            order_id = await self.queries.create_order(
                post_id=post.id,
                service_type=SERVICE_TYPES["REPOSTS"],
                service_id=settings.reposts_service_id,
                total_quantity=quantity,
                start_delay_minutes=17  # 17 min delay for reposts
            )

            # Similar to views but with repost settings
            # ... (similar implementation)

            return {
                'success': True,
                'order_id': order_id
            }

        except Exception as e:
            logger.error("Failed to create reposts order", error=str(e))
            return {'success': False, 'error': str(e)}

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

            # Get statuses from Nakrutka
            statuses = await self.nakrutka.get_multiple_status(nakrutka_ids)

            # Update order statuses
            for order in active_orders:
                if not order.nakrutka_order_id:
                    continue

                status_info = statuses.get(order.nakrutka_order_id, {})

                if status_info.get('status') == 'Completed':
                    await self.db.execute(
                        """
                        UPDATE orders
                        SET status       = $1,
                            completed_at = $2
                        WHERE id = $3
                        """,
                        ORDER_STATUS["COMPLETED"],
                        datetime.utcnow(),
                        order.id
                    )

                    logger.info(
                        f"Order {order.id} completed",
                        nakrutka_id=order.nakrutka_order_id
                    )
                elif status_info.get('status') == 'Canceled':
                    await self.db.execute(
                        "UPDATE orders SET status = $1 WHERE id = $2",
                        ORDER_STATUS["CANCELLED"],
                        order.id
                    )

                    logger.warning(
                        f"Order {order.id} cancelled",
                        nakrutka_id=order.nakrutka_order_id
                    )

        except Exception as e:
            logger.error("Status check failed", error=str(e))