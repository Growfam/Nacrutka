"""
Post processor - creates orders for new posts with advanced portion support
"""
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import random
import json

from src.database.models import (
    Post, PostStatus, Order, OrderStatus,
    ServiceType, ChannelSettings, APIProvider
)
from src.database.repositories.post_repo import post_repo
from src.database.repositories.channel_repo import channel_repo
from src.database.repositories.order_repo import order_repo
from src.database.repositories.service_repo import service_repo
from src.services.twiboost_client import twiboost_client
from src.services.nakrutochka_client import nakrutochka_client
from src.utils.logger import get_logger, LoggerMixin, metrics
from src.config import settings

logger = get_logger(__name__)


class PostProcessor(LoggerMixin):
    """Process new posts and create orders with advanced portion support"""

    def __init__(self):
        self.processing_lock = asyncio.Lock()
        self.processed_count = 0
        self.error_count = 0
        self.processing_posts = set()

    async def process_new_posts(self, limit: int = 10) -> int:
        """Process batch of new posts"""
        async with self.processing_lock:
            try:
                # Get new posts
                posts = await post_repo.get_new_posts(limit)

                if not posts:
                    self.log_debug("No new posts to process")
                    return 0

                self.log_info(f"Processing {len(posts)} new posts")

                # Process each post
                processed = 0
                for post in posts:
                    # Check if already being processed
                    if post.id in self.processing_posts:
                        self.log_warning(
                            f"Post {post.id} already being processed, skipping",
                            post_id=post.id
                        )
                        continue

                    try:
                        # Add to processing set
                        self.processing_posts.add(post.id)

                        await self._process_single_post(post)
                        processed += 1
                        self.processed_count += 1

                    except Exception as e:
                        self.log_error(
                            "Failed to process post",
                            error=e,
                            post_id=post.id,
                            channel_id=post.channel_id
                        )
                        await post_repo.update_status(post.id, PostStatus.FAILED)
                        self.error_count += 1
                        metrics.log_error_rate("post_processing", 1)
                    finally:
                        # Remove from processing set
                        self.processing_posts.discard(post.id)

                self.log_info(
                    "Batch processing completed",
                    processed=processed,
                    total_processed=self.processed_count,
                    errors=self.error_count
                )

                return processed

            except Exception as e:
                self.log_error("Batch processing failed", error=e)
                return 0

    async def _process_single_post(self, post: Post):
        """Process single post with dual API support"""

        # Check for existing orders
        existing_orders = await order_repo.get_orders_by_post(post.id)
        if existing_orders:
            self.log_warning(
                f"Orders already exist for post {post.id}, marking as completed",
                post_id=post.id,
                existing_count=len(existing_orders)
            )
            await post_repo.update_status(post.id, PostStatus.COMPLETED, processed_at=True)
            return

        self.log_info(
            "Processing post",
            post_id=post.id,
            channel_id=post.channel_id,
            message_id=post.message_id
        )

        # Update status to processing
        await post_repo.update_status(post.id, PostStatus.PROCESSING)

        # Get channel settings
        all_settings = await channel_repo.get_channel_settings(post.channel_id)

        if not all_settings:
            self.log_warning(
                "No settings found for channel",
                channel_id=post.channel_id
            )
            # Create default settings
            await channel_repo.bulk_create_default_settings(post.channel_id)
            all_settings = await channel_repo.get_channel_settings(post.channel_id)

        # Get channel username
        channel = await channel_repo.get_channel(post.channel_id)
        if channel and channel.username:
            post.channel_username = channel.username

        # Create orders for each service type
        orders_created = []
        failed_services = []

        for settings in all_settings:
            try:
                # Check if orders of this type already exist
                existing_type_orders = [o for o in existing_orders if o.service_type == settings.service_type]
                if existing_type_orders:
                    self.log_warning(
                        f"Orders of type {settings.service_type} already exist for post {post.id}",
                        post_id=post.id,
                        service_type=settings.service_type
                    )
                    continue

                # Determine API provider
                api_provider = self._determine_api_provider(settings.service_type, settings)

                self.log_info(
                    f"Creating {settings.service_type} orders using {api_provider}",
                    post_id=post.id,
                    api_provider=api_provider
                )

                if settings.service_type == ServiceType.VIEWS:
                    orders = await self._create_advanced_view_orders(post, settings, api_provider)
                    orders_created.extend(orders)

                elif settings.service_type == ServiceType.REACTIONS:
                    orders = await self._create_advanced_reaction_orders(post, settings, api_provider)
                    orders_created.extend(orders)

                elif settings.service_type == ServiceType.REPOSTS:
                    orders = await self._create_repost_orders(post, settings, api_provider)
                    orders_created.extend(orders)

            except Exception as e:
                self.log_error(
                    "Failed to create orders",
                    error=e,
                    post_id=post.id,
                    service_type=settings.service_type
                )
                failed_services.append(settings.service_type)

        # Update post status
        if orders_created:
            await post_repo.update_status(post.id, PostStatus.COMPLETED, processed_at=True)
            self.log_info(
                "Post processed successfully",
                post_id=post.id,
                orders_created=len(orders_created),
                order_types=[o.service_type for o in orders_created]
            )
        else:
            await post_repo.update_status(post.id, PostStatus.FAILED)
            self.log_error(
                "No orders created for post",
                post_id=post.id,
                failed_services=failed_services
            )

    def _determine_api_provider(self, service_type: ServiceType, settings: ChannelSettings) -> APIProvider:
        """Determine which API to use based on service type"""
        if settings.api_preferences:
            provider = settings.api_preferences.get(service_type)
            if provider:
                return APIProvider(provider)

        # Defaults
        if service_type == ServiceType.VIEWS:
            return APIProvider.TWIBOOST
        elif service_type == ServiceType.REACTIONS:
            return APIProvider.NAKRUTOCHKA
        elif service_type == ServiceType.REPOSTS:
            return APIProvider.NAKRUTOCHKA

        return APIProvider.TWIBOOST

    async def _create_advanced_view_orders(
        self,
        post: Post,
        settings: ChannelSettings,
        api_provider: APIProvider
    ) -> List[Order]:
        """Create orders for views with advanced portion configuration"""

        # Check for duplicates
        existing_views = await order_repo.get_orders_by_post_and_type(post.id, ServiceType.VIEWS)
        if existing_views:
            self.log_warning(f"View orders already exist for post {post.id}")
            return []

        # Get metadata for portions
        metadata = await self._get_metadata(settings.channel_id, 'views')
        portions_config = metadata.get('portions', []) if metadata else []

        if not portions_config:
            # Fallback to simple portions
            return await self._create_simple_view_orders(post, settings, api_provider)

        # Get service ID
        service_ids = settings.twiboost_service_ids or {}
        service_id = service_ids.get("views", 4217)  # Your specific service ID

        orders = []

        for portion_config in portions_config:
            # Calculate scheduled time
            delay_minutes = portion_config.get('delay', 0)
            scheduled_at = datetime.now() + timedelta(minutes=delay_minutes) if delay_minutes > 0 else datetime.now()

            # Create order with drip-feed settings
            order_data = {
                "post_id": post.id,
                "service_type": ServiceType.VIEWS,
                "service_id": service_id,
                "api_provider": api_provider,
                "quantity": portion_config['runs'] * portion_config['quantity'],  # Total for this portion
                "actual_quantity": portion_config['runs'] * portion_config['quantity'],
                "portion_number": portion_config['number'],
                "portion_size": portion_config['quantity'],  # Per run
                "runs": portion_config['runs'],
                "interval": portion_config['interval'],
                "scheduled_at": scheduled_at,
                "post_link": post.link
            }

            order = await order_repo.create_order(order_data)
            if order:
                orders.append(order)

                self.log_info(
                    f"View portion {portion_config['number']} created",
                    order_id=order.id,
                    total_quantity=order.quantity,
                    runs=portion_config['runs'],
                    per_run=portion_config['quantity'],
                    interval=portion_config['interval'],
                    delay=delay_minutes,
                    link=post.link
                )

        return orders

    async def _create_simple_view_orders(
        self,
        post: Post,
        settings: ChannelSettings,
        api_provider: APIProvider
    ) -> List[Order]:
        """Fallback to simple view orders"""
        service_ids = settings.twiboost_service_ids or {}
        service_id = service_ids.get("views", 4217)

        order_data = {
            "post_id": post.id,
            "service_type": ServiceType.VIEWS,
            "service_id": service_id,
            "api_provider": api_provider,
            "quantity": settings.base_quantity,
            "actual_quantity": settings.base_quantity,
            "portion_number": 1,
            "portion_size": settings.base_quantity,
            "scheduled_at": datetime.now(),
            "post_link": post.link
        }

        order = await order_repo.create_order(order_data)
        return [order] if order else []

    async def _create_advanced_reaction_orders(
        self,
        post: Post,
        settings: ChannelSettings,
        api_provider: APIProvider
    ) -> List[Order]:
        """Create orders for reactions with multiple services"""

        # Check for duplicates
        existing_reactions = await order_repo.get_orders_by_post_and_type(post.id, ServiceType.REACTIONS)
        if existing_reactions:
            self.log_warning(f"Reaction orders already exist for post {post.id}")
            return []

        # Get metadata
        metadata = await self._get_metadata(settings.channel_id, 'reactions')
        reaction_distribution = metadata.get('reaction_distribution', {}) if metadata else {}

        if not reaction_distribution:
            # Fallback to simple reactions
            return []

        # Apply randomization to total
        total_reactions = settings.base_quantity
        if settings.randomization_percent > 0:
            min_val = int(total_reactions * (1 - settings.randomization_percent / 100))
            max_val = int(total_reactions * (1 + settings.randomization_percent / 100))
            total_reactions = random.randint(min_val, max_val)

        # Get service IDs
        service_ids = settings.nakrutochka_service_ids or {}

        orders = []

        # Create orders for each reaction type
        for reaction_type, percentage in reaction_distribution.items():
            quantity = int(total_reactions * percentage / 100)

            # Get service ID for this reaction type
            service_key = f"reaction_{reaction_type}"
            service_id = service_ids.get(service_key)

            if not service_id:
                self.log_warning(f"No service ID for reaction type {reaction_type}")
                continue

            # Calculate drip-feed
            runs = max(1, quantity // settings.drops_per_run)

            order_data = {
                "post_id": post.id,
                "service_type": ServiceType.REACTIONS,
                "service_id": service_id,
                "api_provider": api_provider,
                "quantity": quantity,
                "actual_quantity": quantity,
                "portion_number": 1,
                "portion_size": settings.drops_per_run,
                "reaction_emoji": reaction_type,  # Store reaction type
                "runs": runs,
                "interval": settings.run_interval,
                "scheduled_at": datetime.now(),
                "post_link": post.link
            }

            order = await order_repo.create_order(order_data)
            if order:
                orders.append(order)

                self.log_info(
                    f"Reaction order created for {reaction_type}",
                    order_id=order.id,
                    quantity=quantity,
                    service_id=service_id,
                    runs=runs,
                    interval=settings.run_interval,
                    link=post.link
                )

        return orders

    async def _create_repost_orders(
        self,
        post: Post,
        settings: ChannelSettings,
        api_provider: APIProvider
    ) -> List[Order]:
        """Create orders for reposts"""

        # Check for duplicates
        existing_reposts = await order_repo.get_orders_by_post_and_type(post.id, ServiceType.REPOSTS)
        if existing_reposts:
            self.log_warning(f"Repost orders already exist for post {post.id}")
            return []

        # Get service ID
        service_ids = settings.nakrutochka_service_ids or {}
        service_id = service_ids.get("reposts", 3943)  # Your specific service ID

        # Apply randomization
        quantity = settings.base_quantity
        if settings.randomization_percent > 0:
            min_val = int(quantity * (1 - settings.randomization_percent / 100))
            max_val = int(quantity * (1 + settings.randomization_percent / 100))
            quantity = random.randint(min_val, max_val)

        # Calculate drip-feed
        runs = max(1, quantity // settings.drops_per_run)

        # Schedule time (0 means immediate)
        scheduled_at = datetime.now() if settings.repost_delay_minutes == 0 else \
                      datetime.now() + timedelta(minutes=settings.repost_delay_minutes)

        order_data = {
            "post_id": post.id,
            "service_type": ServiceType.REPOSTS,
            "service_id": service_id,
            "api_provider": api_provider,
            "quantity": settings.base_quantity,
            "actual_quantity": quantity,
            "portion_number": 1,
            "portion_size": settings.drops_per_run,
            "runs": runs,
            "interval": settings.run_interval,
            "scheduled_at": scheduled_at,
            "post_link": post.link
        }

        order = await order_repo.create_order(order_data)

        if order:
            self.log_info(
                "Repost order created",
                order_id=order.id,
                quantity=quantity,
                runs=runs,
                scheduled_at=scheduled_at,
                service_id=service_id,
                link=post.link
            )
            return [order]

        return []

    async def _get_metadata(self, channel_id: int, service_type: str) -> Optional[Dict]:
        """Get metadata from channel settings"""
        query = """
            SELECT metadata
            FROM channel_settings
            WHERE channel_id = $1 AND service_type = $2
            LIMIT 1
        """

        row = await db.fetchrow(query, channel_id, service_type)

        if row and row['metadata']:
            return json.loads(row['metadata']) if isinstance(row['metadata'], str) else row['metadata']

        return None

    async def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        stats = await post_repo.get_processing_stats()

        stats.update({
            "session_processed": self.processed_count,
            "session_errors": self.error_count,
            "error_rate": (
                self.error_count / self.processed_count * 100
                if self.processed_count > 0 else 0
            ),
            "currently_processing": len(self.processing_posts)
        })

        return stats

    async def reprocess_failed_posts(self, limit: int = 10):
        """Retry processing failed posts"""
        failed_posts = await post_repo.get_posts_by_status(
            PostStatus.FAILED,
            limit=limit
        )

        if not failed_posts:
            self.log_info("No failed posts to reprocess")
            return 0

        self.log_info(f"Reprocessing {len(failed_posts)} failed posts")

        reprocess_count = 0
        for post in failed_posts:
            existing_orders = await order_repo.get_orders_by_post(post.id)
            if existing_orders:
                await post_repo.update_status(post.id, PostStatus.COMPLETED, processed_at=True)
                self.log_info(f"Post {post.id} has orders, marking as completed")
            else:
                await post_repo.update_status(post.id, PostStatus.NEW)
                reprocess_count += 1

        if reprocess_count > 0:
            return await self.process_new_posts(reprocess_count)
        return 0


# Import database connection
from src.database.connection import db

# Global processor instance
post_processor = PostProcessor()