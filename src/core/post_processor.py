"""
Post processor - creates orders for new posts with dual API support
"""
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
import random

from src.database.models import (
    Post, PostStatus, Order, OrderStatus,
    ServiceType, ChannelSettings, APIProvider
)
from src.database.repositories.post_repo import post_repo
from src.database.repositories.channel_repo import channel_repo
from src.database.repositories.order_repo import order_repo
from src.database.repositories.service_repo import service_repo
from src.core.portion_calculator import portion_calculator
from src.services.twiboost_client import twiboost_client
from src.services.nakrutochka_client import nakrutochka_client
from src.utils.logger import get_logger, LoggerMixin, metrics
from src.config import settings

logger = get_logger(__name__)


class PostProcessor(LoggerMixin):
    """Process new posts and create orders with dual API support"""

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

                # Determine API provider based on service type and settings
                api_provider = self._determine_api_provider(settings.service_type, settings)

                self.log_info(
                    f"Creating {settings.service_type} orders using {api_provider}",
                    post_id=post.id,
                    api_provider=api_provider
                )

                if settings.service_type == ServiceType.VIEWS:
                    orders = await self._create_view_orders(post, settings, api_provider)
                    orders_created.extend(orders)

                elif settings.service_type == ServiceType.REACTIONS:
                    orders = await self._create_reaction_orders(post, settings, api_provider)
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
                order_types=[o.service_type for o in orders_created],
                apis_used=list(set([o.api_provider for o in orders_created]))
            )
        else:
            if existing_orders:
                await post_repo.update_status(post.id, PostStatus.COMPLETED, processed_at=True)
                self.log_info(
                    "Post already has orders, marked as completed",
                    post_id=post.id
                )
            else:
                await post_repo.update_status(post.id, PostStatus.FAILED)
                self.log_error(
                    "No orders created for post",
                    post_id=post.id,
                    failed_services=failed_services
                )

    def _determine_api_provider(self, service_type: ServiceType, settings: ChannelSettings) -> APIProvider:
        """Determine which API to use based on service type and configuration"""

        # Check channel-specific preferences first
        if settings.api_preferences:
            provider = settings.api_preferences.get(service_type)
            if provider:
                return APIProvider(provider)

        # Use global settings
        if service_type == ServiceType.VIEWS:
            return APIProvider.TWIBOOST if settings.use_twiboost_for_views else APIProvider.NAKRUTOCHKA
        elif service_type == ServiceType.REACTIONS:
            return APIProvider.NAKRUTOCHKA if settings.use_nakrutochka_for_reactions else APIProvider.TWIBOOST
        elif service_type == ServiceType.REPOSTS:
            return APIProvider.NAKRUTOCHKA if settings.use_nakrutochka_for_reposts else APIProvider.TWIBOOST

        # Default fallback
        return APIProvider.TWIBOOST

    async def _create_view_orders(
        self,
        post: Post,
        settings: ChannelSettings,
        api_provider: APIProvider
    ) -> List[Order]:
        """Create orders for views (usually Twiboost)"""

        # Check for duplicates
        existing_views = await order_repo.get_orders_by_post_and_type(post.id, ServiceType.VIEWS)
        if existing_views:
            self.log_warning(f"View orders already exist for post {post.id}")
            return []

        # Get service with fallback support
        service_info = await service_repo.get_service_with_fallback(
            service_type="views",
            quantity=settings.base_quantity,
            preferred_api=api_provider
        )

        if not service_info:
            raise ValueError("No view service found in any API")

        service_id = service_info["service_id"]
        actual_api = service_info["api_provider"]

        self.log_info(
            f"View service selected from {actual_api}",
            service_id=service_id,
            originally_requested=api_provider
        )

        # Calculate portions (no randomization for views)
        portions = portion_calculator.calculate_portions(
            settings.base_quantity,
            settings,
            ServiceType.VIEWS
        )

        # Create order for each portion
        orders = []
        for portion in portions:
            order_data = {
                "post_id": post.id,
                "service_type": ServiceType.VIEWS,
                "service_id": service_id,
                "api_provider": actual_api,
                "quantity": portion.quantity,
                "actual_quantity": portion.quantity,
                "portion_number": portion.number,
                "portion_size": portion.quantity,
                "scheduled_at": portion.scheduled_at,
                "post_link": post.link
            }

            order = await order_repo.create_order(order_data)
            if order:
                orders.append(order)

                self.log_info(
                    "View order created",
                    order_id=order.id,
                    api_provider=actual_api,
                    portion=portion.number,
                    quantity=portion.quantity,
                    scheduled_at=portion.scheduled_at,
                    link=post.link
                )

        return orders

    async def _create_reaction_orders(
        self,
        post: Post,
        settings: ChannelSettings,
        api_provider: APIProvider
    ) -> List[Order]:
        """Create orders for reactions (usually Nakrutochka)"""

        # Check for duplicates
        existing_reactions = await order_repo.get_orders_by_post_and_type(post.id, ServiceType.REACTIONS)
        if existing_reactions:
            self.log_warning(f"Reaction orders already exist for post {post.id}")
            return []

        if not settings.reaction_distribution:
            self.log_warning("No reaction distribution configured")
            return []

        # Apply randomization to total
        total_reactions = portion_calculator._apply_randomization(
            settings.base_quantity,
            settings.randomization_percent,
            ServiceType.REACTIONS
        )

        # Distribute among reaction types
        reaction_quantities = portion_calculator.distribute_reactions(
            total_reactions,
            settings.reaction_distribution
        )

        # Create order for each reaction type
        orders = []
        for emoji, quantity in reaction_quantities.items():
            # Check if order for this emoji already exists
            existing_emoji_order = await order_repo.check_emoji_order_exists(post.id, emoji)
            if existing_emoji_order:
                self.log_warning(f"Order for reaction {emoji} already exists for post {post.id}")
                continue

            # Get service for specific emoji with fallback
            service_info = await service_repo.get_service_with_fallback(
                service_type="reactions",
                quantity=quantity,
                preferred_api=api_provider,
                emoji=emoji
            )

            if not service_info:
                self.log_warning(f"No service found for reaction {emoji}")
                continue

            service_id = service_info["service_id"]
            actual_api = service_info["api_provider"]

            # Calculate drip-feed parameters
            runs = settings.calculate_runs(quantity)

            order_data = {
                "post_id": post.id,
                "service_type": ServiceType.REACTIONS,
                "service_id": service_id,
                "api_provider": actual_api,
                "quantity": settings.base_quantity,
                "actual_quantity": quantity,
                "portion_number": 1,
                "portion_size": quantity,
                "reaction_emoji": emoji,
                "runs": runs,
                "interval": settings.run_interval,
                "scheduled_at": datetime.now(),
                "post_link": post.link
            }

            order = await order_repo.create_order(order_data)
            if order:
                orders.append(order)

                self.log_info(
                    "Reaction order created",
                    order_id=order.id,
                    api_provider=actual_api,
                    emoji=emoji,
                    quantity=quantity,
                    runs=runs,
                    interval=settings.run_interval,
                    service_id=service_id,
                    link=post.link
                )

        return orders

    async def _create_repost_orders(
        self,
        post: Post,
        settings: ChannelSettings,
        api_provider: APIProvider
    ) -> List[Order]:
        """Create orders for reposts (usually Nakrutochka)"""

        # Check for duplicates
        existing_reposts = await order_repo.get_orders_by_post_and_type(post.id, ServiceType.REPOSTS)
        if existing_reposts:
            self.log_warning(f"Repost orders already exist for post {post.id}")
            return []

        # Get service with fallback
        service_info = await service_repo.get_service_with_fallback(
            service_type="reposts",
            quantity=settings.base_quantity,
            preferred_api=api_provider
        )

        if not service_info:
            self.log_warning("No repost service found in any API")
            return []

        service_id = service_info["service_id"]
        actual_api = service_info["api_provider"]

        self.log_info(
            f"Repost service selected from {actual_api}",
            service_id=service_id,
            originally_requested=api_provider
        )

        # Calculate portions (with randomization)
        portions = portion_calculator.calculate_portions(
            settings.base_quantity,
            settings,
            ServiceType.REPOSTS
        )

        # Should be only one portion for reposts
        if not portions:
            return []

        portion = portions[0]

        # Calculate drip-feed
        runs = settings.calculate_runs(portion.quantity)

        order_data = {
            "post_id": post.id,
            "service_type": ServiceType.REPOSTS,
            "service_id": service_id,
            "api_provider": actual_api,
            "quantity": settings.base_quantity,
            "actual_quantity": portion.quantity,
            "portion_number": 1,
            "portion_size": portion.quantity,
            "runs": runs,
            "interval": settings.run_interval,
            "scheduled_at": portion.scheduled_at,
            "post_link": post.link
        }

        order = await order_repo.create_order(order_data)

        if order:
            self.log_info(
                "Repost order created",
                order_id=order.id,
                api_provider=actual_api,
                quantity=portion.quantity,
                runs=runs,
                scheduled_at=portion.scheduled_at,
                service_id=service_id,
                link=post.link
            )
            return [order]

        return []

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


# Global processor instance
post_processor = PostProcessor()