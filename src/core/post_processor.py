"""
Post processor - creates orders for new posts
"""
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
import random

from src.database.models import (
    Post, PostStatus, Order, OrderStatus,
    ServiceType, ChannelSettings
)
from src.database.repositories.post_repo import post_repo
from src.database.repositories.channel_repo import channel_repo
from src.database.repositories.order_repo import order_repo
from src.database.repositories.service_repo import service_repo
from src.core.portion_calculator import portion_calculator
from src.services.twiboost_client import twiboost_client
from src.utils.logger import get_logger, LoggerMixin, metrics
from src.config import settings

logger = get_logger(__name__)


class PostProcessor(LoggerMixin):
    """Process new posts and create orders"""

    def __init__(self):
        self.processing_lock = asyncio.Lock()
        self.processed_count = 0
        self.error_count = 0

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
                    try:
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
        """Process single post"""
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

        # Create orders for each service type
        orders_created = []

        for settings in all_settings:
            try:
                if settings.service_type == ServiceType.VIEWS:
                    orders = await self._create_view_orders(post, settings)
                    orders_created.extend(orders)

                elif settings.service_type == ServiceType.REACTIONS:
                    orders = await self._create_reaction_orders(post, settings)
                    orders_created.extend(orders)

                elif settings.service_type == ServiceType.REPOSTS:
                    orders = await self._create_repost_orders(post, settings)
                    orders_created.extend(orders)

            except Exception as e:
                self.log_error(
                    "Failed to create orders",
                    error=e,
                    post_id=post.id,
                    service_type=settings.service_type
                )

        # Update post status
        if orders_created:
            await post_repo.update_status(post.id, PostStatus.COMPLETED, processed_at=True)
            self.log_info(
                "Post processed successfully",
                post_id=post.id,
                orders_created=len(orders_created)
            )
        else:
            await post_repo.update_status(post.id, PostStatus.FAILED)
            self.log_error(
                "No orders created for post",
                post_id=post.id
            )

    async def _create_view_orders(
            self,
            post: Post,
            settings: ChannelSettings
    ) -> List[Order]:
        """Create orders for views"""

        # Спочатку перевіряємо збережені service_ids
        service_ids = settings.twiboost_service_ids or {}
        service_id = service_ids.get("views")

        # Якщо немає збереженого - шукаємо в БД
        if not service_id:
            # Шукаємо найкращий сервіс для views в БД
            service = await service_repo.find_best_service(
                service_type="views",
                quantity=settings.base_quantity,
                name_filter="10 в минуту"  # Пріоритет для гнучкої швидкості
            )

            if not service:
                # Якщо не знайшли специфічний - беремо будь-який
                services = await service_repo.get_services_by_type("views")
                if services:
                    service = services[0]

            if service:
                service_id = service.service_id
                # Зберігаємо для наступного разу
                await channel_repo.update_service_ids(
                    post.channel_id,
                    ServiceType.VIEWS,
                    {"views": service_id}
                )
                self.log_info(
                    "View service selected from DB",
                    service_id=service_id,
                    service_name=service.name
                )
            else:
                raise ValueError("No view service found in database")

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
                "quantity": portion.quantity,
                "actual_quantity": portion.quantity,  # No randomization
                "portion_number": portion.number,
                "portion_size": portion.quantity,
                "scheduled_at": portion.scheduled_at,
                "post_link": post.link
            }

            order = await order_repo.create_order(order_data)
            orders.append(order)

            self.log_info(
                "View order created",
                order_id=order.id,
                portion=portion.number,
                quantity=portion.quantity,
                scheduled_at=portion.scheduled_at
            )

        return orders

    async def _create_reaction_orders(
            self,
            post: Post,
            settings: ChannelSettings
    ) -> List[Order]:
        """Create orders for reactions"""

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

        # Отримуємо маппінг реакцій з БД
        reaction_mapping = await service_repo.get_reaction_services()

        if not reaction_mapping:
            self.log_error("No reaction services found in database")
            return []

        # Create order for each reaction type
        orders = []
        for emoji, quantity in reaction_quantities.items():
            # Find service ID for this reaction
            service_key = f"reaction_{emoji}"
            service_id = reaction_mapping.get(service_key)

            if not service_id:
                # Спробуємо знайти в збережених налаштуваннях
                service_ids = settings.twiboost_service_ids or {}
                service_id = service_ids.get(service_key)

                if not service_id:
                    self.log_warning(f"No service found for reaction {emoji}")
                    continue

            # Calculate drip-feed parameters
            runs = settings.calculate_runs(quantity)

            order_data = {
                "post_id": post.id,
                "service_type": ServiceType.REACTIONS,
                "service_id": service_id,
                "quantity": settings.base_quantity,  # Original
                "actual_quantity": quantity,  # After randomization
                "portion_number": 1,
                "portion_size": quantity,
                "reaction_emoji": emoji,
                "runs": runs,
                "interval": settings.run_interval,
                "scheduled_at": datetime.now(),  # Start immediately
                "post_link": post.link
            }

            order = await order_repo.create_order(order_data)
            orders.append(order)

            self.log_info(
                "Reaction order created",
                order_id=order.id,
                emoji=emoji,
                quantity=quantity,
                runs=runs,
                interval=settings.run_interval,
                service_id=service_id
            )

        return orders

    async def _create_repost_orders(
            self,
            post: Post,
            settings: ChannelSettings
    ) -> List[Order]:
        """Create orders for reposts"""

        # Спочатку перевіряємо збережені service_ids
        service_ids = settings.twiboost_service_ids or {}
        service_id = service_ids.get("reposts")

        # Якщо немає збереженого - шукаємо в БД
        if not service_id:
            # Шукаємо сервіс для репостів в БД
            service = await service_repo.find_best_service(
                service_type="reposts",
                quantity=settings.base_quantity
            )

            if not service:
                # Беремо будь-який доступний
                services = await service_repo.get_services_by_type("reposts")
                if services:
                    service = services[0]

            if service:
                service_id = service.service_id
                # Зберігаємо для наступного разу
                await channel_repo.update_service_ids(
                    post.channel_id,
                    ServiceType.REPOSTS,
                    {"reposts": service_id}
                )
                self.log_info(
                    "Repost service selected from DB",
                    service_id=service_id,
                    service_name=service.name
                )
            else:
                self.log_warning("No repost service found in database")
                return []

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
            "quantity": settings.base_quantity,
            "actual_quantity": portion.quantity,
            "portion_number": 1,
            "portion_size": portion.quantity,
            "runs": runs,
            "interval": settings.run_interval,
            "scheduled_at": portion.scheduled_at,  # Has delay
            "post_link": post.link
        }

        order = await order_repo.create_order(order_data)

        self.log_info(
            "Repost order created",
            order_id=order.id,
            quantity=portion.quantity,
            runs=runs,
            scheduled_at=portion.scheduled_at,
            service_id=service_id
        )

        return [order]

    async def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        stats = await post_repo.get_processing_stats()

        stats.update({
            "session_processed": self.processed_count,
            "session_errors": self.error_count,
            "error_rate": (
                self.error_count / self.processed_count * 100
                if self.processed_count > 0 else 0
            )
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

        # Reset status to NEW and process again
        for post in failed_posts:
            await post_repo.update_status(post.id, PostStatus.NEW)

        return await self.process_new_posts(limit)


# Global processor instance
post_processor = PostProcessor()