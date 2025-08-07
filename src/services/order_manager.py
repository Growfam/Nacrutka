"""
Order manager - handles order execution and status tracking
"""
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from src.database.models import Order, OrderStatus, Post
from src.database.repositories.order_repo import order_repo
from src.database.repositories.post_repo import post_repo
from src.services.twiboost_client import twiboost_client, TwiboostAPIError
from src.utils.logger import get_logger, LoggerMixin, metrics
from src.config import settings


logger = get_logger(__name__)


class OrderManager(LoggerMixin):
    """Manages order execution and status tracking"""

    def __init__(self):
        self.execution_lock = asyncio.Lock()
        self.active_orders = {}
        self.execution_count = 0
        self.error_count = 0

    async def execute_pending_orders(self, limit: int = 10) -> int:
        """Execute pending orders"""
        async with self.execution_lock:
            try:
                # Get pending orders
                orders = await order_repo.get_pending_orders(limit)

                if not orders:
                    self.log_debug("No pending orders to execute")
                    return 0

                self.log_info(f"Executing {len(orders)} pending orders")

                # Execute orders concurrently with limit
                semaphore = asyncio.Semaphore(settings.max_concurrent_orders)

                async def execute_with_limit(order):
                    async with semaphore:
                        return await self._execute_single_order(order)

                tasks = [execute_with_limit(order) for order in orders]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Count successful executions
                executed = sum(1 for r in results if r and not isinstance(r, Exception))

                self.log_info(
                    "Batch execution completed",
                    executed=executed,
                    total=len(orders),
                    total_executed=self.execution_count
                )

                return executed

            except Exception as e:
                self.log_error("Batch execution failed", error=e)
                return 0

    async def _execute_single_order(self, order: Order) -> bool:
        """Execute single order"""
        try:
            self.log_info(
                "Executing order",
                order_id=order.id,
                service_type=order.service_type,
                quantity=order.actual_quantity
            )

            # Get post for link
            post = await post_repo.get_post(order.post_id)
            if not post:
                raise ValueError(f"Post not found: {order.post_id}")

            # Update status to in progress
            await order_repo.update_order_status(
                order.id,
                OrderStatus.IN_PROGRESS
            )

            # Prepare parameters for Twiboost
            if order.runs and order.interval:
                # Drip-feed order
                twiboost_order_id = await twiboost_client.create_order(
                    service_id=order.service_id,
                    link=post.link,
                    quantity=order.actual_quantity,
                    runs=order.runs,
                    interval=order.interval
                )
            else:
                # Regular order
                twiboost_order_id = await twiboost_client.create_order(
                    service_id=order.service_id,
                    link=post.link,
                    quantity=order.actual_quantity
                )

            # Update order with Twiboost ID
            await order_repo.update_order_status(
                order.id,
                OrderStatus.IN_PROGRESS,
                twiboost_order_id=twiboost_order_id
            )

            # Track active order
            self.active_orders[order.id] = twiboost_order_id

            self.log_info(
                "Order executed successfully",
                order_id=order.id,
                twiboost_id=twiboost_order_id
            )

            self.execution_count += 1

            # Update post statistics
            if order.service_type == "views":
                await post_repo.update_stats(post.id, views=order.actual_quantity)
            elif order.service_type == "reactions":
                await post_repo.update_stats(post.id, reactions=order.actual_quantity)
            elif order.service_type == "reposts":
                await post_repo.update_stats(post.id, reposts=order.actual_quantity)

            return True

        except TwiboostAPIError as e:
            self.log_error(
                "Twiboost API error",
                error=e,
                order_id=order.id
            )

            # Update retry count
            retry_count = await order_repo.increment_retry_count(order.id)

            if retry_count >= settings.max_retries:
                # Max retries reached, mark as failed
                await order_repo.update_order_status(
                    order.id,
                    OrderStatus.FAILED,
                    error_message=str(e)
                )
            else:
                # Reset to pending for retry
                await order_repo.update_order_status(
                    order.id,
                    OrderStatus.PENDING,
                    error_message=f"Retry {retry_count}: {str(e)}"
                )

            self.error_count += 1
            metrics.log_error_rate("order_execution", 1)
            return False

        except Exception as e:
            self.log_error(
                "Order execution failed",
                error=e,
                order_id=order.id
            )

            await order_repo.update_order_status(
                order.id,
                OrderStatus.FAILED,
                error_message=str(e)
            )

            self.error_count += 1
            return False

    async def check_orders_status(self, limit: int = 50) -> int:
        """Check status of active orders from Twiboost"""
        try:
            # Get orders to check
            orders = await order_repo.get_orders_to_check_status(limit)

            if not orders:
                self.log_debug("No orders to check status")
                return 0

            self.log_debug(f"Checking status for {len(orders)} orders")

            # Group orders by Twiboost ID for batch check
            twiboost_ids = [o.twiboost_order_id for o in orders if o.twiboost_order_id]

            if not twiboost_ids:
                return 0

            # Get statuses from Twiboost
            statuses = await twiboost_client.get_multiple_orders_status(twiboost_ids)

            # Update orders based on status
            updated = 0
            for order in orders:
                if order.twiboost_order_id in statuses:
                    status_data = statuses[order.twiboost_order_id]

                    if isinstance(status_data, dict):
                        # Map Twiboost status to our status
                        new_status = self._map_twiboost_status(status_data.get("status"))

                        if new_status != order.status:
                            await order_repo.update_order_status(
                                order.id,
                                new_status,
                                response_data=status_data
                            )

                            # Remove from active if completed
                            if new_status in [OrderStatus.COMPLETED, OrderStatus.FAILED, OrderStatus.CANCELED]:
                                self.active_orders.pop(order.id, None)

                            updated += 1

                            self.log_info(
                                "Order status updated",
                                order_id=order.id,
                                old_status=order.status,
                                new_status=new_status
                            )

            self.log_info(f"Updated {updated} order statuses")
            return updated

        except Exception as e:
            self.log_error("Status check failed", error=e)
            return 0

    def _map_twiboost_status(self, twiboost_status: str) -> OrderStatus:
        """Map Twiboost status to our OrderStatus"""
        mapping = {
            "In progress": OrderStatus.IN_PROGRESS,
            "Completed": OrderStatus.COMPLETED,
            "Awaiting": OrderStatus.AWAITING,
            "Canceled": OrderStatus.CANCELED,
            "Fail": OrderStatus.FAILED,
            "Partial": OrderStatus.PARTIAL
        }

        return mapping.get(twiboost_status, OrderStatus.FAILED)

    async def cancel_order(self, order_id: int) -> bool:
        """Cancel order"""
        try:
            order = await order_repo.get_order(order_id)

            if not order:
                self.log_error("Order not found", order_id=order_id)
                return False

            if not order.twiboost_order_id:
                # Order not sent to Twiboost yet
                await order_repo.update_order_status(
                    order_id,
                    OrderStatus.CANCELED
                )
                return True

            # Cancel in Twiboost
            success = await twiboost_client.cancel_order(order.twiboost_order_id)

            if success:
                await order_repo.update_order_status(
                    order_id,
                    OrderStatus.CANCELED
                )

                # Remove from active orders
                self.active_orders.pop(order_id, None)

                self.log_info("Order canceled", order_id=order_id)
                return True
            else:
                self.log_error("Failed to cancel order", order_id=order_id)
                return False

        except Exception as e:
            self.log_error("Cancel order failed", error=e, order_id=order_id)
            return False

    async def retry_failed_orders(self, limit: int = 10) -> int:
        """Retry failed orders"""
        try:
            # Get failed orders with retry count < max
            query = """
                SELECT * FROM orders 
                WHERE status = $1 
                    AND retry_count < $2
                ORDER BY created_at DESC
                LIMIT $3
            """

            rows = await db.fetch(
                query,
                OrderStatus.FAILED,
                settings.max_retries,
                limit
            )

            if not rows:
                self.log_info("No failed orders to retry")
                return 0

            # Reset to pending for retry
            order_ids = [row["id"] for row in rows]
            await order_repo.bulk_update_statuses(
                {oid: {"status": OrderStatus.PENDING} for oid in order_ids}
            )

            self.log_info(f"Reset {len(order_ids)} failed orders for retry")

            # Execute them
            return await self.execute_pending_orders(limit)

        except Exception as e:
            self.log_error("Retry failed orders error", error=e)
            return 0

    async def get_execution_stats(self) -> Dict[str, Any]:
        """Get execution statistics"""
        stats = await order_repo.get_statistics()

        stats.update({
            "session_executed": self.execution_count,
            "session_errors": self.error_count,
            "active_orders": len(self.active_orders),
            "success_rate": (
                self.execution_count / (self.execution_count + self.error_count) * 100
                if (self.execution_count + self.error_count) > 0 else 0
            )
        })

        return stats

    async def cleanup_completed_orders(self, days: int = 7):
        """Clean up old completed orders"""
        query = """
            DELETE FROM orders 
            WHERE status = $1 
                AND completed_at < NOW() - INTERVAL '%s days'
        """ % days

        result = await db.execute(query, OrderStatus.COMPLETED)

        # Extract count
        count = int(result.split()[-1]) if result else 0

        self.log_info(f"Cleaned up {count} completed orders")
        return count


# Import db after class definition
from src.database.connection import db

# Global manager instance
order_manager = OrderManager()