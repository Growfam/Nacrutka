"""
Order repository for database operations
"""
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import json

from src.database.connection import db
from src.database.models import Order, OrderStatus, ServiceType, ExecutionLog
from src.utils.logger import get_logger, LoggerMixin, metrics

logger = get_logger(__name__)


class OrderRepository(LoggerMixin):
    """Repository for order operations"""

    async def create_order(self, order_data: Dict[str, Any]) -> Order:
        """Create new order"""
        query = """
                INSERT INTO orders (post_id, twiboost_order_id, service_type, service_id, \
                                    quantity, actual_quantity, portion_number, portion_size, \
                                    runs, interval, reaction_emoji, status, scheduled_at) \
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING * \
                """

        row = await db.fetchrow(
            query,
            order_data["post_id"],
            order_data.get("twiboost_order_id"),
            order_data["service_type"],
            order_data["service_id"],
            order_data["quantity"],
            order_data["actual_quantity"],
            order_data.get("portion_number", 1),
            order_data.get("portion_size"),
            order_data.get("runs"),
            order_data.get("interval"),
            order_data.get("reaction_emoji"),
            OrderStatus.PENDING,
            order_data.get("scheduled_at")
        )

        order = self._row_to_order(row)

        self.log_info(
            "Order created",
            order_id=order.id,
            service_type=order.service_type,
            quantity=order.actual_quantity
        )

        # Log execution
        await self.create_execution_log(
            order.id,
            "order_created",
            {"quantity": order.actual_quantity, "service_id": order.service_id}
        )

        return order

    async def get_order(self, order_id: int) -> Optional[Order]:
        """Get order by ID"""
        query = "SELECT * FROM orders WHERE id = $1"
        row = await db.fetchrow(query, order_id)

        if row:
            return self._row_to_order(row)
        return None

    async def get_pending_orders(self, limit: int = 10) -> List[Order]:
        """Get pending orders ready for execution"""
        query = """
                SELECT * \
                FROM orders
                WHERE status = $1
                  AND (scheduled_at IS NULL OR scheduled_at <= NOW())
                  AND retry_count < $2
                ORDER BY scheduled_at ASC NULLS FIRST, created_at ASC
                    LIMIT $3 \
                """

        rows = await db.fetch(
            query,
            OrderStatus.PENDING,
            settings.max_retries,
            limit
        )

        orders = [self._row_to_order(row) for row in rows]
        self.log_debug(f"Found {len(orders)} pending orders")
        return orders

    async def get_orders_by_post(self, post_id: int) -> List[Order]:
        """Get all orders for a post"""
        query = """
                SELECT * \
                FROM orders
                WHERE post_id = $1
                ORDER BY portion_number, created_at \
                """
        rows = await db.fetch(query, post_id)

        return [self._row_to_order(row) for row in rows]

    async def get_active_orders(self) -> List[Order]:
        """Get all active orders (in progress)"""
        query = """
                SELECT * \
                FROM orders
                WHERE status IN ($1, $2)
                ORDER BY started_at DESC \
                """
        rows = await db.fetch(
            query,
            OrderStatus.IN_PROGRESS,
            OrderStatus.AWAITING
        )

        return [self._row_to_order(row) for row in rows]

    async def update_order_status(
            self,
            order_id: int,
            status: OrderStatus,
            twiboost_order_id: Optional[int] = None,
            response_data: Optional[Dict[str, Any]] = None,
            error_message: Optional[str] = None
    ):
        """Update order status and related fields"""
        updates = ["status = $2"]
        params = [order_id, status]
        param_count = 2

        if twiboost_order_id is not None:
            param_count += 1
            updates.append(f"twiboost_order_id = ${param_count}")
            params.append(twiboost_order_id)

        if response_data is not None:
            param_count += 1
            updates.append(f"response_data = ${param_count}")
            params.append(json.dumps(response_data))

        if error_message is not None:
            param_count += 1
            updates.append(f"error_message = ${param_count}")
            params.append(error_message)

        # Set timestamps based on status
        if status == OrderStatus.IN_PROGRESS:
            updates.append("started_at = NOW()")
        elif status in [OrderStatus.COMPLETED, OrderStatus.FAILED, OrderStatus.CANCELED]:
            updates.append("completed_at = NOW()")

        query = f"""
            UPDATE orders 
            SET {', '.join(updates)}
            WHERE id = $1
            RETURNING *
        """

        row = await db.fetchrow(query, *params)

        self.log_info(
            "Order status updated",
            order_id=order_id,
            status=status,
            twiboost_id=twiboost_order_id
        )

        # Log execution
        await self.create_execution_log(
            order_id,
            f"status_changed_to_{status}",
            {"twiboost_id": twiboost_order_id, "error": error_message}
        )

        return self._row_to_order(row) if row else None

    async def increment_retry_count(self, order_id: int):
        """Increment retry count for failed order"""
        query = """
                UPDATE orders
                SET retry_count = retry_count + 1
                WHERE id = $1 RETURNING retry_count \
                """

        new_count = await db.fetchval(query, order_id)

        self.log_info(
            "Retry count incremented",
            order_id=order_id,
            new_count=new_count
        )

        return new_count

    async def get_orders_to_check_status(self, limit: int = 50) -> List[Order]:
        """Get orders that need status check from Twiboost"""
        query = """
                SELECT * \
                FROM orders
                WHERE status IN ($1, $2)
                  AND twiboost_order_id IS NOT NULL
                  AND (updated_at < NOW() - INTERVAL '1 minute' OR updated_at IS NULL)
                ORDER BY updated_at ASC NULLS FIRST
                    LIMIT $3 \
                """

        rows = await db.fetch(
            query,
            OrderStatus.IN_PROGRESS,
            OrderStatus.AWAITING,
            limit
        )

        return [self._row_to_order(row) for row in rows]

    async def bulk_update_statuses(
            self,
            status_updates: Dict[int, Dict[str, Any]]
    ):
        """Bulk update order statuses from Twiboost response"""
        if not status_updates:
            return

        async with db.transaction():
            for order_id, data in status_updates.items():
                status = self._map_twiboost_status(data.get("status"))

                await self.update_order_status(
                    order_id,
                    status,
                    response_data=data
                )

    async def get_statistics(
            self,
            channel_id: Optional[int] = None,
            hours: int = 24
    ) -> Dict[str, Any]:
        """Get order statistics"""
        if channel_id:
            query = """
                SELECT 
                    o.status,
                    o.service_type,
                    COUNT(*) as count,
                    SUM(o.actual_quantity) as total_quantity
                FROM orders o
                JOIN posts p ON o.post_id = p.id
                WHERE p.channel_id = $1
                    AND o.created_at > NOW() - INTERVAL '%s hours'
                GROUP BY o.status, o.service_type
            """ % hours
            rows = await db.fetch(query, channel_id)
        else:
            query = """
                SELECT 
                    status,
                    service_type,
                    COUNT(*) as count,
                    SUM(actual_quantity) as total_quantity
                FROM orders
                WHERE created_at > NOW() - INTERVAL '%s hours'
                GROUP BY status, service_type
            """ % hours
            rows = await db.fetch(query)

        # Process results
        stats = {
            "by_status": {},
            "by_service": {},
            "total_orders": 0,
            "total_quantity": 0
        }

        for row in rows:
            status = row["status"]
            service = row["service_type"]
            count = row["count"]
            quantity = row["total_quantity"] or 0

            # By status
            if status not in stats["by_status"]:
                stats["by_status"][status] = {"count": 0, "quantity": 0}
            stats["by_status"][status]["count"] += count
            stats["by_status"][status]["quantity"] += quantity

            # By service
            if service not in stats["by_service"]:
                stats["by_service"][service] = {"count": 0, "quantity": 0}
            stats["by_service"][service]["count"] += count
            stats["by_service"][service]["quantity"] += quantity

            # Totals
            stats["total_orders"] += count
            stats["total_quantity"] += quantity

        return stats

    # ========== Execution Logs ==========

    async def create_execution_log(
            self,
            order_id: int,
            action: str,
            details: Dict[str, Any]
    ):
        """Create execution log entry"""
        query = """
                INSERT INTO execution_logs (order_id, action, details)
                VALUES ($1, $2, $3) \
                """

        await db.execute(query, order_id, action, json.dumps(details))

    async def get_execution_logs(
            self,
            order_id: int,
            limit: int = 100
    ) -> List[ExecutionLog]:
        """Get execution logs for order"""
        query = """
                SELECT * \
                FROM execution_logs
                WHERE order_id = $1
                ORDER BY created_at DESC
                    LIMIT $2 \
                """

        rows = await db.fetch(query, order_id, limit)

        return [self._row_to_log(row) for row in rows]

    # ========== Helper Methods ==========

    def _row_to_order(self, row) -> Order:
        """Convert database row to Order model"""
        return Order(
            id=row["id"],
            post_id=row["post_id"],
            twiboost_order_id=row["twiboost_order_id"],
            service_type=ServiceType(row["service_type"]),
            service_id=row["service_id"],
            quantity=row["quantity"],
            actual_quantity=row["actual_quantity"],
            portion_number=row["portion_number"],
            portion_size=row["portion_size"],
            runs=row["runs"],
            interval=row["interval"],
            status=OrderStatus(row["status"]),
            scheduled_at=row["scheduled_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            response_data=json.loads(row["response_data"]) if row["response_data"] else None,
            error_message=row["error_message"],
            reaction_emoji=row.get("reaction_emoji")
        )

    def _row_to_log(self, row) -> ExecutionLog:
        """Convert database row to ExecutionLog model"""
        return ExecutionLog(
            id=row["id"],
            order_id=row["order_id"],
            action=row["action"],
            details=json.loads(row["details"]) if row["details"] else {},
            created_at=row["created_at"]
        )

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


# Import settings after class definition to avoid circular import
from src.config import settings

# Global repository instance
order_repo = OrderRepository()