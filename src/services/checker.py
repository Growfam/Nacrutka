"""
Status checker - checks order status from Nakrutka
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from src.config import settings
from src.database.connection import db
from src.services.nakrutka import NakrutkaClient

logger = logging.getLogger(__name__)


class StatusChecker:
    """Checks status of sent orders"""

    def __init__(self):
        self.db = db
        self.nakrutka = NakrutkaClient()

    async def check_order_status(self):
        """Check status of all sent orders"""
        try:
            # Get sent orders
            orders = await self.db.fetch("""
                                         SELECT id, nakrutka_order_id, service_type, quantity
                                         FROM ready_orders
                                         WHERE status = 'sent'
                                           AND nakrutka_order_id IS NOT NULL
                                         ORDER BY sent_at LIMIT 100
                                         """)

            if not orders:
                logger.debug("No orders to check")
                return

            logger.info(f"Checking status of {len(orders)} orders")

            # Group by Nakrutka order IDs for batch check
            nakrutka_ids = [o['nakrutka_order_id'] for o in orders]

            # Check status in batches of 20
            for i in range(0, len(nakrutka_ids), 20):
                batch = nakrutka_ids[i:i + 20]

                try:
                    # Get statuses from Nakrutka
                    statuses = await self.nakrutka.get_multiple_status(batch)

                    # Process each status
                    for order in orders[i:i + 20]:
                        nakrutka_id = order['nakrutka_order_id']

                        if nakrutka_id in statuses:
                            status_info = statuses[nakrutka_id]
                            await self.process_status_update(order, status_info)

                    await asyncio.sleep(2)  # Rate limiting

                except Exception as e:
                    logger.error(f"Failed to check batch status: {e}")

        except Exception as e:
            logger.error(f"Status check failed: {e}")

    async def process_status_update(self, order: Dict[str, Any], status_info: Dict[str, Any]):
        """Process status update from Nakrutka"""

        order_id = order['id']
        nakrutka_status = status_info.get('status', '').lower()

        # Map Nakrutka status to our status
        if nakrutka_status in ['completed', 'complete']:
            new_status = 'completed'
        elif nakrutka_status in ['partial']:
            new_status = 'partial'
        elif nakrutka_status in ['canceled', 'cancelled', 'refunded']:
            new_status = 'failed'
        elif nakrutka_status in ['pending', 'processing', 'in progress']:
            new_status = 'sent'  # Keep as sent
        else:
            logger.warning(f"Unknown status '{nakrutka_status}' for order {order_id}")
            return

        # Update if status changed
        if new_status in ['completed', 'partial', 'failed']:
            await self.db.execute("""
                                  UPDATE ready_orders
                                  SET status       = $1,
                                      completed_at = $2
                                  WHERE id = $3
                                  """, new_status, datetime.utcnow(), order_id)

            # Log status change
            await self.db.execute("""
                                  INSERT INTO logs (level, message, context)
                                  VALUES ('info', 'Order status updated', $1)
                                  """, {
                                      'order_id': order_id,
                                      'old_status': 'sent',
                                      'new_status': new_status,
                                      'nakrutka_status': nakrutka_status,
                                      'start_count': status_info.get('start_count', 0),
                                      'remains': status_info.get('remains', 0)
                                  })

            logger.info(
                f"Order {order_id} status updated to {new_status} "
                f"(remains: {status_info.get('remains', 0)})"
            )

    async def check_scheduled_orders(self):
        """Move scheduled orders to pending when time comes"""

        # Get all scheduled orders with their configs
        orders = await self.db.fetch("""
                                     SELECT ro.id,
                                            ro.portion_number,
                                            ro.service_type,
                                            ro.created_at,
                                            cc.config
                                     FROM ready_orders ro
                                              JOIN posts p ON p.id = ro.post_id
                                              JOIN channel_config cc ON cc.channel_id = p.channel_id
                                     WHERE ro.status = 'scheduled'
                                     """)

        for order in orders:
            delay_minutes = 0

            # Get delay from config based on service type
            config = order['config']

            if order['service_type'] == 'views':
                portions = config.get('views', {}).get('portions', [])
                for portion in portions:
                    if portion.get('number') == order['portion_number']:
                        delay_minutes = portion.get('delay', 0)
                        break

            elif order['service_type'] == 'reposts':
                delay_minutes = config.get('reposts', {}).get('delay', 0)

            # Check if it's time to activate
            time_passed = (datetime.utcnow() - order['created_at']).total_seconds() / 60

            if time_passed >= delay_minutes:
                await self.db.execute("""
                                      UPDATE ready_orders
                                      SET status = 'pending'
                                      WHERE id = $1
                                      """, order['id'])

                logger.info(f"Scheduled order {order['id']} is now pending")

    async def cleanup_old_orders(self):
        """Mark very old sent orders as completed"""

        # Orders sent more than 7 days ago
        cutoff_date = datetime.utcnow() - timedelta(days=7)

        result = await self.db.execute("""
                                       UPDATE ready_orders
                                       SET status       = 'completed',
                                           completed_at = NOW()
                                       WHERE status = 'sent'
                                         AND sent_at < $1
                                       """, cutoff_date)

        # Parse affected rows from result
        if result and 'UPDATE' in result:
            count = int(result.split()[-1])
            if count > 0:
                logger.info(f"Cleaned up {count} old orders")

    async def get_statistics(self) -> Dict[str, Any]:
        """Get current statistics"""

        stats = await self.db.fetchrow("""
                                       SELECT COUNT(CASE WHEN status = 'pending' THEN 1 END)   as pending,
                                              COUNT(CASE WHEN status = 'scheduled' THEN 1 END) as scheduled,
                                              COUNT(CASE WHEN status = 'sent' THEN 1 END)      as sent,
                                              COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                                              COUNT(CASE WHEN status = 'failed' THEN 1 END)    as failed,
                                              COUNT(*)                                         as total
                                       FROM ready_orders
                                       WHERE created_at > NOW() - INTERVAL '24 hours'
                                       """)

        return dict(stats) if stats else {}