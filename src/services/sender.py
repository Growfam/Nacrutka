"""
Order sender - sends ready orders to Nakrutka API
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from src.config import settings
from src.database.connection import db
from src.services.nakrutka import NakrutkaClient

logger = logging.getLogger(__name__)


class OrderSender:
    """Sends orders from database to Nakrutka"""

    def __init__(self):
        self.db = db
        self.nakrutka = NakrutkaClient()
        self.processing = set()  # Track orders being processed

    async def send_pending_orders(self):
        """Main function to send pending orders"""
        try:
            # Get pending orders
            orders = await self.db.fetch("""
                SELECT id, service_id, quantity, runs, interval, link, 
                       service_type, emoji, portion_number
                FROM ready_orders
                WHERE status = 'pending'
                ORDER BY created_at
                LIMIT $1
            """, settings.max_orders_per_batch)

            if not orders:
                logger.debug("No pending orders")
                return

            logger.info(f"Processing {len(orders)} pending orders")

            # Send each order
            for order in orders:
                # Skip if already processing
                if order['id'] in self.processing:
                    continue

                self.processing.add(order['id'])

                try:
                    await self.send_single_order(order)
                    await asyncio.sleep(1)  # Rate limiting

                except Exception as e:
                    logger.error(f"Failed to send order {order['id']}: {e}")
                    await self.mark_order_failed(order['id'], str(e))

                finally:
                    self.processing.discard(order['id'])

        except Exception as e:
            logger.error(f"Send orders failed: {e}")

    async def send_single_order(self, order: Dict[str, Any]):
        """Send single order to Nakrutka"""

        order_id = order['id']

        # Log attempt
        logger.info(
            f"Sending order {order_id}: "
            f"{order['service_type']} x{order['quantity']} "
            f"(runs={order['runs']}, interval={order['interval']})"
        )

        # Prepare API call
        api_data = {
            'service': order['service_id'],
            'link': order['link'],
            'quantity': order['quantity']
        }

        # Add drip-feed if needed
        if order['runs'] > 1:
            api_data['runs'] = order['runs']
            api_data['interval'] = order['interval']

        # Send to Nakrutka
        result = await self.nakrutka.create_order(**api_data)

        if result and 'order' in result:
            nakrutka_order_id = str(result['order'])

            # Update order status
            await self.db.execute("""
                UPDATE ready_orders 
                SET status = 'sent',
                    nakrutka_order_id = $1,
                    sent_at = $2
                WHERE id = $3
            """, nakrutka_order_id, datetime.utcnow(), order_id)

            # Log to database
            await self.db.execute("""
                INSERT INTO logs (level, message, context)
                VALUES ('info', 'Order sent successfully', $1)
            """, {
                'order_id': order_id,
                'nakrutka_id': nakrutka_order_id,
                'service_type': order['service_type'],
                'quantity': order['quantity'],
                'charge': result.get('charge', 0),
                'balance': result.get('balance', 0)
            })

            logger.info(
                f"Order {order_id} sent successfully. "
                f"Nakrutka ID: {nakrutka_order_id}, "
                f"Charge: ${result.get('charge', 0)}"
            )

        else:
            raise Exception(f"Invalid API response: {result}")

    async def mark_order_failed(self, order_id: int, error: str):
        """Mark order as failed"""
        await self.db.execute("""
            UPDATE ready_orders 
            SET status = 'failed'
            WHERE id = $1
        """, order_id)

        await self.db.execute("""
            INSERT INTO logs (level, message, context)
            VALUES ('error', 'Order failed', $1)
        """, {
            'order_id': order_id,
            'error': error
        })

    async def send_scheduled_orders(self):
        """Send orders that were scheduled for later"""
        # Get scheduled orders based on delay from config
        orders = await self.db.fetch("""
            SELECT ro.*, cc.config
            FROM ready_orders ro
            JOIN posts p ON p.id = ro.post_id
            JOIN channel_config cc ON cc.channel_id = p.channel_id
            WHERE ro.status = 'scheduled'
        """)

        for order in orders:
            config = order['config']

            # Check if it's time to send based on portion delay
            if order['service_type'] == 'views':
                portions = config.get('views', {}).get('portions', [])
                for portion in portions:
                    if portion.get('number') == order['portion_number']:
                        delay_minutes = portion.get('delay', 0)

                        # Check if enough time passed
                        time_passed = (datetime.utcnow() - order['created_at']).total_seconds() / 60

                        if time_passed >= delay_minutes:
                            # Change status to pending
                            await self.db.execute("""
                                UPDATE ready_orders
                                SET status = 'pending'
                                WHERE id = $1
                            """, order['id'])

                            logger.info(f"Order {order['id']} moved from scheduled to pending")