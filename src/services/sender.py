"""
Order sender - sends ready orders to Nakrutka API (FIXED)
"""
import asyncio
import logging
import json
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

        try:
            # Send to Nakrutka API
            result = await self.nakrutka.create_order(
                service=order['service_id'],
                link=order['link'],
                quantity=order['quantity'],
                runs=order['runs'] if order['runs'] > 1 else None,
                interval=order['interval'] if order['runs'] > 1 else None
            )

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

                # Log success to database - ВИПРАВЛЕНО: використовуємо JSON для context
                context_data = {
                    'order_id': order_id,
                    'nakrutka_id': nakrutka_order_id,
                    'service_type': order['service_type'],
                    'quantity': order['quantity'],
                    'charge': result.get('charge', 0),
                    'balance': result.get('balance', 0)
                }

                await self.db.execute("""
                    INSERT INTO logs (level, message, context)
                    VALUES ($1, $2, $3::jsonb)
                """, 'info', 'Order sent successfully', json.dumps(context_data))

                logger.info(
                    f"Order created: {nakrutka_order_id}, charge: ${result.get('charge', 0)}"
                )

            else:
                raise Exception(f"Invalid API response: {result}")

        except Exception as e:
            # ВИПРАВЛЕНО: правильна обробка помилок
            error_message = str(e)
            logger.error(f"Failed to send order {order_id}: {error_message}")

            # Зберігаємо помилку в БД - використовуємо JSON для context
            error_context = {
                'order_id': order_id,
                'error': error_message[:500]  # Обмежуємо довжину помилки
            }

            await self.db.execute("""
                INSERT INTO logs (level, message, context)
                VALUES ($1, $2, $3::jsonb)
            """, 'error', f'Failed to send order {order_id}', json.dumps(error_context))

            # Помічаємо замовлення як failed
            await self.mark_order_failed(order_id, error_message)

    async def mark_order_failed(self, order_id: int, error: str):
        """Mark order as failed"""
        await self.db.execute("""
            UPDATE ready_orders 
            SET status = 'failed',
                error_message = $2
            WHERE id = $1
        """, order_id, error[:500])  # Обмежуємо довжину помилки

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

            elif order['service_type'] == 'reposts':
                delay_minutes = config.get('reposts', {}).get('delay', 0)
                time_passed = (datetime.utcnow() - order['created_at']).total_seconds() / 60

                if time_passed >= delay_minutes:
                    await self.db.execute("""
                        UPDATE ready_orders
                        SET status = 'pending'
                        WHERE id = $1
                    """, order['id'])

                    logger.info(f"Repost order {order['id']} moved to pending")