"""
Nakrutka API client
"""
import aiohttp
import json
from typing import Dict, Any, Optional, List
from urllib.parse import urlencode

from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class NakrutkaClient:
    """Client for Nakrutka API"""

    def __init__(self):
        self.api_url = settings.nakrutka_api_url
        self.api_key = settings.nakrutka_api_key
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    async def _make_request(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make request to Nakrutka API"""
        # Add API key to request
        data['key'] = self.api_key

        # Prepare form data
        form_data = aiohttp.FormData()
        for key, value in data.items():
            form_data.add_field(key, str(value))

        # Create session if not exists
        if not self.session:
            self.session = aiohttp.ClientSession()

        try:
            async with self.session.post(
                    self.api_url,
                    data=form_data,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (compatible; TelegramSMMBot/1.0)'
                    }
            ) as response:
                response_text = await response.text()

                # Log request and response
                logger.debug(
                    "Nakrutka API request",
                    action=data.get('action'),
                    status=response.status,
                    response_preview=response_text[:200]
                )

                # Parse JSON response
                try:
                    result = json.loads(response_text)
                except json.JSONDecodeError:
                    logger.error(
                        "Invalid JSON response from Nakrutka",
                        response=response_text
                    )
                    raise Exception(f"Invalid API response: {response_text}")

                # Check for errors
                if isinstance(result, dict) and result.get('error'):
                    error_msg = result.get('error')
                    logger.error("Nakrutka API error", error=error_msg)
                    raise Exception(f"Nakrutka API error: {error_msg}")

                return result

        except aiohttp.ClientError as e:
            logger.error("HTTP request failed", error=str(e))
            raise

    async def create_order(
            self,
            service_id: int,
            link: str,
            quantity: int,
            runs: Optional[int] = None,
            interval: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create new order"""
        data = {
            'action': 'add',
            'service': service_id,
            'link': link,
            'quantity': quantity
        }

        # Add drip-feed parameters if provided
        if runs and interval:
            data['runs'] = runs
            data['interval'] = interval

        logger.info(
            "Creating Nakrutka order",
            service_id=service_id,
            quantity=quantity,
            runs=runs,
            interval=interval
        )

        result = await self._make_request(data)

        logger.info(
            "Nakrutka order created",
            order_id=result.get('order'),
            charge=result.get('charge'),
            currency=result.get('currency')
        )

        return result

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """Get order status"""
        data = {
            'action': 'status',
            'order': order_id
        }

        result = await self._make_request(data)

        logger.debug(
            "Order status retrieved",
            order_id=order_id,
            status=result.get('status'),
            remains=result.get('remains')
        )

        return result

    async def get_multiple_status(self, order_ids: List[str]) -> Dict[str, Any]:
        """Get status for multiple orders"""
        data = {
            'action': 'status',
            'orders': ','.join(order_ids)
        }

        result = await self._make_request(data)
        return result

    async def get_services(self) -> List[Dict[str, Any]]:
        """Get all available services"""
        data = {'action': 'services'}
        result = await self._make_request(data)

        logger.info(f"Retrieved {len(result)} services from Nakrutka")
        return result

    async def get_balance(self) -> Dict[str, Any]:
        """Get account balance"""
        data = {'action': 'balance'}
        result = await self._make_request(data)

        logger.info(
            "Balance retrieved",
            balance=result.get('balance'),
            currency=result.get('currency')
        )

        return result

    async def refill_order(self, order_id: str) -> Dict[str, Any]:
        """Refill an order"""
        data = {
            'action': 'refill',
            'order': order_id
        }

        result = await self._make_request(data)

        logger.info(
            "Order refilled",
            order_id=order_id,
            refill_id=result.get('refill')
        )

        return result

    async def cancel_orders(self, order_ids: List[str]) -> Dict[str, Any]:
        """Cancel multiple orders"""
        data = {
            'action': 'cancel',
            'orders': ','.join(order_ids)
        }

        result = await self._make_request(data)

        logger.info(
            "Orders cancelled",
            count=len(order_ids),
            results=result
        )

        return result

    async def create_drip_feed_order(
            self,
            service_id: int,
            link: str,
            total_quantity: int,
            portions: List[Dict[str, int]]
    ) -> List[Dict[str, Any]]:
        """
        Create order with custom drip-feed portions

        Args:
            service_id: Service ID from Nakrutka
            link: Target URL
            total_quantity: Total quantity to order
            portions: List of dicts with keys:
                - quantity_per_run: Amount per run
                - runs: Number of runs
                - interval: Minutes between runs
        """
        results = []

        for i, portion in enumerate(portions):
            try:
                result = await self.create_order(
                    service_id=service_id,
                    link=link,
                    quantity=portion['quantity_per_run'],
                    runs=portion['runs'],
                    interval=portion['interval']
                )

                results.append({
                    'portion_number': i + 1,
                    'order_id': result.get('order'),
                    'success': True,
                    'result': result
                })

            except Exception as e:
                logger.error(
                    f"Failed to create portion {i + 1}",
                    error=str(e),
                    portion=portion
                )
                results.append({
                    'portion_number': i + 1,
                    'success': False,
                    'error': str(e)
                })

        return results

    async def validate_order_params(
            self,
            service_id: int,
            quantity: int,
            min_quantity: int,
            max_quantity: int
    ) -> bool:
        """Validate order parameters against service limits"""
        if quantity < min_quantity:
            logger.warning(
                "Quantity below minimum",
                quantity=quantity,
                min_quantity=min_quantity
            )
            return False

        if quantity > max_quantity:
            logger.warning(
                "Quantity above maximum",
                quantity=quantity,
                max_quantity=max_quantity
            )
            return False

        return True