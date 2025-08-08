"""
Simplified Nakrutka API client
"""
import aiohttp
import asyncio
import logging
from typing import Dict, Any, List, Optional

from src.config import settings

logger = logging.getLogger(__name__)


class NakrutkaClient:
    """Simple client for Nakrutka API"""

    def __init__(self):
        self.api_url = settings.nakrutka_api_url
        self.api_key = settings.nakrutka_api_key
        self.session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self):
        """Create session if needed"""
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def close(self):
        """Close session"""
        if self.session and not self.session.closed:
            await self.session.close()

    async def _make_request(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make API request"""
        await self._ensure_session()

        # Add API key
        data['key'] = self.api_key

        # Prepare form data
        form_data = aiohttp.FormData()
        for key, value in data.items():
            form_data.add_field(key, str(value))

        try:
            async with self.session.post(
                    self.api_url,
                    data=form_data,
                    timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                text = await response.text()

                # Parse JSON
                import json
                result = json.loads(text) if text else {}

                # Check for error
                if 'error' in result:
                    logger.error(f"Nakrutka API error: {result['error']}")
                    raise Exception(result['error'])

                return result

        except asyncio.TimeoutError:
            logger.error("Nakrutka API timeout")
            raise
        except Exception as e:
            logger.error(f"Nakrutka API request failed: {e}")
            raise

    async def create_order(
            self,
            service: int,
            link: str,
            quantity: int,
            runs: Optional[int] = None,
            interval: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create new order"""

        data = {
            'action': 'add',
            'service': service,
            'link': link,
            'quantity': quantity
        }

        # Add drip-feed if specified
        if runs and runs > 1:
            data['runs'] = runs
            data['interval'] = interval or 0

        logger.debug(f"Creating order: service={service}, quantity={quantity}, runs={runs}")

        result = await self._make_request(data)

        # Validate response
        if 'order' not in result:
            raise Exception(f"No order ID in response: {result}")

        logger.info(f"Order created: {result['order']}, charge: ${result.get('charge', 0)}")

        return result

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """Get single order status"""

        data = {
            'action': 'status',
            'order': order_id
        }

        return await self._make_request(data)

    async def get_multiple_status(self, order_ids: List[str]) -> Dict[str, Any]:
        """Get multiple orders status"""

        if not order_ids:
            return {}

        data = {
            'action': 'status',
            'orders': ','.join(str(oid) for oid in order_ids)
        }

        result = await self._make_request(data)

        # Result should be dict with order_id as keys
        return result if isinstance(result, dict) else {}

    async def get_balance(self) -> Dict[str, Any]:
        """Get account balance"""

        data = {'action': 'balance'}

        result = await self._make_request(data)

        balance = float(result.get('balance', 0))
        currency = result.get('currency', 'USD')

        logger.info(f"Balance: {balance} {currency}")

        return result

    async def get_services(self) -> List[Dict[str, Any]]:
        """Get available services"""

        data = {'action': 'services'}

        result = await self._make_request(data)

        # Result should be list of services
        if not isinstance(result, list):
            logger.error(f"Invalid services response: {type(result)}")
            return []

        return result