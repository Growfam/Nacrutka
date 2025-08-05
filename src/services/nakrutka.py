"""
Nakrutka API client with retry logic and better error handling
"""
import aiohttp
import json
from typing import Dict, Any, Optional, List
from urllib.parse import urlencode
import asyncio
from datetime import datetime

from src.config import settings
from src.utils.logger import get_logger
from src.utils.helpers import async_retry, RateLimiter

logger = get_logger(__name__)


class NakrutkaError(Exception):
    """Custom exception for Nakrutka API errors"""
    pass


class NakrutkaClient:
    """Client for Nakrutka API with retry and rate limiting"""

    def __init__(self):
        self.api_url = settings.nakrutka_api_url
        self.api_key = settings.nakrutka_api_key
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limiter = RateLimiter(calls_per_minute=60)

        # Response cache for balance/services
        self._cache = {}
        self._cache_ttl = 300  # 5 minutes

    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            connector=aiohttp.TCPConnector(limit=10)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    @async_retry(max_attempts=3, exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    async def _make_request(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make request to Nakrutka API with retry logic"""
        # Rate limiting
        await self.rate_limiter.acquire()

        # Add API key
        data['key'] = self.api_key

        # Prepare form data
        form_data = aiohttp.FormData()
        for key, value in data.items():
            form_data.add_field(key, str(value))

        # Create session if not exists
        if not self.session:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=10)
            )

        try:
            async with self.session.post(
                self.api_url,
                data=form_data,
                headers={
                    'User-Agent': 'Mozilla/5.0 (compatible; TelegramSMMBot/1.0)',
                    'Accept': 'application/json'
                }
            ) as response:
                response_text = await response.text()

                # Log request details
                logger.debug(
                    "Nakrutka API request",
                    action=data.get('action'),
                    status=response.status,
                    response_preview=response_text[:200] if response_text else "empty"
                )

                # Check HTTP status
                if response.status >= 500:
                    raise aiohttp.ClientError(f"Server error: {response.status}")

                if response.status == 429:
                    # Rate limited
                    logger.warning("Nakrutka rate limit hit")
                    await asyncio.sleep(60)  # Wait 1 minute
                    raise aiohttp.ClientError("Rate limited")

                # Parse JSON response
                try:
                    result = json.loads(response_text)
                except json.JSONDecodeError:
                    logger.error(
                        "Invalid JSON response from Nakrutka",
                        response=response_text,
                        status=response.status
                    )
                    raise NakrutkaError(f"Invalid API response: {response_text[:100]}")

                # Check for API errors
                if isinstance(result, dict) and result.get('error'):
                    error_msg = result.get('error')
                    error_code = result.get('error_code')

                    logger.error(
                        "Nakrutka API error",
                        error=error_msg,
                        error_code=error_code
                    )

                    # Handle specific errors
                    if 'Incorrect API key' in error_msg:
                        raise NakrutkaError("Invalid API key")
                    elif 'Insufficient funds' in error_msg:
                        raise NakrutkaError("Insufficient balance")
                    elif 'quantity' in error_msg.lower():
                        raise NakrutkaError(f"Quantity error: {error_msg}")
                    else:
                        raise NakrutkaError(f"API error: {error_msg}")

                return result

        except aiohttp.ClientError as e:
            logger.error("HTTP request failed", error=str(e), url=self.api_url)
            raise
        except asyncio.TimeoutError:
            logger.error("Request timeout", url=self.api_url)
            raise

    async def create_order(
        self,
        service_id: int,
        link: str,
        quantity: int,
        runs: Optional[int] = None,
        interval: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create new order with validation"""
        # Validate inputs
        if not service_id or service_id <= 0:
            raise ValueError(f"Invalid service_id: {service_id}")

        if not link or not link.startswith(('http://', 'https://')):
            raise ValueError(f"Invalid link: {link}")

        if quantity <= 0:
            raise ValueError(f"Invalid quantity: {quantity}")

        if runs is not None and runs <= 0:
            raise ValueError(f"Invalid runs: {runs}")

        if interval is not None and interval <= 0:
            raise ValueError(f"Invalid interval: {interval}")

        # Build request data
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
                "Creating drip-feed order",
                service_id=service_id,
                total_quantity=quantity * runs,
                quantity_per_run=quantity,
                runs=runs,
                interval=interval
            )
        else:
            logger.info(
                "Creating regular order",
                service_id=service_id,
                quantity=quantity
            )

        try:
            result = await self._make_request(data)

            # Validate response
            if not isinstance(result, dict):
                raise NakrutkaError(f"Unexpected response type: {type(result)}")

            if 'order' not in result:
                raise NakrutkaError(f"No order ID in response: {result}")

            logger.info(
                "Order created successfully",
                order_id=result.get('order'),
                charge=result.get('charge'),
                currency=result.get('currency'),
                service_id=service_id
            )

            return result

        except NakrutkaError:
            raise
        except Exception as e:
            logger.error(
                "Failed to create order",
                error=str(e),
                service_id=service_id,
                quantity=quantity
            )
            raise NakrutkaError(f"Order creation failed: {str(e)}")

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """Get order status with caching"""
        cache_key = f"status_{order_id}"

        # Check cache (short TTL for status)
        if cache_key in self._cache:
            cached_time, cached_data = self._cache[cache_key]
            if (datetime.utcnow() - cached_time).total_seconds() < 30:
                return cached_data

        data = {
            'action': 'status',
            'order': order_id
        }

        try:
            result = await self._make_request(data)

            # Cache result
            self._cache[cache_key] = (datetime.utcnow(), result)

            logger.debug(
                "Order status retrieved",
                order_id=order_id,
                status=result.get('status'),
                start_count=result.get('start_count'),
                remains=result.get('remains')
            )

            return result

        except Exception as e:
            logger.error(f"Failed to get order status: {e}")
            raise

    async def get_multiple_status(self, order_ids: List[str]) -> Dict[str, Any]:
        """Get status for multiple orders efficiently"""
        if not order_ids:
            return {}

        # Limit batch size
        batch_size = 100
        all_results = {}

        for i in range(0, len(order_ids), batch_size):
            batch = order_ids[i:i + batch_size]

            data = {
                'action': 'status',
                'orders': ','.join(batch)
            }

            try:
                result = await self._make_request(data)

                # Merge results
                if isinstance(result, dict):
                    all_results.update(result)

            except Exception as e:
                logger.error(
                    f"Failed to get batch status for {len(batch)} orders",
                    error=str(e)
                )
                # Continue with other batches

        return all_results

    async def get_services(self) -> List[Dict[str, Any]]:
        """Get all available services with caching"""
        cache_key = "services"

        # Check cache
        if cache_key in self._cache:
            cached_time, cached_data = self._cache[cache_key]
            if (datetime.utcnow() - cached_time).total_seconds() < self._cache_ttl:
                logger.debug("Using cached services")
                return cached_data

        data = {'action': 'services'}

        try:
            result = await self._make_request(data)

            if not isinstance(result, list):
                raise NakrutkaError(f"Expected list of services, got {type(result)}")

            # Cache result
            self._cache[cache_key] = (datetime.utcnow(), result)

            logger.info(f"Retrieved {len(result)} services from Nakrutka")
            return result

        except Exception as e:
            logger.error(f"Failed to get services: {e}")
            raise

    async def get_balance(self) -> Dict[str, Any]:
        """Get account balance with caching"""
        cache_key = "balance"

        # Check cache (short TTL for balance)
        if cache_key in self._cache:
            cached_time, cached_data = self._cache[cache_key]
            if (datetime.utcnow() - cached_time).total_seconds() < 60:
                return cached_data

        data = {'action': 'balance'}

        try:
            result = await self._make_request(data)

            # Validate response
            if not isinstance(result, dict) or 'balance' not in result:
                raise NakrutkaError(f"Invalid balance response: {result}")

            # Cache result
            self._cache[cache_key] = (datetime.utcnow(), result)

            balance = float(result.get('balance', 0))
            currency = result.get('currency', 'USD')

            logger.info(f"Balance: {balance} {currency}")

            # Warn if low balance
            if balance < 10:
                logger.warning(f"Low balance warning: {balance} {currency}")

            return result

        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            raise

    async def refill_order(self, order_id: str) -> Dict[str, Any]:
        """Refill an order"""
        data = {
            'action': 'refill',
            'order': order_id
        }

        try:
            result = await self._make_request(data)

            logger.info(
                "Order refilled",
                order_id=order_id,
                refill_id=result.get('refill')
            )

            return result

        except Exception as e:
            logger.error(f"Failed to refill order: {e}")
            raise

    async def cancel_orders(self, order_ids: List[str]) -> Dict[str, Any]:
        """Cancel multiple orders"""
        if not order_ids:
            return {}

        data = {
            'action': 'cancel',
            'orders': ','.join(order_ids)
        }

        try:
            result = await self._make_request(data)

            logger.info(
                "Orders cancelled",
                count=len(order_ids),
                results=result
            )

            return result

        except Exception as e:
            logger.error(f"Failed to cancel orders: {e}")
            raise

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
                - delay_minutes: Initial delay (handled separately)
        """
        results = []

        for i, portion in enumerate(portions):
            try:
                # Log portion details
                portion_total = portion['quantity_per_run'] * portion['runs']
                logger.info(
                    f"Creating portion {i + 1}/{len(portions)}",
                    quantity_per_run=portion['quantity_per_run'],
                    runs=portion['runs'],
                    interval=portion['interval'],
                    total=portion_total
                )

                # Create order
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
                    'result': result,
                    'total_quantity': portion_total
                })

                # Small delay between portions
                if i < len(portions) - 1:
                    await asyncio.sleep(1)

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

        # Log summary
        success_count = sum(1 for r in results if r['success'])
        total_ordered = sum(
            r.get('total_quantity', 0)
            for r in results
            if r['success']
        )

        logger.info(
            "Drip-feed order completed",
            success_portions=f"{success_count}/{len(portions)}",
            total_quantity_ordered=total_ordered,
            target_quantity=total_quantity
        )

        return results

    async def validate_service_params(
        self,
        service_id: int,
        quantity: int
    ) -> Dict[str, Any]:
        """Validate order parameters against service limits"""
        try:
            # Get services from cache or API
            services = await self.get_services()

            # Find service
            service = next(
                (s for s in services if s.get('service') == service_id),
                None
            )

            if not service:
                return {
                    'valid': False,
                    'error': f"Service {service_id} not found"
                }

            # Check limits
            min_qty = int(service.get('min', 1))
            max_qty = int(service.get('max', 1000000))

            if quantity < min_qty:
                return {
                    'valid': False,
                    'error': f"Quantity {quantity} below minimum {min_qty}",
                    'min': min_qty,
                    'max': max_qty
                }

            if quantity > max_qty:
                return {
                    'valid': False,
                    'error': f"Quantity {quantity} above maximum {max_qty}",
                    'min': min_qty,
                    'max': max_qty
                }

            return {
                'valid': True,
                'service': service,
                'min': min_qty,
                'max': max_qty
            }

        except Exception as e:
            logger.error(f"Failed to validate service params: {e}")
            return {
                'valid': False,
                'error': str(e)
            }

    async def check_service_availability(self, service_id: int) -> bool:
        """Check if service is available"""
        try:
            services = await self.get_services()
            service = next(
                (s for s in services if s.get('service') == service_id),
                None
            )

            if not service:
                logger.warning(f"Service {service_id} not found")
                return False

            # Check if service is active (some services may have status field)
            if 'status' in service and service['status'] != 'Active':
                logger.warning(
                    f"Service {service_id} is not active",
                    status=service.get('status')
                )
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to check service availability: {e}")
            return False

    def clear_cache(self):
        """Clear response cache"""
        self._cache.clear()
        logger.debug("Cache cleared")