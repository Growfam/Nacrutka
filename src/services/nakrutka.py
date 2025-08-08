"""
Nakrutka API client with enhanced error handling and validation
"""
import aiohttp
import json
from typing import Dict, Any, Optional, List, Union
from urllib.parse import urlencode
import asyncio
from datetime import datetime
from decimal import Decimal

from src.config import settings
from src.utils.logger import get_logger
from src.utils.helpers import async_retry, RateLimiter

logger = get_logger(__name__)


# Custom exceptions for better error handling
class NakrutkaError(Exception):
    """Base exception for Nakrutka API errors"""
    pass


class NakrutkaAuthError(NakrutkaError):
    """Authentication error"""
    pass


class NakrutkaBalanceError(NakrutkaError):
    """Insufficient balance error"""
    pass


class NakrutkaServiceError(NakrutkaError):
    """Service-related error"""
    pass


class NakrutkaRateLimitError(NakrutkaError):
    """Rate limit exceeded error"""
    pass


class NakrutkaValidationError(NakrutkaError):
    """Validation error"""
    pass


class NakrutkaConnectionError(NakrutkaError):
    """Connection error"""
    pass


class NakrutkaClient:
    """Client for Nakrutka API with enhanced error handling and validation"""

    def __init__(self):
        self.api_url = settings.nakrutka_api_url
        self.api_key = settings.nakrutka_api_key
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limiter = RateLimiter(calls_per_minute=60)

        # Response cache for balance/services
        self._cache = {}
        self._cache_ttl = 300  # 5 minutes

        # Service validation cache
        self._service_limits_cache = {}

        # Connection settings
        self.timeout = aiohttp.ClientTimeout(total=30)
        self.max_retries = 3
        self.retry_delay = 5

    async def __aenter__(self):
        """Async context manager entry"""
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    async def _ensure_session(self):
        """Ensure session exists"""
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=self.timeout,
                connector=aiohttp.TCPConnector(
                    limit=10,
                    force_close=True,
                    enable_cleanup_closed=True
                )
            )

    async def close(self):
        """Close session"""
        if self.session and not self.session.closed:
            await self.session.close()

    def _classify_error(self, error_msg: str, status_code: Optional[int] = None) -> Exception:
        """Classify error and return appropriate exception"""
        error_lower = error_msg.lower()

        # Authentication errors
        if any(term in error_lower for term in ['api key', 'invalid key', 'unauthorized']):
            return NakrutkaAuthError(error_msg)

        # Balance errors
        if any(term in error_lower for term in ['insufficient funds', 'balance', 'not enough']):
            return NakrutkaBalanceError(error_msg)

        # Rate limit errors
        if 'rate limit' in error_lower or status_code == 429:
            return NakrutkaRateLimitError(error_msg)

        # Service errors
        if any(term in error_lower for term in ['service', 'not found', 'disabled']):
            return NakrutkaServiceError(error_msg)

        # Validation errors
        if any(term in error_lower for term in ['quantity', 'minimum', 'maximum', 'invalid']):
            return NakrutkaValidationError(error_msg)

        # Connection errors
        if any(term in error_lower for term in ['timeout', 'connection', 'network']):
            return NakrutkaConnectionError(error_msg)

        # Default
        return NakrutkaError(error_msg)

    @async_retry(
        max_attempts=3,
        exceptions=(aiohttp.ClientError, asyncio.TimeoutError, NakrutkaConnectionError)
    )
    async def _make_request(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make request to Nakrutka API with enhanced error handling"""
        # Rate limiting
        await self.rate_limiter.acquire()

        # Validate API key
        if not self.api_key:
            raise NakrutkaAuthError("API key not configured")

        # Add API key
        data['key'] = self.api_key

        # Prepare form data
        form_data = aiohttp.FormData()
        for key, value in data.items():
            form_data.add_field(key, str(value))

        # Ensure session
        await self._ensure_session()

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
                    response_length=len(response_text) if response_text else 0
                )

                # Check HTTP status
                if response.status >= 500:
                    raise self._classify_error(
                        f"Server error: {response.status}",
                        response.status
                    )

                if response.status == 429:
                    # Rate limited
                    retry_after = response.headers.get('Retry-After', '60')
                    logger.warning(f"Rate limited, retry after {retry_after}s")
                    raise NakrutkaRateLimitError(f"Rate limited, retry after {retry_after}s")

                if response.status == 401:
                    raise NakrutkaAuthError("Unauthorized - check API key")

                if response.status == 400:
                    # Bad request - likely validation error
                    try:
                        error_data = json.loads(response_text) if response_text else {}
                        error_msg = error_data.get('error', 'Bad request')
                    except:
                        error_msg = f"Bad request: {response_text[:100]}"
                    raise NakrutkaValidationError(error_msg)

                # Parse JSON response
                try:
                    result = json.loads(response_text) if response_text else {}
                except json.JSONDecodeError as e:
                    logger.error(
                        "Invalid JSON response from Nakrutka",
                        response_preview=response_text[:200],
                        status=response.status,
                        json_error=str(e)
                    )
                    raise NakrutkaError(f"Invalid API response format")

                # Check for API errors in response
                if isinstance(result, dict) and result.get('error'):
                    error_msg = result.get('error', 'Unknown error')
                    error_code = result.get('error_code')

                    logger.error(
                        "Nakrutka API returned error",
                        error=error_msg,
                        error_code=error_code,
                        action=data.get('action')
                    )

                    # Classify and raise appropriate error
                    raise self._classify_error(error_msg)

                return result

        except aiohttp.ClientError as e:
            logger.error(
                "HTTP request failed",
                error=str(e),
                error_type=type(e).__name__,
                url=self.api_url
            )
            raise NakrutkaConnectionError(f"Connection failed: {str(e)}")
        except asyncio.TimeoutError:
            logger.error("Request timeout", url=self.api_url)
            raise NakrutkaConnectionError("Request timed out")
        except NakrutkaError:
            # Re-raise our custom errors
            raise
        except Exception as e:
            logger.error(
                "Unexpected error in request",
                error=str(e),
                error_type=type(e).__name__
            )
            raise NakrutkaError(f"Unexpected error: {str(e)}")

    def _validate_service_id(self, service_id: Union[int, str]) -> int:
        """Validate and convert service ID"""
        try:
            service_id = int(service_id)
            if service_id <= 0:
                raise ValueError("Service ID must be positive")
            return service_id
        except (ValueError, TypeError) as e:
            raise NakrutkaValidationError(f"Invalid service ID: {service_id}")

    def _validate_link(self, link: str) -> str:
        """Validate link format"""
        if not link:
            raise NakrutkaValidationError("Link cannot be empty")

        link = link.strip()

        # Check if it's a valid URL
        if not link.startswith(('http://', 'https://')):
            raise NakrutkaValidationError(f"Link must start with http:// or https://: {link}")

        # Validate Telegram link
        if 't.me' not in link and 'telegram' not in link.lower():
            logger.warning(f"Link doesn't appear to be a Telegram link: {link}")

        return link

    def _validate_quantity(self, quantity: Union[int, str], min_limit: int = 1, max_limit: int = 1000000) -> int:
        """Validate quantity"""
        try:
            quantity = int(quantity)
            if quantity < min_limit:
                raise NakrutkaValidationError(
                    f"Quantity {quantity} below minimum {min_limit}"
                )
            if quantity > max_limit:
                raise NakrutkaValidationError(
                    f"Quantity {quantity} above maximum {max_limit}"
                )
            return quantity
        except (ValueError, TypeError):
            raise NakrutkaValidationError(f"Invalid quantity: {quantity}")

    async def create_order(
        self,
        service_id: Union[int, str],
        link: str,
        quantity: Union[int, str],
        runs: Optional[int] = None,
        interval: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create new order with comprehensive validation"""
        # Validate all inputs
        service_id = self._validate_service_id(service_id)
        link = self._validate_link(link)

        # Get service limits from cache or API
        service_info = await self._get_service_info(service_id)
        min_quantity = service_info.get('min', 1)
        max_quantity = service_info.get('max', 1000000)

        quantity = self._validate_quantity(quantity, min_quantity, max_quantity)

        # Validate drip-feed parameters
        if runs is not None:
            if runs <= 0:
                raise NakrutkaValidationError(f"Runs must be positive: {runs}")
            if runs > 100:
                raise NakrutkaValidationError(f"Runs exceeds maximum (100): {runs}")

        if interval is not None:
            if interval < 0:
                raise NakrutkaValidationError(f"Interval cannot be negative: {interval}")
            if runs and runs > 1 and interval < 5:
                raise NakrutkaValidationError(
                    f"Interval {interval} too small for drip-feed (minimum 5)"
                )

        # Additional validation for drip-feed total
        if runs:
            total_quantity = quantity * runs
            if total_quantity > max_quantity:
                raise NakrutkaValidationError(
                    f"Total quantity {total_quantity} exceeds service maximum {max_quantity}"
                )

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
                link=link[:50] + "...",
                total_quantity=quantity * runs,
                quantity_per_run=quantity,
                runs=runs,
                interval=interval
            )
        else:
            logger.info(
                "Creating regular order",
                service_id=service_id,
                link=link[:50] + "...",
                quantity=quantity
            )

        try:
            result = await self._make_request(data)

            # Validate response structure
            if not isinstance(result, dict):
                raise NakrutkaError(f"Unexpected response type: {type(result)}")

            order_id = result.get('order')
            if not order_id:
                raise NakrutkaError(f"No order ID in response: {result}")

            # Log success
            logger.info(
                "Order created successfully",
                order_id=order_id,
                charge=result.get('charge'),
                currency=result.get('currency'),
                balance=result.get('balance')
            )

            # Clear cache as balance changed
            self._cache.pop('balance', None)

            return result

        except NakrutkaError:
            raise
        except Exception as e:
            logger.error(
                "Failed to create order",
                error=str(e),
                error_type=type(e).__name__,
                service_id=service_id
            )
            raise NakrutkaError(f"Order creation failed: {str(e)}")

    async def get_order_status(self, order_id: Union[str, int]) -> Dict[str, Any]:
        """Get order status with validation"""
        if not order_id:
            raise NakrutkaValidationError("Order ID cannot be empty")

        order_id = str(order_id)

        # Check cache (short TTL for status)
        cache_key = f"status_{order_id}"
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

            # Validate response
            if not isinstance(result, dict):
                raise NakrutkaError(f"Invalid status response format")

            # Cache result
            self._cache[cache_key] = (datetime.utcnow(), result)

            # Log status
            status = result.get('status', 'unknown')
            logger.debug(
                "Order status retrieved",
                order_id=order_id,
                status=status,
                start_count=result.get('start_count', 0),
                remains=result.get('remains', 0)
            )

            return result

        except NakrutkaError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to get order status",
                error=str(e),
                order_id=order_id
            )
            raise NakrutkaError(f"Status check failed: {str(e)}")

    async def get_multiple_status(self, order_ids: List[Union[str, int]]) -> Dict[str, Any]:
        """Get status for multiple orders with validation"""
        if not order_ids:
            return {}

        # Convert and validate order IDs
        validated_ids = []
        for order_id in order_ids:
            if order_id:
                validated_ids.append(str(order_id))

        if not validated_ids:
            return {}

        # Process in batches
        batch_size = 100
        all_results = {}

        for i in range(0, len(validated_ids), batch_size):
            batch = validated_ids[i:i + batch_size]

            data = {
                'action': 'status',
                'orders': ','.join(batch)
            }

            try:
                result = await self._make_request(data)

                # Validate response
                if not isinstance(result, dict):
                    logger.error(
                        f"Invalid batch status response",
                        batch_size=len(batch),
                        response_type=type(result)
                    )
                    continue

                # Merge results
                all_results.update(result)

            except NakrutkaError as e:
                logger.error(
                    f"Failed to get batch status",
                    error=str(e),
                    batch_size=len(batch)
                )
                # Continue with other batches
            except Exception as e:
                logger.error(
                    f"Unexpected error in batch status",
                    error=str(e),
                    error_type=type(e).__name__
                )

        return all_results

    async def get_services(self) -> List[Dict[str, Any]]:
        """Get all available services with caching and validation"""
        cache_key = "services"

        # Check cache
        if cache_key in self._cache:
            cached_time, cached_data = self._cache[cache_key]
            if (datetime.utcnow() - cached_time).total_seconds() < self._cache_ttl:
                logger.debug(f"Using cached services ({len(cached_data)} items)")
                return cached_data

        data = {'action': 'services'}

        try:
            result = await self._make_request(data)

            # Validate response
            if not isinstance(result, list):
                raise NakrutkaError(
                    f"Expected list of services, got {type(result).__name__}"
                )

            # Validate each service
            valid_services = []
            for service in result:
                if not isinstance(service, dict):
                    logger.warning(f"Invalid service format: {type(service)}")
                    continue

                # Required fields
                if not all(key in service for key in ['service', 'name', 'min', 'max', 'rate']):
                    logger.warning(f"Service missing required fields: {service}")
                    continue

                valid_services.append(service)

            # Update service limits cache
            for service in valid_services:
                service_id = service.get('service')
                if service_id:
                    self._service_limits_cache[service_id] = {
                        'min': int(service.get('min', 1)),
                        'max': int(service.get('max', 1000000)),
                        'rate': float(service.get('rate', 0)),
                        'name': service.get('name', ''),
                        'category': service.get('category', ''),
                        'type': service.get('type', '')
                    }

            # Cache result
            self._cache[cache_key] = (datetime.utcnow(), valid_services)

            logger.info(
                f"Retrieved {len(valid_services)} valid services from Nakrutka",
                total_received=len(result),
                categories=len(set(s.get('category', '') for s in valid_services))
            )

            return valid_services

        except NakrutkaError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to get services",
                error=str(e),
                error_type=type(e).__name__
            )
            raise NakrutkaError(f"Failed to retrieve services: {str(e)}")

    async def get_balance(self) -> Dict[str, Any]:
        """Get account balance with validation and caching"""
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
            if not isinstance(result, dict):
                raise NakrutkaError(f"Invalid balance response format")

            if 'balance' not in result:
                raise NakrutkaError(f"No balance field in response")

            # Validate balance value
            try:
                balance = float(result.get('balance', 0))
                if balance < 0:
                    logger.error(f"Negative balance received: {balance}")
                    balance = 0
            except (ValueError, TypeError):
                raise NakrutkaError(f"Invalid balance value: {result.get('balance')}")

            currency = result.get('currency', 'USD')

            # Update result with validated values
            result['balance'] = balance
            result['currency'] = currency

            # Cache result
            self._cache[cache_key] = (datetime.utcnow(), result)

            # Log balance
            logger.info(
                f"Balance retrieved",
                balance=f"{balance:.2f}",
                currency=currency
            )

            # Warnings for low balance
            if balance < 1:
                logger.error(f"Critical: Balance too low: {balance} {currency}")
            elif balance < 10:
                logger.warning(f"Low balance warning: {balance} {currency}")

            return result

        except NakrutkaError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to get balance",
                error=str(e),
                error_type=type(e).__name__
            )
            raise NakrutkaError(f"Balance check failed: {str(e)}")

    async def refill_order(self, order_id: Union[str, int]) -> Dict[str, Any]:
        """Refill an order with validation"""
        if not order_id:
            raise NakrutkaValidationError("Order ID cannot be empty")

        order_id = str(order_id)

        data = {
            'action': 'refill',
            'order': order_id
        }

        try:
            result = await self._make_request(data)

            # Validate response
            if not isinstance(result, dict):
                raise NakrutkaError("Invalid refill response format")

            refill_id = result.get('refill')
            if not refill_id:
                raise NakrutkaError("No refill ID in response")

            logger.info(
                "Order refilled successfully",
                order_id=order_id,
                refill_id=refill_id
            )

            return result

        except NakrutkaError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to refill order",
                error=str(e),
                order_id=order_id
            )
            raise NakrutkaError(f"Refill failed: {str(e)}")

    async def cancel_orders(self, order_ids: List[Union[str, int]]) -> Dict[str, Any]:
        """Cancel multiple orders with validation"""
        if not order_ids:
            return {}

        # Validate order IDs
        validated_ids = []
        for order_id in order_ids:
            if order_id:
                validated_ids.append(str(order_id))

        if not validated_ids:
            return {}

        data = {
            'action': 'cancel',
            'orders': ','.join(validated_ids)
        }

        try:
            result = await self._make_request(data)

            logger.info(
                "Orders cancel request sent",
                count=len(validated_ids),
                results=result
            )

            return result

        except NakrutkaError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to cancel orders",
                error=str(e),
                count=len(validated_ids)
            )
            raise NakrutkaError(f"Cancel failed: {str(e)}")

    async def _get_service_info(self, service_id: int) -> Dict[str, Any]:
        """Get service info from cache or API"""
        # Check cache first
        if service_id in self._service_limits_cache:
            return self._service_limits_cache[service_id]

        # Get from API
        services = await self.get_services()

        # Find service
        for service in services:
            if service.get('service') == service_id:
                return {
                    'min': int(service.get('min', 1)),
                    'max': int(service.get('max', 1000000)),
                    'rate': float(service.get('rate', 0)),
                    'name': service.get('name', ''),
                    'category': service.get('category', '')
                }

        # Not found - return defaults
        logger.warning(f"Service {service_id} not found in API response")
        return {
            'min': 1,
            'max': 1000000,
            'rate': 0,
            'name': f'Unknown service {service_id}'
        }

    async def validate_service_params(
        self,
        service_id: int,
        quantity: int
    ) -> Dict[str, Any]:
        """Validate order parameters against service limits"""
        try:
            service_info = await self._get_service_info(service_id)

            min_qty = service_info['min']
            max_qty = service_info['max']

            if quantity < min_qty:
                return {
                    'valid': False,
                    'error': f"Quantity {quantity} below minimum {min_qty}",
                    'min': min_qty,
                    'max': max_qty,
                    'adjusted_quantity': min_qty
                }

            if quantity > max_qty:
                return {
                    'valid': False,
                    'error': f"Quantity {quantity} above maximum {max_qty}",
                    'min': min_qty,
                    'max': max_qty,
                    'adjusted_quantity': max_qty
                }

            return {
                'valid': True,
                'service': service_info,
                'min': min_qty,
                'max': max_qty
            }

        except Exception as e:
            logger.error(
                f"Failed to validate service params",
                error=str(e),
                service_id=service_id
            )
            return {
                'valid': False,
                'error': str(e)
            }

    async def check_service_availability(self, service_id: int) -> bool:
        """Check if service is available and active"""
        try:
            services = await self.get_services()

            for service in services:
                if service.get('service') == service_id:
                    # Check status if available
                    status = service.get('status', 'Active')
                    is_active = status.lower() in ['active', 'enabled', '1', 'true']

                    if not is_active:
                        logger.warning(
                            f"Service {service_id} is not active",
                            status=status,
                            name=service.get('name', 'Unknown')
                        )

                    return is_active

            logger.warning(f"Service {service_id} not found in available services")
            return False

        except Exception as e:
            logger.error(
                f"Failed to check service availability",
                error=str(e),
                service_id=service_id
            )
            return False

    async def estimate_order_cost(
        self,
        service_id: int,
        quantity: int,
        runs: Optional[int] = None
    ) -> Dict[str, Any]:
        """Estimate order cost before creating"""
        try:
            service_info = await self._get_service_info(service_id)

            rate = float(service_info.get('rate', 0))
            total_quantity = quantity * (runs or 1)

            # Calculate cost
            cost = (total_quantity / 1000) * rate

            # Get current balance
            balance_info = await self.get_balance()
            balance = float(balance_info.get('balance', 0))
            currency = balance_info.get('currency', 'USD')

            # Check if affordable
            can_afford = balance >= cost

            return {
                'service_id': service_id,
                'service_name': service_info.get('name', ''),
                'quantity_per_run': quantity,
                'runs': runs or 1,
                'total_quantity': total_quantity,
                'rate_per_1000': rate,
                'estimated_cost': round(cost, 4),
                'currency': currency,
                'current_balance': balance,
                'can_afford': can_afford,
                'balance_after': round(balance - cost, 4) if can_afford else None
            }

        except Exception as e:
            logger.error(
                f"Failed to estimate cost",
                error=str(e),
                service_id=service_id,
                quantity=quantity
            )
            raise NakrutkaError(f"Cost estimation failed: {str(e)}")

    def clear_cache(self):
        """Clear all caches"""
        self._cache.clear()
        self._service_limits_cache.clear()
        logger.debug("All caches cleared")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        now = datetime.utcnow()

        cache_ages = {}
        for key, (cached_time, _) in self._cache.items():
            age = (now - cached_time).total_seconds()
            cache_ages[key] = age

        return {
            'cache_size': len(self._cache),
            'service_limits_cache_size': len(self._service_limits_cache),
            'cache_keys': list(self._cache.keys()),
            'cache_ages': cache_ages,
            'oldest_cache_age': max(cache_ages.values()) if cache_ages else 0
        }

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on Nakrutka API"""
        health = {
            'status': 'unknown',
            'api_reachable': False,
            'auth_valid': False,
            'balance_check': False,
            'services_available': False,
            'errors': []
        }

        try:
            # Check balance (also validates auth)
            balance_info = await self.get_balance()
            health['api_reachable'] = True
            health['auth_valid'] = True
            health['balance_check'] = True
            health['balance'] = balance_info.get('balance', 0)
            health['currency'] = balance_info.get('currency', 'USD')

            # Check services
            services = await self.get_services()
            health['services_available'] = len(services) > 0
            health['service_count'] = len(services)

            health['status'] = 'healthy'

        except NakrutkaAuthError as e:
            health['errors'].append(f"Auth error: {str(e)}")
            health['status'] = 'auth_error'
        except NakrutkaConnectionError as e:
            health['errors'].append(f"Connection error: {str(e)}")
            health['status'] = 'connection_error'
        except Exception as e:
            health['errors'].append(f"Unknown error: {str(e)}")
            health['status'] = 'error'

        return health