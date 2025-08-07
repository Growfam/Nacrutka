"""
Nakrutochka API client for SMM services
"""
import aiohttp
import asyncio
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import time
from urllib.parse import urlencode
import json

from src.config import settings
from src.utils.logger import get_logger, metrics, LoggerMixin

logger = get_logger(__name__)


class NakrutochkaAPIError(Exception):
    """Nakrutochka API error"""
    pass


class NakrutochkaClient(LoggerMixin):
    """Client for Nakrutochka API v2"""

    def __init__(self):
        self.api_url = settings.nakrutochka_api_url
        self.api_key = settings.nakrutochka_api_key
        self.session: Optional[aiohttp.ClientSession] = None
        self._services_cache = {}
        self._cache_timestamp = 0
        self.cache_ttl = 3600  # 1 hour cache

    async def init(self):
        """Initialize HTTP session"""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(limit=100, verify_ssl=False)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector
            )
            self.log_info("Nakrutochka client initialized")

    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
            self.session = None
            self.log_info("Nakrutochka client closed")

    async def _make_request(self, data: Dict[str, Any]) -> Any:
        """Make API request using POST with form data"""
        if not self.session:
            await self.init()

        # Add API key
        data["key"] = self.api_key

        # Convert to URL encoded format for POST body
        post_data = urlencode(data)

        start_time = time.time()

        try:
            self.log_debug("Nakrutochka API request", action=data.get("action"))

            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'Mozilla/4.0 (compatible; MSIE 5.01; Windows NT 5.0)'
            }

            async with self.session.post(
                    self.api_url,
                    data=post_data,
                    headers=headers,
                    ssl=False
            ) as response:
                duration = time.time() - start_time

                text = await response.text()

                # Try to parse JSON
                try:
                    result = json.loads(text)
                except json.JSONDecodeError:
                    # Some endpoints return plain text (like balance)
                    result = text

                # Check for errors
                if isinstance(result, dict) and "error" in result:
                    raise NakrutochkaAPIError(f"API error: {result['error']}")

                metrics.log_api_call("nakrutochka", data.get("action"), duration, True)

                self.log_debug(
                    "Nakrutochka API response",
                    action=data.get("action"),
                    duration=duration
                )

                return result

        except aiohttp.ClientError as e:
            duration = time.time() - start_time
            metrics.log_api_call("nakrutochka", data.get("action"), duration, False)
            self.log_error("HTTP request failed", error=e)
            raise NakrutochkaAPIError(f"HTTP error: {str(e)}")

    async def get_services(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Get list of available services"""
        # Check cache
        if not force_refresh and self._services_cache:
            if time.time() - self._cache_timestamp < self.cache_ttl:
                self.log_debug("Using cached Nakrutochka services")
                return self._services_cache

        try:
            result = await self._make_request({"action": "services"})

            if not isinstance(result, list):
                # Try to parse if it's a string
                if isinstance(result, str):
                    try:
                        result = json.loads(result)
                    except:
                        raise NakrutochkaAPIError(f"Invalid services response")

            # Filter and categorize Telegram services
            telegram_services = []
            for service in result:
                name_lower = (service.get("name") or "").lower()
                category_lower = (service.get("category") or "").lower()

                # Check if it's Telegram service
                is_telegram = any(keyword in name_lower or keyword in category_lower
                                  for keyword in ['telegram', 'телеграм', 'тг'])

                if is_telegram:
                    # Determine type based on name
                    if any(keyword in name_lower for keyword in
                           ['реакц', 'reaction', 'эмодз', 'emoji', '👍', '❤️', '🔥', '😊', '😢', '😮', '😡']):
                        service['parsed_type'] = 'reactions'

                        # Extract emoji from name
                        emoji_map = {
                            '👍': ['👍', 'лайк', 'like', 'thumbs'],
                            '❤️': ['❤', '❤️', 'серд', 'heart', 'love'],
                            '🔥': ['🔥', 'огонь', 'fire', 'пламя'],
                            '😊': ['😊', 'смайл', 'smile'],
                            '😢': ['😢', 'слез', 'cry', 'tear'],
                            '😮': ['😮', 'удивл', 'wow', 'surprise'],
                            '😡': ['😡', 'злост', 'angry', 'rage'],
                            '👎': ['👎', 'дизлайк', 'dislike', 'thumbs down'],
                            '💯': ['💯', '100', 'сто'],
                            '🎉': ['🎉', 'праздн', 'party', 'celebration']
                        }

                        for emoji, keywords in emoji_map.items():
                            if any(kw in name_lower for kw in keywords):
                                service['emoji'] = emoji
                                break

                        telegram_services.append(service)

                    elif any(keyword in name_lower for keyword in
                             ['репост', 'repost', 'share', 'пересыл', 'перепост']):
                        service['parsed_type'] = 'reposts'
                        telegram_services.append(service)

                    elif any(keyword in name_lower for keyword in
                             ['просмотр', 'view', 'перегляд']):
                        # На випадок якщо є перегляди в Nakrutochka
                        service['parsed_type'] = 'views'
                        telegram_services.append(service)

            # Cache the results
            self._services_cache = telegram_services
            self._cache_timestamp = time.time()

            self.log_info(f"Fetched {len(telegram_services)} Telegram services from Nakrutochka")

            # Log service details for debugging
            for srv in telegram_services[:5]:  # Log first 5
                self.log_debug(
                    "Nakrutochka service",
                    id=srv.get("service"),
                    name=srv.get("name")[:50],
                    type=srv.get("parsed_type"),
                    emoji=srv.get("emoji", "N/A"),
                    rate=srv.get("rate")
                )

            return telegram_services

        except Exception as e:
            self.log_error("Failed to get services", error=e)
            raise

    async def create_order(
            self,
            service_id: int,
            link: str,
            quantity: int,
            runs: Optional[int] = None,
            interval: Optional[int] = None,
            comments: Optional[str] = None,
            answer_number: Optional[str] = None,
            username: Optional[str] = None,
            posts: Optional[int] = None,
            old_posts: Optional[int] = None,
            min_quantity: Optional[int] = None,
            max_quantity: Optional[int] = None,
            delay: Optional[int] = None,
            expiry: Optional[str] = None
    ) -> Union[int, str]:
        """
        Create new order with all possible parameters

        Parameters match Nakrutochka API documentation
        """
        data = {
            "action": "add",
            "service": service_id,
            "link": link,
            "quantity": quantity
        }

        # Add optional parameters for different order types
        if runs:  # Drip-feed
            data["runs"] = runs
        if interval:
            data["interval"] = interval
        if comments:  # Custom comments
            data["comments"] = comments
        if answer_number:  # Poll
            data["answer_number"] = answer_number
        if username:  # Subscriptions
            data["username"] = username
        if posts is not None:  # Old posts only
            data["posts"] = posts
        if old_posts is not None:  # Mix of new and old
            data["old_posts"] = old_posts
        if min_quantity:
            data["min"] = min_quantity
        if max_quantity:
            data["max"] = max_quantity
        if delay:
            data["delay"] = delay
        if expiry:
            data["expiry"] = expiry

        try:
            result = await self._make_request(data)

            # Parse response
            if isinstance(result, dict):
                order_id = result.get("order")
                if not order_id:
                    raise NakrutochkaAPIError(f"No order ID in response: {result}")
            else:
                # Sometimes returns just the order ID
                order_id = str(result).strip()

            self.log_info(
                "Nakrutochka order created",
                order_id=order_id,
                service_id=service_id,
                quantity=quantity,
                runs=runs,
                interval=interval
            )

            metrics.log_order_created({
                "id": order_id,
                "service_type": service_id,
                "quantity": quantity,
                "api": "nakrutochka"
            })

            return order_id

        except Exception as e:
            self.log_error(
                "Failed to create Nakrutochka order",
                error=e,
                service_id=service_id,
                quantity=quantity
            )
            raise

    async def get_order_status(self, order_id: Union[int, str]) -> Dict[str, Any]:
        """Get single order status"""
        data = {
            "action": "status",
            "order": order_id
        }

        try:
            result = await self._make_request(data)

            if not isinstance(result, dict):
                raise NakrutochkaAPIError(f"Invalid status response: {result}")

            # Normalize status format
            normalized = {
                "charge": result.get("charge"),
                "start_count": result.get("start_count"),
                "status": result.get("status"),
                "remains": result.get("remains"),
                "currency": result.get("currency", "RUB")
            }

            self.log_debug(
                "Nakrutochka order status",
                order_id=order_id,
                status=normalized.get("status")
            )

            return normalized

        except Exception as e:
            self.log_error("Failed to get order status", error=e, order_id=order_id)
            raise

    async def get_multiple_orders_status(
            self,
            order_ids: List[Union[int, str]]
    ) -> Dict[Union[int, str], Dict[str, Any]]:
        """Get status for multiple orders"""
        if not order_ids:
            return {}

        data = {
            "action": "status",
            "orders": ",".join(map(str, order_ids))
        }

        try:
            result = await self._make_request(data)

            if not isinstance(result, dict):
                raise NakrutochkaAPIError(f"Invalid multi-status response")

            # Normalize each order status
            normalized = {}
            for order_id, status in result.items():
                if isinstance(status, dict):
                    normalized[order_id] = {
                        "charge": status.get("charge"),
                        "start_count": status.get("start_count"),
                        "status": status.get("status"),
                        "remains": status.get("remains"),
                        "currency": status.get("currency", "RUB")
                    }

            return normalized

        except Exception as e:
            self.log_error("Failed to get orders status", error=e)
            raise

    async def refill_order(self, order_id: Union[int, str]) -> Dict[str, Any]:
        """Refill single order"""
        data = {
            "action": "refill",
            "order": order_id
        }

        try:
            result = await self._make_request(data)

            if isinstance(result, dict):
                refill_id = result.get("refill")
            else:
                refill_id = str(result)

            self.log_info(
                "Nakrutochka order refill requested",
                order_id=order_id,
                refill_id=refill_id
            )

            return {"refill": refill_id}

        except Exception as e:
            self.log_error("Failed to refill order", error=e, order_id=order_id)
            raise

    async def refill_multiple_orders(
            self,
            order_ids: List[Union[int, str]]
    ) -> Dict[str, Any]:
        """Refill multiple orders"""
        data = {
            "action": "refill",
            "orders": ",".join(map(str, order_ids))
        }

        try:
            result = await self._make_request(data)

            self.log_info(
                "Nakrutochka orders refill requested",
                order_ids=order_ids,
                result=result
            )

            return result

        except Exception as e:
            self.log_error("Failed to refill orders", error=e)
            raise

    async def get_refill_status(self, refill_id: Union[int, str]) -> Dict[str, Any]:
        """Get refill status"""
        data = {
            "action": "refill_status",
            "refill": refill_id
        }

        try:
            result = await self._make_request(data)

            self.log_debug(
                "Nakrutochka refill status",
                refill_id=refill_id,
                status=result.get("status")
            )

            return result

        except Exception as e:
            self.log_error("Failed to get refill status", error=e)
            raise

    async def cancel_orders(self, order_ids: List[Union[int, str]]) -> Dict[str, Any]:
        """Cancel multiple orders"""
        data = {
            "action": "cancel",
            "orders": ",".join(map(str, order_ids))
        }

        try:
            result = await self._make_request(data)

            self.log_info(
                "Nakrutochka orders cancelled",
                order_ids=order_ids,
                result=result
            )

            return result

        except Exception as e:
            self.log_error("Failed to cancel orders", error=e)
            raise

    async def get_balance(self) -> Dict[str, Any]:
        """Get account balance"""
        data = {"action": "balance"}

        try:
            result = await self._make_request(data)

            # Parse balance based on response format
            if isinstance(result, dict):
                balance = result.get("balance", result.get("Balance"))
                currency = result.get("currency", "RUB")
            else:
                # Plain text response (just number)
                try:
                    balance = float(str(result).strip())
                    currency = "RUB"
                except:
                    balance = 0
                    currency = "RUB"

            response = {
                "balance": balance,
                "currency": currency
            }

            self.log_info(
                "Nakrutochka balance fetched",
                balance=balance,
                currency=currency
            )

            return response

        except Exception as e:
            self.log_error("Failed to get balance", error=e)
            raise

    async def get_service_for_reaction(self, emoji: str) -> Optional[Dict[str, Any]]:
        """Find best service for specific reaction emoji"""
        services = await self.get_services()

        # Filter reaction services
        reaction_services = [s for s in services if s.get('parsed_type') == 'reactions']

        # Try to find exact emoji match
        for service in reaction_services:
            if service.get('emoji') == emoji:
                return service

        # Fallback: search by keywords in name
        emoji_keywords = {
            '👍': ['лайк', 'like', 'thumbs up', '👍', 'палец вверх'],
            '❤️': ['сердц', 'heart', 'love', '❤', 'любовь'],
            '🔥': ['огонь', 'fire', '🔥', 'пламя', 'жарко'],
            '😊': ['смайл', 'smile', '😊', 'улыбка'],
            '😢': ['слез', 'cry', '😢', 'плач', 'грусть'],
            '😮': ['удивл', 'wow', '😮', 'surprise', 'шок'],
            '😡': ['злост', 'angry', '😡', 'rage', 'гнев'],
            '👎': ['дизлайк', 'dislike', '👎', 'палец вниз'],
            '💯': ['100', '💯', 'сто', 'процент'],
            '🎉': ['праздн', 'party', '🎉', 'celebration', 'салют']
        }

        keywords = emoji_keywords.get(emoji, [])

        for service in reaction_services:
            name_lower = (service.get('name') or '').lower()
            if any(keyword in name_lower for keyword in keywords):
                service['emoji'] = emoji  # Tag it
                return service

        # If no specific match, return first reaction service
        if reaction_services:
            self.log_warning(
                f"No specific service for emoji {emoji}, using generic",
                emoji=emoji
            )
            return reaction_services[0]

        return None

    async def get_service_for_reposts(self) -> Optional[Dict[str, Any]]:
        """Find best service for reposts"""
        services = await self.get_services()

        # Filter repost services
        repost_services = [s for s in services if s.get('parsed_type') == 'reposts']

        if not repost_services:
            self.log_warning("No repost services found in Nakrutochka")
            return None

        # Sort by rate (cheapest first)
        repost_services.sort(key=lambda x: float(x.get('rate', 999999)))

        return repost_services[0]

    def map_status_to_order_status(self, nakrutochka_status: str) -> str:
        """Map Nakrutochka status to our OrderStatus enum"""
        status_map = {
            'Pending': 'pending',
            'In progress': 'in_progress',
            'Completed': 'completed',
            'Canceled': 'canceled',
            'Fail': 'failed',
            'Partial': 'partial',
            'Processing': 'in_progress',
            'Awaiting': 'awaiting'
        }

        return status_map.get(nakrutochka_status, 'failed')


# Global client instance
nakrutochka_client = NakrutochkaClient()