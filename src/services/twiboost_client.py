"""
Twiboost API client for SMM services
"""
import aiohttp
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
import time
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import settings
from src.utils.logger import get_logger, metrics, LoggerMixin

logger = get_logger(__name__)


class TwiboostAPIError(Exception):
    """Twiboost API error"""
    pass


class TwiboostClient(LoggerMixin):
    """Client for Twiboost API"""

    def __init__(self):
        self.api_url = settings.twiboost_api_url
        self.api_key = settings.twiboost_api_key
        self.session: Optional[aiohttp.ClientSession] = None
        self._services_cache = {}
        self._cache_timestamp = 0
        self.cache_ttl = 3600  # 1 hour cache

    async def __aenter__(self):
        """Async context manager entry"""
        await self.init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    async def init(self):
        """Initialize HTTP session"""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)
            self.log_info("Twiboost client initialized")

    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
            self.session = None
            self.log_info("Twiboost client closed")

    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make API request with retry logic"""
        if not self.session:
            await self.init()

        params["key"] = self.api_key
        start_time = time.time()

        try:
            self.log_debug("API request", params=params)

            async with self.session.get(self.api_url, params=params) as response:
                duration = time.time() - start_time

                if response.status != 200:
                    error_text = await response.text()
                    metrics.log_api_call("twiboost", params.get("action"), duration, False)
                    raise TwiboostAPIError(f"API error {response.status}: {error_text}")

                result = await response.json()
                metrics.log_api_call("twiboost", params.get("action"), duration, True)

                # Check for API errors in response
                if isinstance(result, dict) and "error" in result:
                    raise TwiboostAPIError(f"API error: {result['error']}")

                self.log_debug("API response", action=params.get("action"), duration=duration)
                return result

        except aiohttp.ClientError as e:
            duration = time.time() - start_time
            metrics.log_api_call("twiboost", params.get("action"), duration, False)
            self.log_error("HTTP request failed", error=e)
            raise TwiboostAPIError(f"HTTP error: {str(e)}")

    async def get_services(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """
        Get list of available services
        Returns list of services with their IDs, names, types, prices
        """
        # Check cache
        if not force_refresh and self._services_cache:
            if time.time() - self._cache_timestamp < self.cache_ttl:
                self.log_debug("Using cached services")
                return self._services_cache

        try:
            result = await self._make_request({"action": "services"})

            if not isinstance(result, list):
                raise TwiboostAPIError(f"Invalid services response: {result}")

            # Cache the results
            self._services_cache = result
            self._cache_timestamp = time.time()

            self.log_info(f"Fetched {len(result)} services from API")

            # Log service IDs for important types
            for service in result:
                if service.get("type") in ["view", "reaction", "repost"]:
                    self.log_info(
                        "Service found",
                        service_id=service.get("service"),
                        name=service.get("name"),
                        type=service.get("type"),
                        rate=service.get("rate"),
                        min=service.get("min"),
                        max=service.get("max")
                    )

            return result

        except Exception as e:
            self.log_error("Failed to get services", error=e)
            raise

    async def get_service_by_type(self, service_type: str, name_filter: str = None) -> Optional[Dict[str, Any]]:
        """Get specific service by type and optional name filter"""
        services = await self.get_services()

        for service in services:
            if service.get("type") == service_type:
                if name_filter and name_filter.lower() not in service.get("name", "").lower():
                    continue
                return service

        return None

    async def create_order(
            self,
            service_id: int,
            link: str,
            quantity: int,
            runs: Optional[int] = None,
            interval: Optional[int] = None
    ) -> int:
        """
        Create new order
        Returns order ID
        """
        params = {
            "action": "add",
            "service": service_id,
            "link": link,
            "quantity": quantity
        }

        # Add runs and interval for drip-feed orders
        if runs:
            params["runs"] = runs
        if interval:
            params["interval"] = interval

        try:
            result = await self._make_request(params)

            if not isinstance(result, dict) or "order" not in result:
                raise TwiboostAPIError(f"Invalid order response: {result}")

            order_id = result["order"]

            self.log_info(
                "Order created",
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
                "runs": runs
            })

            return order_id

        except Exception as e:
            self.log_error(
                "Failed to create order",
                error=e,
                service_id=service_id,
                quantity=quantity
            )
            raise

    async def get_order_status(self, order_id: int) -> Dict[str, Any]:
        """Get order status"""
        params = {
            "action": "status",
            "order": order_id
        }

        try:
            result = await self._make_request(params)

            if not isinstance(result, dict):
                raise TwiboostAPIError(f"Invalid status response: {result}")

            self.log_debug("Order status fetched", order_id=order_id, status=result.get("status"))
            return result

        except Exception as e:
            self.log_error("Failed to get order status", error=e, order_id=order_id)
            raise

    async def get_multiple_orders_status(self, order_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """Get status for multiple orders"""
        if not order_ids:
            return {}

        params = {
            "action": "status",
            "orders": ",".join(map(str, order_ids))
        }

        try:
            result = await self._make_request(params)

            if not isinstance(result, dict):
                raise TwiboostAPIError(f"Invalid status response: {result}")

            # Convert keys to integers
            return {int(k): v for k, v in result.items() if k.isdigit()}

        except Exception as e:
            self.log_error("Failed to get orders status", error=e, order_ids=order_ids)
            raise

    async def cancel_order(self, order_id: int) -> bool:
        """Cancel order"""
        params = {
            "action": "cancel",
            "order": order_id
        }

        try:
            result = await self._make_request(params)

            success = result.get("ok") == "true"
            self.log_info("Order cancelled", order_id=order_id, success=success)
            return success

        except Exception as e:
            self.log_error("Failed to cancel order", error=e, order_id=order_id)
            raise

    async def get_balance(self) -> Dict[str, Any]:
        """Get account balance"""
        params = {"action": "balance"}

        try:
            result = await self._make_request(params)

            self.log_info(
                "Balance fetched",
                balance=result.get("balance"),
                currency=result.get("currency")
            )
            return result

        except Exception as e:
            self.log_error("Failed to get balance", error=e)
            raise

    async def sync_services_to_db(self, db_connection):
        """Sync available services to database"""
        services = await self.get_services()

        # Filter relevant services
        relevant_types = ["view", "reaction", "repost"]
        filtered_services = [s for s in services if s.get("type") in relevant_types]

        self.log_info(f"Syncing {len(filtered_services)} services to database")

        # Store in database (implement based on your DB schema)
        # This is a placeholder for DB sync logic
        return filtered_services


# Global client instance
twiboost_client = TwiboostClient()