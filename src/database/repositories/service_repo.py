"""
Service repository for Twiboost and Nakrutochka services
"""
from typing import List, Optional, Dict, Any
import json

from src.database.connection import db
from src.database.models import TwiboostService, NakrutochkaService, APIProvider
from src.utils.logger import get_logger, LoggerMixin

logger = get_logger(__name__)


class ServiceRepository(LoggerMixin):
    """Repository for dual API service operations"""

    # ========== TWIBOOST SERVICES ==========

    async def sync_twiboost_services(self, services: List[Dict[str, Any]]) -> int:
        """Sync services from Twiboost API to database"""
        count = 0

        for service in services:
            try:
                # Визначаємо чи це Telegram сервіс
                name_lower = service.get("name", "").lower()
                is_telegram = 'telegram' in name_lower or 'телеграм' in name_lower

                if not is_telegram:
                    continue

                # Визначаємо тип сервісу по назві
                service_type = "other"
                if 'просмотр' in name_lower or 'view' in name_lower:
                    service_type = "views"
                elif 'реакц' in name_lower or 'reaction' in name_lower or 'эмодз' in name_lower:
                    service_type = "reactions"
                elif 'репост' in name_lower or 'repost' in name_lower or 'share' in name_lower:
                    service_type = "reposts"

                # Зберігаємо в БД
                query = """
                    INSERT INTO twiboost_services (
                        service_id, name, type, category, rate,
                        min_quantity, max_quantity, refill, cancel, is_active, metadata
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (service_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        type = EXCLUDED.type,
                        category = EXCLUDED.category,
                        rate = EXCLUDED.rate,
                        min_quantity = EXCLUDED.min_quantity,
                        max_quantity = EXCLUDED.max_quantity,
                        refill = EXCLUDED.refill,
                        cancel = EXCLUDED.cancel,
                        is_active = EXCLUDED.is_active,
                        metadata = EXCLUDED.metadata,
                        synced_at = NOW()
                """

                metadata = {
                    "original_type": service.get("type"),
                    "service_category": service_type,
                    "is_telegram": is_telegram
                }

                await db.execute(
                    query,
                    service["service"],
                    service["name"],
                    service_type,
                    service.get("category", ""),
                    float(service["rate"]),
                    service["min"],
                    service["max"],
                    service.get("refill", False),
                    service.get("cancel", False),
                    True,
                    json.dumps(metadata)
                )

                count += 1

            except Exception as e:
                self.log_error(
                    "Failed to sync Twiboost service",
                    error=e,
                    service_id=service.get("service"),
                    service_name=service.get("name")
                )

        self.log_info(f"Synced {count} Twiboost services to database")
        return count

    async def get_twiboost_service(self, service_id: int) -> Optional[TwiboostService]:
        """Get Twiboost service by ID"""
        query = "SELECT * FROM twiboost_services WHERE service_id = $1"
        row = await db.fetchrow(query, service_id)

        if row:
            return self._row_to_twiboost_service(row)
        return None

    async def get_services_by_type(self, service_type: str) -> List[TwiboostService]:
        """Get all Twiboost services of specific type (mainly for views)"""
        query = """
            SELECT *
            FROM twiboost_services
            WHERE type = $1
              AND is_active = true
            ORDER BY rate ASC
        """
        rows = await db.fetch(query, service_type)

        return [self._row_to_twiboost_service(row) for row in rows]

    async def find_best_twiboost_service(
        self,
        service_type: str,
        quantity: int,
        name_filter: Optional[str] = None
    ) -> Optional[TwiboostService]:
        """Find best Twiboost service for given parameters"""
        query = """
            SELECT *
            FROM twiboost_services
            WHERE type = $1
              AND is_active = true
              AND min_quantity <= $2
              AND max_quantity >= $2
              AND ($3::text IS NULL OR name ILIKE '%' || $3 || '%')
            ORDER BY rate ASC
            LIMIT 1
        """

        row = await db.fetchrow(query, service_type, quantity, name_filter)

        if row:
            return self._row_to_twiboost_service(row)
        return None

    # ========== NAKRUTOCHKA SERVICES ==========

    async def sync_nakrutochka_services(self, services: List[Dict[str, Any]]) -> int:
        """Sync services from Nakrutochka API to database"""
        count = 0

        for service in services:
            try:
                # Service should already be filtered and have parsed_type
                service_type = service.get("parsed_type", "other")

                if service_type not in ["reactions", "reposts"]:
                    continue

                # Save to database
                query = """
                    INSERT INTO nakrutochka_services (
                        service_id, name, type, category, rate,
                        min_quantity, max_quantity, refill, cancel, is_active, metadata
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (service_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        type = EXCLUDED.type,
                        category = EXCLUDED.category,
                        rate = EXCLUDED.rate,
                        min_quantity = EXCLUDED.min_quantity,
                        max_quantity = EXCLUDED.max_quantity,
                        refill = EXCLUDED.refill,
                        cancel = EXCLUDED.cancel,
                        is_active = EXCLUDED.is_active,
                        metadata = EXCLUDED.metadata,
                        synced_at = NOW()
                """

                metadata = {
                    "original_type": service.get("type"),
                    "parsed_type": service_type,
                    "emoji": service.get("emoji"),
                    "is_telegram": True
                }

                await db.execute(
                    query,
                    service["service"],
                    service["name"],
                    service_type,
                    service.get("category", ""),
                    float(service.get("rate", 0)),
                    service.get("min", 1),
                    service.get("max", 10000),
                    service.get("refill", False),
                    service.get("cancel", False),
                    True,
                    json.dumps(metadata)
                )

                count += 1

            except Exception as e:
                self.log_error(
                    "Failed to sync Nakrutochka service",
                    error=e,
                    service_id=service.get("service"),
                    service_name=service.get("name")
                )

        self.log_info(f"Synced {count} Nakrutochka services to database")
        return count

    async def get_nakrutochka_service(self, service_id: int) -> Optional[NakrutochkaService]:
        """Get Nakrutochka service by ID"""
        query = "SELECT * FROM nakrutochka_services WHERE service_id = $1"
        row = await db.fetchrow(query, service_id)

        if row:
            return self._row_to_nakrutochka_service(row)
        return None

    async def get_nakrutochka_services_by_type(self, service_type: str) -> List[NakrutochkaService]:
        """Get all Nakrutochka services of specific type"""
        query = """
            SELECT *
            FROM nakrutochka_services
            WHERE type = $1
              AND is_active = true
            ORDER BY rate ASC
        """
        rows = await db.fetch(query, service_type)

        return [self._row_to_nakrutochka_service(row) for row in rows]

    async def find_best_nakrutochka_service(
        self,
        service_type: str,
        quantity: int,
        emoji: Optional[str] = None
    ) -> Optional[NakrutochkaService]:
        """Find best Nakrutochka service for given parameters"""

        if emoji and service_type == "reactions":
            # First try to find service with specific emoji
            query = """
                SELECT *
                FROM nakrutochka_services
                WHERE type = $1
                  AND is_active = true
                  AND min_quantity <= $2
                  AND max_quantity >= $2
                  AND metadata->>'emoji' = $3
                ORDER BY rate ASC
                LIMIT 1
            """
            row = await db.fetchrow(query, service_type, quantity, emoji)

            if row:
                return self._row_to_nakrutochka_service(row)

            # Fallback: search by emoji in name
            emoji_keywords = {
                '👍': ['лайк', 'like', 'thumbs', '👍'],
                '❤️': ['сердц', 'heart', 'love', '❤'],
                '🔥': ['огонь', 'fire', '🔥'],
                '😊': ['смайл', 'smile', '😊'],
                '😢': ['слез', 'cry', '😢'],
                '😮': ['удивл', 'wow', '😮'],
                '😡': ['злост', 'angry', '😡'],
                '👎': ['дизлайк', 'dislike', '👎'],
                '💯': ['100', '💯'],
                '🎉': ['праздн', 'party', '🎉']
            }

            keywords = emoji_keywords.get(emoji, [emoji])

            for keyword in keywords:
                query = """
                    SELECT *
                    FROM nakrutochka_services
                    WHERE type = $1
                      AND is_active = true
                      AND min_quantity <= $2
                      AND max_quantity >= $2
                      AND name ILIKE '%' || $3 || '%'
                    ORDER BY rate ASC
                    LIMIT 1
                """
                row = await db.fetchrow(query, service_type, quantity, keyword)

                if row:
                    return self._row_to_nakrutochka_service(row)

        # Generic search
        query = """
            SELECT *
            FROM nakrutochka_services
            WHERE type = $1
              AND is_active = true
              AND min_quantity <= $2
              AND max_quantity >= $2
            ORDER BY rate ASC
            LIMIT 1
        """

        row = await db.fetchrow(query, service_type, quantity)

        if row:
            return self._row_to_nakrutochka_service(row)
        return None

    async def get_nakrutochka_reaction_services(self) -> Dict[str, int]:
        """Get mapping of reaction emojis to Nakrutochka service IDs"""
        query = """
            SELECT service_id, name, metadata
            FROM nakrutochka_services
            WHERE type = 'reactions'
              AND is_active = true
        """
        rows = await db.fetch(query)

        mapping = {}
        for row in rows:
            metadata = json.loads(row["metadata"]) if row["metadata"] else {}
            emoji = metadata.get("emoji")

            if emoji:
                mapping[f"reaction_{emoji}"] = row["service_id"]
                continue

            # Try to extract emoji from name
            name = row["name"]
            emojis = ["👍", "❤️", "🔥", "😊", "😢", "😮", "😡", "👎", "💯", "🎉"]
            for e in emojis:
                if e in name:
                    mapping[f"reaction_{e}"] = row["service_id"]
                    break

        return mapping

    # ========== UNIFIED METHODS ==========

    async def get_service_for_order(
        self,
        service_type: str,
        quantity: int,
        api_provider: APIProvider,
        emoji: Optional[str] = None,
        name_filter: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get best service for order from specified API"""

        if api_provider == APIProvider.TWIBOOST:
            service = await self.find_best_twiboost_service(
                service_type,
                quantity,
                name_filter
            )
            if service:
                return {
                    "service_id": service.service_id,
                    "api_provider": APIProvider.TWIBOOST,
                    "service": service
                }

        elif api_provider == APIProvider.NAKRUTOCHKA:
            service = await self.find_best_nakrutochka_service(
                service_type,
                quantity,
                emoji
            )
            if service:
                return {
                    "service_id": service.service_id,
                    "api_provider": APIProvider.NAKRUTOCHKA,
                    "service": service
                }

        return None

    async def get_service_with_fallback(
        self,
        service_type: str,
        quantity: int,
        preferred_api: APIProvider,
        emoji: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get service with fallback to other API if preferred fails"""

        # Try preferred API first
        result = await self.get_service_for_order(
            service_type,
            quantity,
            preferred_api,
            emoji
        )

        if result:
            return result

        # Fallback to other API
        fallback_api = (
            APIProvider.NAKRUTOCHKA
            if preferred_api == APIProvider.TWIBOOST
            else APIProvider.TWIBOOST
        )

        self.log_warning(
            f"No service found in {preferred_api}, trying {fallback_api}",
            service_type=service_type,
            quantity=quantity
        )

        return await self.get_service_for_order(
            service_type,
            quantity,
            fallback_api,
            emoji
        )

    async def update_service_status(
        self,
        service_id: int,
        is_active: bool,
        api_provider: APIProvider
    ):
        """Update service active status"""
        if api_provider == APIProvider.TWIBOOST:
            table = "twiboost_services"
        else:
            table = "nakrutochka_services"

        query = f"""
            UPDATE {table}
            SET is_active = $2,
                synced_at = NOW()
            WHERE service_id = $1
        """
        await db.execute(query, service_id, is_active)

    async def get_all_active_services_count(self) -> Dict[str, int]:
        """Get count of all active services by type and API"""

        # Twiboost counts
        twiboost_query = """
            SELECT type, COUNT(*) as count
            FROM twiboost_services
            WHERE is_active = true
            GROUP BY type
        """
        twiboost_rows = await db.fetch(twiboost_query)

        # Nakrutochka counts
        nakrutochka_query = """
            SELECT type, COUNT(*) as count
            FROM nakrutochka_services
            WHERE is_active = true
            GROUP BY type
        """
        nakrutochka_rows = await db.fetch(nakrutochka_query)

        result = {
            "twiboost": {row["type"]: row["count"] for row in twiboost_rows},
            "nakrutochka": {row["type"]: row["count"] for row in nakrutochka_rows}
        }

        return result

    # ========== HELPER METHODS ==========

    def _row_to_twiboost_service(self, row) -> TwiboostService:
        """Convert database row to TwiboostService model"""
        return TwiboostService(
            service_id=row["service_id"],
            name=row["name"],
            type=row["type"],
            category=row["category"],
            rate=float(row["rate"]),
            min_quantity=row["min_quantity"],
            max_quantity=row["max_quantity"],
            refill=row["refill"],
            cancel=row["cancel"]
        )

    def _row_to_nakrutochka_service(self, row) -> NakrutochkaService:
        """Convert database row to NakrutochkaService model"""
        metadata = json.loads(row["metadata"]) if row["metadata"] else {}

        return NakrutochkaService(
            service_id=row["service_id"],
            name=row["name"],
            type=row["type"],
            category=row["category"],
            rate=float(row["rate"]),
            min_quantity=row["min_quantity"],
            max_quantity=row["max_quantity"],
            refill=row["refill"],
            cancel=row["cancel"],
            emoji=metadata.get("emoji")
        )


# Global repository instance
service_repo = ServiceRepository()