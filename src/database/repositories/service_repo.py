"""
Service repository for Twiboost services
"""
from typing import List, Optional, Dict, Any
import json

from src.database.connection import db
from src.database.models import TwiboostService
from src.utils.logger import get_logger, LoggerMixin

logger = get_logger(__name__)


class ServiceRepository(LoggerMixin):
    """Repository for Twiboost service operations"""

    async def sync_services(self, services: List[Dict[str, Any]]) -> int:
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

                # Зберігаємо в БД з тим самим ID що і в API!
                query = """
                        INSERT INTO twiboost_services (service_id, name, type, category, rate, \
                                                       min_quantity, max_quantity, refill, cancel, is_active, metadata) \
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (service_id) DO \
                        UPDATE SET
                            name = EXCLUDED.name, \
                            type = EXCLUDED.type, \
                            category = EXCLUDED.category, \
                            rate = EXCLUDED.rate, \
                            min_quantity = EXCLUDED.min_quantity, \
                            max_quantity = EXCLUDED.max_quantity, \
                            refill = EXCLUDED.refill, \
                            cancel = EXCLUDED.cancel, \
                            is_active = EXCLUDED.is_active, \
                            metadata = EXCLUDED.metadata, \
                            synced_at = NOW() \
                        """

                metadata = {
                    "original_type": service.get("type"),
                    "service_category": service_type,
                    "is_telegram": is_telegram
                }

                await db.execute(
                    query,
                    service["service"],  # ВАЖЛИВО! Використовуємо оригінальний ID з API
                    service["name"],
                    service_type,  # Наш визначений тип
                    service.get("category", ""),
                    float(service["rate"]),
                    service["min"],
                    service["max"],
                    service.get("refill", False),
                    service.get("cancel", False),
                    True,  # is_active
                    json.dumps(metadata)
                )

                count += 1

            except Exception as e:
                self.log_error(
                    "Failed to sync service",
                    error=e,
                    service_id=service.get("service"),
                    service_name=service.get("name")
                )

        self.log_info(f"Synced {count} Telegram services to database")
        return count

    async def get_service(self, service_id: int) -> Optional[TwiboostService]:
        """Get service by ID"""
        query = "SELECT * FROM twiboost_services WHERE service_id = $1"
        row = await db.fetchrow(query, service_id)

        if row:
            return self._row_to_service(row)
        return None

    async def get_services_by_type(self, service_type: str) -> List[TwiboostService]:
        """Get all services of specific type"""
        query = """
                SELECT * \
                FROM twiboost_services
                WHERE type = $1 \
                  AND is_active = true
                ORDER BY rate ASC \
                """
        rows = await db.fetch(query, service_type)

        return [self._row_to_service(row) for row in rows]

    async def find_best_service(
            self,
            service_type: str,
            quantity: int,
            name_filter: Optional[str] = None
    ) -> Optional[TwiboostService]:
        """Find best service for given parameters"""
        query = """
                SELECT * \
                FROM twiboost_services
                WHERE type = $1
                  AND is_active = true
                  AND min_quantity <= $2
                  AND max_quantity >= $2
                  AND ($3::text IS NULL OR name ILIKE '%' || $3 || '%')
                ORDER BY rate ASC LIMIT 1 \
                """

        row = await db.fetchrow(query, service_type, quantity, name_filter)

        if row:
            return self._row_to_service(row)
        return None

    async def get_reaction_services(self) -> Dict[str, int]:
        """Get mapping of reaction emojis to service IDs"""
        query = """
                SELECT service_id, name \
                FROM twiboost_services
                WHERE type = 'reactions' \
                  AND is_active = true \
                """
        rows = await db.fetch(query)

        mapping = {}
        for row in rows:
            name = row["name"]
            service_id = row["service_id"]

            # Визначаємо емодзі з назви
            emojis = ["👍", "❤️", "🔥", "😊", "😢", "😮", "😡", "👎", "💯", "🎉"]
            for emoji in emojis:
                if emoji in name:
                    mapping[f"reaction_{emoji}"] = service_id
                    break

        return mapping

    async def update_service_status(self, service_id: int, is_active: bool):
        """Update service active status"""
        query = """
                UPDATE twiboost_services
                SET is_active = $2, \
                    synced_at = NOW()
                WHERE service_id = $1 \
                """
        await db.execute(query, service_id, is_active)

    def _row_to_service(self, row) -> TwiboostService:
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


# Global repository instance
service_repo = ServiceRepository()