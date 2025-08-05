"""
Database initialization with default data
"""
from typing import Dict, List, Any, Optional
from datetime import datetime

from src.database.connection import DatabaseConnection
from src.database.queries import Queries
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseInitializer:
    """Initialize database with required data"""

    def __init__(self, db: DatabaseConnection):
        self.db = db
        self.queries = Queries(db)

    async def initialize_all(self):
        """Run all initialization steps"""
        logger.info("Starting database initialization...")

        # Check if already initialized
        if await self.is_initialized():
            logger.info("Database already initialized")
            return

        # Initialize in order
        await self.initialize_services()
        await self.initialize_api_keys()
        await self.initialize_default_channel()

        logger.info("Database initialization completed")

    async def is_initialized(self) -> bool:
        """Check if database is already initialized"""
        # Check if we have services
        service_count = await self.db.fetchval(
            "SELECT COUNT(*) FROM services WHERE is_active = true"
        )
        return service_count > 0

    async def initialize_services(self):
        """Initialize services from default list"""
        logger.info("Initializing services...")

        # Check if services already exist
        existing = await self.db.fetchval("SELECT COUNT(*) FROM services")
        if existing > 0:
            logger.info(f"Services already exist ({existing}), skipping")
            return

        # Get default services
        services = self.get_default_services()

        # Bulk insert
        query = """
                INSERT INTO services (nakrutka_id, service_name, service_type, \
                                      price_per_1000, min_quantity, max_quantity) \
                VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (nakrutka_id) DO NOTHING \
                """

        count = 0
        for service in services:
            try:
                await self.db.execute(
                    query,
                    service['nakrutka_id'],
                    service['service_name'],
                    service['service_type'],
                    service['price_per_1000'],
                    service['min_quantity'],
                    service['max_quantity']
                )
                count += 1
            except Exception as e:
                logger.error(f"Failed to insert service {service['nakrutka_id']}: {e}")

        logger.info(f"Initialized {count} services")

    async def initialize_api_keys(self):
        """Initialize API keys from environment"""
        logger.info("Checking API keys...")

        # Check Nakrutka key
        from src.config import settings

        if settings.nakrutka_api_key:
            # Check if already exists
            existing = await self.db.fetchval(
                "SELECT COUNT(*) FROM api_keys WHERE service_name = 'nakrutka'"
            )

            if existing == 0:
                await self.db.execute(
                    """
                    INSERT INTO api_keys (service_name, api_key, is_active)
                    VALUES ('nakrutka', $1, true)
                    """,
                    settings.nakrutka_api_key
                )
                logger.info("Initialized Nakrutka API key from environment")

    async def initialize_default_channel(self):
        """Initialize default channel if specified"""
        # This is optional - can be configured to add a default channel
        # For now, we'll skip this as channels should be added manually
        pass

    def get_default_services(self) -> List[Dict[str, Any]]:
        """Get default services list"""
        return [
            # Views services
            {
                'nakrutka_id': 3791,
                'service_name': 'Telegram - Просмотры | Моментальные | Без списаний',
                'service_type': 'views',
                'price_per_1000': 0.0225,
                'min_quantity': 1,
                'max_quantity': 100000
            },
            {
                'nakrutka_id': 4331,
                'service_name': 'Telegram - Просмотры на пост медленные | Без списания',
                'service_type': 'views',
                'price_per_1000': 0.0225,
                'min_quantity': 1,
                'max_quantity': 100000
            },
            {
                'nakrutka_id': 3822,
                'service_name': 'Telegram - Просмотры со статистикой | Казахстан',
                'service_type': 'views',
                'price_per_1000': 0.0225,
                'min_quantity': 1,
                'max_quantity': 100000
            },
            {
                'nakrutka_id': 4014,
                'service_name': 'Telegram - Просмотры со статистикой | США',
                'service_type': 'views',
                'price_per_1000': 0.0225,
                'min_quantity': 10,
                'max_quantity': 1000000
            },

            # Reaction services - mixes
            {
                'nakrutka_id': 3911,
                'service_name': 'Telegram - Позитивные реакции на пост 👍 ❤️ 🔥 🎉',
                'service_type': 'reactions',
                'price_per_1000': 0.0225,
                'min_quantity': 1,
                'max_quantity': 100000
            },
            {
                'nakrutka_id': 3870,
                'service_name': 'Telegram - Негативные реакции на пост 👎 😁 😢 💩 🤮 🤔 🤯 🤬',
                'service_type': 'reactions',
                'price_per_1000': 0.0225,
                'min_quantity': 1,
                'max_quantity': 100000
            },

            # Individual reactions
            {
                'nakrutka_id': 3850,
                'service_name': 'Telegram - Реакции на пост ❤️',
                'service_type': 'reactions',
                'price_per_1000': 0.0225,
                'min_quantity': 1,
                'max_quantity': 100000
            },
            {
                'nakrutka_id': 3849,
                'service_name': 'Telegram - Реакции на пост 🔥',
                'service_type': 'reactions',
                'price_per_1000': 0.0225,
                'min_quantity': 1,
                'max_quantity': 100000
            },
            {
                'nakrutka_id': 3838,
                'service_name': 'Telegram - Реакции на пост 👍',
                'service_type': 'reactions',
                'price_per_1000': 0.0225,
                'min_quantity': 1,
                'max_quantity': 100000
            },
            {
                'nakrutka_id': 3839,
                'service_name': 'Telegram - Реакции на пост ⚡️',
                'service_type': 'reactions',
                'price_per_1000': 0.0225,
                'min_quantity': 1,
                'max_quantity': 100000
            },
            {
                'nakrutka_id': 3872,
                'service_name': 'Telegram - Реакции на пост 🐳',
                'service_type': 'reactions',
                'price_per_1000': 0.0225,
                'min_quantity': 1,
                'max_quantity': 100000
            },

            # Reposts
            {
                'nakrutka_id': 3943,
                'service_name': 'Telegram - Репосты постов | Для статистики',
                'service_type': 'reposts',
                'price_per_1000': 0.2625,
                'min_quantity': 1,
                'max_quantity': 50000
            }
        ]

    async def add_channel_with_settings(
            self,
            username: str,
            channel_id: int,
            views_target: int,
            reactions_target: int,
            reposts_target: int,
            reaction_distribution: Dict[int, int],  # {service_id: quantity}
            randomize_reactions: bool = True,
            randomize_reposts: bool = True,
            randomize_percent: int = 40
    ) -> Optional[int]:
        """Add new channel with complete settings"""
        try:
            async with self.db.transaction() as conn:
                # Create channel
                channel_db_id = await conn.fetchval(
                    """
                    INSERT INTO channels (channel_username, channel_id, is_active)
                    VALUES ($1, $2, true) RETURNING id
                    """,
                    username, channel_id
                )

                # Get service IDs
                views_service = await conn.fetchval(
                    "SELECT nakrutka_id FROM services WHERE service_type = 'views' LIMIT 1"
                )
                reactions_service = await conn.fetchval(
                    "SELECT nakrutka_id FROM services WHERE service_type = 'reactions' LIMIT 1"
                )
                reposts_service = await conn.fetchval(
                    "SELECT nakrutka_id FROM services WHERE service_type = 'reposts' LIMIT 1"
                )

                # Create channel settings
                await conn.execute(
                    """
                    INSERT INTO channel_settings (channel_id, views_target, reactions_target, reposts_target,
                                                  views_service_id, reactions_service_id, reposts_service_id,
                                                  randomize_reactions, randomize_reposts, randomize_percent)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    """,
                    channel_db_id, views_target, reactions_target, reposts_target,
                    views_service, reactions_service, reposts_service,
                    randomize_reactions, randomize_reposts, randomize_percent
                )

                # Add reaction distribution
                for service_id, quantity in reaction_distribution.items():
                    # Get emoji from service
                    service = await conn.fetchrow(
                        "SELECT service_name FROM services WHERE nakrutka_id = $1",
                        service_id
                    )

                    emoji = self._extract_emoji_from_service_name(
                        service['service_name'] if service else ''
                    )

                    await conn.execute(
                        """
                        INSERT INTO channel_reaction_services
                            (channel_id, service_id, emoji, target_quantity)
                        VALUES ($1, $2, $3, $4)
                        """,
                        channel_db_id, service_id, emoji, quantity
                    )

                # Add default portion templates
                await self._add_default_portion_templates(conn, channel_db_id)

                logger.info(f"Added channel {username} with ID {channel_db_id}")
                return channel_db_id

        except Exception as e:
            logger.error(f"Failed to add channel: {e}")
            return None

    async def _add_default_portion_templates(self, conn, channel_id: int):
        """Add default portion templates for channel"""
        # Default templates based on the 70/30 distribution
        templates = [
            # Views - 5 portions
            ('views', 1, 29.13, 'quantity/134', 15, 0),
            ('views', 2, 22.50, 'quantity/115', 18, 0),
            ('views', 3, 17.04, 'quantity/98', 22, 0),
            ('views', 4, 13.09, 'quantity/86', 26, 0),
            ('views', 5, 18.24, 'quantity/23', 15, 156),

            # Reactions - 5 portions
            ('reactions', 1, 45.45, 'quantity/6', 12, 0),
            ('reactions', 2, 18.18, 'quantity/4', 15, 0),
            ('reactions', 3, 13.64, 'quantity/3', 18, 0),
            ('reactions', 4, 6.06, 'quantity/2', 20, 0),
            ('reactions', 5, 16.67, 'quantity/1', 15, 45),

            # Reposts - 5 portions
            ('reposts', 1, 26.09, 'quantity/2', 10, 5),
            ('reposts', 2, 17.39, 'quantity/2', 12, 5),
            ('reposts', 3, 13.04, 'quantity/1', 15, 5),
            ('reposts', 4, 8.70, 'quantity/1', 18, 5),
            ('reposts', 5, 34.78, 'quantity/2', 15, 45),
        ]

        for template in templates:
            await conn.execute(
                """
                INSERT INTO portion_templates (channel_id, service_type, portion_number,
                                               quantity_percent, runs_formula, interval_minutes,
                                               start_delay_minutes)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                channel_id, *template
            )

    def _extract_emoji_from_service_name(self, service_name: str) -> str:
        """Extract emoji from service name"""
        import re

        # Find emoji pattern
        emoji_pattern = r'[^\w\s,.\-()]{1,2}'
        emojis = re.findall(emoji_pattern, service_name)

        if emojis:
            return emojis[0]

        # Check for mix
        if 'микс' in service_name.lower() or 'mix' in service_name.lower():
            return 'mix'

        return 'unknown'

    async def verify_initialization(self) -> Dict[str, Any]:
        """Verify that initialization was successful"""
        result = {
            'services': 0,
            'channels': 0,
            'api_keys': 0,
            'errors': []
        }

        try:
            # Count services
            result['services'] = await self.db.fetchval(
                "SELECT COUNT(*) FROM services WHERE is_active = true"
            )

            # Count channels
            result['channels'] = await self.db.fetchval(
                "SELECT COUNT(*) FROM channels WHERE is_active = true"
            )

            # Count API keys
            result['api_keys'] = await self.db.fetchval(
                "SELECT COUNT(*) FROM api_keys WHERE is_active = true"
            )

            # Check for required services
            required_types = ['views', 'reactions', 'reposts']
            for service_type in required_types:
                count = await self.db.fetchval(
                    "SELECT COUNT(*) FROM services WHERE service_type = $1 AND is_active = true",
                    service_type
                )
                if count == 0:
                    result['errors'].append(f"No active {service_type} services")

            # Check for API key
            nakrutka_key = await self.db.fetchval(
                "SELECT COUNT(*) FROM api_keys WHERE service_name = 'nakrutka' AND is_active = true"
            )
            if nakrutka_key == 0:
                result['errors'].append("No Nakrutka API key configured")

        except Exception as e:
            result['errors'].append(f"Verification error: {str(e)}")

        return result


async def quick_setup_channel(
        db: DatabaseConnection,
        username: str,
        channel_id: int,
        views: int = 5000,
        reactions: int = 100,
        reposts: int = 50
):
    """Quick setup for a new channel with default settings"""
    initializer = DatabaseInitializer(db)

    # Default reaction distribution (70% positive mix, 30% individual)
    reaction_distribution = {
        3911: int(reactions * 0.7),  # Positive mix
        3850: int(reactions * 0.1),  # ❤️
        3849: int(reactions * 0.1),  # 🔥
        3838: int(reactions * 0.1),  # 👍
    }

    channel_id_db = await initializer.add_channel_with_settings(
        username=username,
        channel_id=channel_id,
        views_target=views,
        reactions_target=reactions,
        reposts_target=reposts,
        reaction_distribution=reaction_distribution
    )

    return channel_id_db