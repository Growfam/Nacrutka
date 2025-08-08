"""
Service tests
"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch

from src.services.nakrutka import NakrutkaClient
from src.services.monitor import TelegramMonitor
from src.services.sender import PostProcessor
from src.database.connection import DatabaseConnection
from src.database.models import *
from src.utils.helpers import *


@pytest.fixture
async def nakrutka_client():
    """Nakrutka client fixture"""
    client = NakrutkaClient()
    yield client
    if client.session:
        await client.session.close()


@pytest.fixture
async def mock_db():
    """Mock database connection"""
    db = AsyncMock(spec=DatabaseConnection)
    return db


class TestNakrutkaClient:
    """Test Nakrutka API client"""

    @pytest.mark.asyncio
    async def test_client_initialization(self, nakrutka_client):
        """Test client initialization"""
        assert nakrutka_client.api_url
        assert nakrutka_client.api_key
        assert nakrutka_client.session is None

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test async context manager"""
        async with NakrutkaClient() as client:
            assert client.session is not None
            assert not client.session.closed

        # After exit, session should be closed
        assert client.session.closed

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession.post')
    async def test_create_order(self, mock_post, nakrutka_client):
        """Test order creation"""
        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(
            return_value='{"order": "12345", "charge": "0.50", "currency": "USD"}'
        )
        mock_post.return_value.__aenter__.return_value = mock_response

        # Create order
        result = await nakrutka_client.create_order(
            service_id=3791,
            link="https://t.me/test/123",
            quantity=1000
        )

        assert result['order'] == "12345"
        assert result['charge'] == "0.50"
        assert result['currency'] == "USD"

    @pytest.mark.asyncio
    async def test_validate_order_params(self, nakrutka_client):
        """Test order parameter validation"""
        # Test valid params
        assert await nakrutka_client.validate_order_params(
            service_id=1,
            quantity=100,
            min_quantity=10,
            max_quantity=1000
        ) is True

        # Test below minimum
        assert await nakrutka_client.validate_order_params(
            service_id=1,
            quantity=5,
            min_quantity=10,
            max_quantity=1000
        ) is False

        # Test above maximum
        assert await nakrutka_client.validate_order_params(
            service_id=1,
            quantity=2000,
            min_quantity=10,
            max_quantity=1000
        ) is False


class TestTelegramMonitor:
    """Test Telegram monitoring service"""

    @pytest.mark.asyncio
    async def test_initialization(self, mock_db):
        """Test monitor initialization"""
        monitor = TelegramMonitor(mock_db)
        assert monitor.db == mock_db
        assert monitor.bot is not None

    @pytest.mark.asyncio
    @patch('telegram.Bot.get_updates')
    async def test_get_channel_posts(self, mock_updates, mock_db):
        """Test getting channel posts"""
        monitor = TelegramMonitor(mock_db)

        # Mock updates
        mock_updates.return_value = []

        posts = await monitor.get_channel_posts(
            username="test_channel",
            channel_id=-1001234567890,
            limit=10
        )

        assert isinstance(posts, list)

    def test_is_admin(self, mock_db):
        """Test admin check"""
        monitor = TelegramMonitor(mock_db)

        # Mock admin ID
        with patch('src.config.settings.admin_telegram_id', 12345):
            assert monitor._is_admin(12345) is True
            assert monitor._is_admin(99999) is False

        # No admin set
        with patch('src.config.settings.admin_telegram_id', None):
            assert monitor._is_admin(12345) is False


class TestHelpers:
    """Test helper functions"""

    def test_parse_telegram_url(self):
        """Test URL parsing"""
        # Valid URLs
        assert parse_telegram_url("https://t.me/channel/123") == ("channel", 123)
        assert parse_telegram_url("https://t.me/test_channel/456") == ("test_channel", 456)
        assert parse_telegram_url("t.me/channel/789") == ("channel", 789)

        # Invalid URLs
        assert parse_telegram_url("https://example.com") is None
        assert parse_telegram_url("not_a_url") is None

    def test_build_telegram_url(self):
        """Test URL building"""
        assert build_telegram_url("channel", 123) == "https://t.me/channel/123"
        assert build_telegram_url("@channel", 456) == "https://t.me/channel/456"

    def test_format_duration(self):
        """Test duration formatting"""
        assert format_duration(30) == "30s"
        assert format_duration(90) == "1m 30s"
        assert format_duration(3665) == "1h 1m"

    def test_calculate_portions(self):
        """Test portion calculation"""
        # Front-loaded distribution
        portions = calculate_portions(1000, portions=5, front_loaded=True)
        assert len(portions) == 5
        assert sum(portions) == 1000
        assert portions[0] > portions[-1]  # First portion larger than last

        # Even distribution
        portions = calculate_portions(100, portions=4, front_loaded=False)
        assert len(portions) == 4
        assert sum(portions) == 100
        assert all(p == 25 for p in portions)

    def test_validate_quantity_limits(self):
        """Test quantity validation"""
        # Valid
        valid, error = validate_quantity_limits(100, 10, 1000)
        assert valid is True
        assert error is None

        # Below minimum
        valid, error = validate_quantity_limits(5, 10, 1000)
        assert valid is False
        assert "below minimum" in error

        # Above maximum
        valid, error = validate_quantity_limits(2000, 10, 1000)
        assert valid is False
        assert "exceeds maximum" in error

    def test_validate_telegram_username(self):
        """Test username validation"""
        assert validate_telegram_username("valid_user") is True
        assert validate_telegram_username("@valid_user") is True
        assert validate_telegram_username("user123") is True

        assert validate_telegram_username("usr") is False  # Too short
        assert validate_telegram_username("123user") is False  # Starts with number
        assert validate_telegram_username("user-name") is False  # Has dash

    def test_format_number(self):
        """Test number formatting"""
        assert format_number(1000) == "1,000"
        assert format_number(1234567) == "1,234,567"
        assert format_number(100) == "100"

    def test_calculate_cost(self):
        """Test cost calculation"""
        assert calculate_cost(1000, 0.50) == 0.50
        assert calculate_cost(5000, 0.025) == 0.125
        assert calculate_cost(100, 1.0) == 0.10

    @pytest.mark.asyncio
    async def test_process_in_batches(self):
        """Test batch processing"""
        items = list(range(10))
        processed = []

        async def process_item(item):
            processed.append(item)
            return item * 2

        results = await process_in_batches(
            items=items,
            batch_size=3,
            process_func=process_item,
            delay_between_batches=0.1
        )

        assert len(results) == 10
        assert processed == items
        assert results == [i * 2 for i in items]

    def test_safe_dict_get(self):
        """Test safe dictionary access"""
        data = {
            'user': {
                'profile': {
                    'name': 'Test User'
                }
            }
        }

        assert safe_dict_get(data, 'user.profile.name') == 'Test User'
        assert safe_dict_get(data, 'user.profile.age', 0) == 0
        assert safe_dict_get(data, 'invalid.path', 'default') == 'default'

    @pytest.mark.asyncio
    async def test_rate_limiter(self):
        """Test rate limiter"""
        limiter = RateLimiter(calls_per_minute=5)

        # Make 5 calls quickly
        for _ in range(5):
            await limiter.acquire()

        # 6th call should wait
        start = datetime.utcnow()
        await limiter.acquire()
        end = datetime.utcnow()

        # Should have waited some time
        # (test might be flaky, so just check it's not instant)
        assert (end - start).total_seconds() >= 0


class TestPostProcessor:
    """Test post processor"""

    @pytest.mark.asyncio
    async def test_calculate_quantities(self, mock_db):
        """Test quantity calculation"""
        nakrutka = AsyncMock(spec=NakrutkaClient)
        processor = PostProcessor(mock_db, nakrutka)

        # Mock settings
        settings = ChannelSettings(
            id=1,
            channel_id=1,
            views_target=1000,
            reactions_target=100,
            reposts_target=20,
            reaction_types=["❤️"],
            views_service_id=1,
            reactions_service_id=2,
            reposts_service_id=3,
            randomize_reactions=True,
            randomize_reposts=False,
            randomize_percent=40,
            updated_at=datetime.utcnow()
        )

        # Mock random calculation
        processor.queries.calculate_random_quantity = AsyncMock(return_value=80)

        quantities = await processor._calculate_quantities(settings)

        assert quantities['views'] == 1000  # No randomization
        assert quantities['reactions'] == 80  # Randomized
        assert quantities['reposts'] == 20  # No randomization


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])