"""
Database tests
"""
import pytest
import asyncio
from datetime import datetime

from src.database.connection import DatabaseConnection, test_connection
from src.database.queries import Queries
from src.database.models import *
from src.config import POST_STATUS, ORDER_STATUS


@pytest.fixture
async def db():
    """Database connection fixture"""
    connection = DatabaseConnection()
    await connection.init()
    yield connection
    await connection.close()


@pytest.fixture
async def queries(db):
    """Queries fixture"""
    return Queries(db)


class TestDatabaseConnection:
    """Test database connection"""

    @pytest.mark.asyncio
    async def test_connection(self, db):
        """Test basic connection"""
        result = await test_connection()
        assert result is True

    @pytest.mark.asyncio
    async def test_execute_query(self, db):
        """Test query execution"""
        result = await db.fetchval("SELECT 1 + 1")
        assert result == 2

    @pytest.mark.asyncio
    async def test_transaction(self, db):
        """Test transaction"""
        async with db.transaction() as conn:
            # This should work within transaction
            await conn.execute(
                "INSERT INTO logs (level, message, context) VALUES ($1, $2, $3)",
                "info", "test", "{}"
            )

        # Verify insert
        count = await db.fetchval(
            "SELECT COUNT(*) FROM logs WHERE message = 'test'"
        )
        assert count >= 1


class TestQueries:
    """Test query functions"""

    @pytest.mark.asyncio
    async def test_get_active_channels(self, queries):
        """Test getting active channels"""
        channels = await queries.get_active_channels()
        assert isinstance(channels, list)

        if channels:
            channel = channels[0]
            assert isinstance(channel, Channel)
            assert hasattr(channel, 'channel_username')
            assert hasattr(channel, 'is_active')
            assert channel.is_active is True

    @pytest.mark.asyncio
    async def test_get_services(self, queries):
        """Test getting services"""
        services = await queries.get_services_by_type("views")
        assert isinstance(services, list)

        if services:
            service = services[0]
            assert isinstance(service, Service)
            assert service.service_type == "views"
            assert service.is_active is True

    @pytest.mark.asyncio
    async def test_create_and_get_post(self, queries):
        """Test creating and retrieving post"""
        # Get first active channel
        channels = await queries.get_active_channels()
        if not channels:
            pytest.skip("No active channels")

        channel = channels[0]
        test_post_id = 999999
        test_url = f"https://t.me/{channel.channel_username}/{test_post_id}"

        # Create post
        post_id = await queries.create_post(
            channel_id=channel.id,
            post_id=test_post_id,
            post_url=test_url
        )

        # If post already exists, it returns None
        if post_id:
            assert isinstance(post_id, int)

            # Get new posts
            new_posts = await queries.get_new_posts()
            assert any(p.id == post_id for p in new_posts)

    @pytest.mark.asyncio
    async def test_random_quantity(self, queries):
        """Test random quantity calculation"""
        base = 100
        percent = 40

        # Test multiple times to ensure randomness
        results = []
        for _ in range(10):
            result = await queries.calculate_random_quantity(base, percent)
            results.append(result)

            # Check bounds
            min_expected = base - (base * percent // 100)
            max_expected = base + (base * percent // 100)
            assert min_expected <= result <= max_expected

        # Check that we get different values (randomness)
        assert len(set(results)) > 1

    @pytest.mark.asyncio
    async def test_portion_calculation(self, queries):
        """Test portion calculation"""
        # Get first channel with templates
        channel_id = 1  # Assuming test channel exists

        portions = await queries.calculate_portion_details(
            channel_id=channel_id,
            service_type="views",
            total_quantity=1000
        )

        if portions:
            assert isinstance(portions, list)
            assert len(portions) == 5  # Should be 5 portions

            # Check total
            total = sum(p['quantity_per_run'] * p['runs'] for p in portions)
            assert abs(total - 1000) < 100  # Allow small deviation


class TestChannelSettings:
    """Test channel settings"""

    @pytest.mark.asyncio
    async def test_get_channel_settings(self, queries):
        """Test getting channel settings"""
        channels = await queries.get_active_channels()
        if not channels:
            pytest.skip("No active channels")

        channel = channels[0]
        settings = await queries.get_channel_settings(channel.id)

        if settings:
            assert isinstance(settings, ChannelSettings)
            assert settings.channel_id == channel.id
            assert settings.views_target >= 0
            assert settings.reactions_target >= 0
            assert settings.reposts_target >= 0

    @pytest.mark.asyncio
    async def test_reaction_services(self, queries):
        """Test getting reaction services"""
        channels = await queries.get_active_channels()
        if not channels:
            pytest.skip("No active channels")

        channel = channels[0]
        reaction_services = await queries.get_reaction_services(channel.id)

        assert isinstance(reaction_services, list)
        if reaction_services:
            rs = reaction_services[0]
            assert isinstance(rs, ChannelReactionService)
            assert rs.channel_id == channel.id


class TestStatistics:
    """Test statistics queries"""

    @pytest.mark.asyncio
    async def test_channel_stats(self, queries):
        """Test channel statistics"""
        channels = await queries.get_active_channels()
        if not channels:
            pytest.skip("No active channels")

        channel = channels[0]
        stats = await queries.get_channel_stats(channel.id)

        assert isinstance(stats, dict)
        assert 'total_posts' in stats
        assert 'completed_posts' in stats
        assert 'processing_posts' in stats
        assert 'failed_posts' in stats

        # Verify counts
        assert stats['total_posts'] >= 0
        assert stats['completed_posts'] >= 0
        assert stats['processing_posts'] >= 0
        assert stats['failed_posts'] >= 0

    @pytest.mark.asyncio
    async def test_today_costs(self, queries):
        """Test today's costs calculation"""
        costs = await queries.get_today_costs()

        assert isinstance(costs, dict)

        # Check that all values are floats
        for service_type, cost in costs.items():
            assert isinstance(cost, float)
            assert cost >= 0


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])