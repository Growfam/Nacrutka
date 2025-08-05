"""
Database connection management
"""
import asyncpg
from typing import Optional
from contextlib import asynccontextmanager

from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseConnection:
    """Manages database connection pool"""

    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def init(self):
        """Initialize connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                str(settings.database_url),
                min_size=5,
                max_size=20,
                max_queries=50000,
                max_inactive_connection_lifetime=300,
            )
            logger.info("Database connection pool created")
        except Exception as e:
            logger.error("Failed to create connection pool", error=str(e))
            raise

    async def close(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    @asynccontextmanager
    async def acquire(self):
        """Acquire connection from pool"""
        async with self.pool.acquire() as connection:
            yield connection

    @asynccontextmanager
    async def transaction(self):
        """Execute in transaction"""
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                yield connection

    async def execute(self, query: str, *args):
        """Execute query"""
        async with self.acquire() as connection:
            return await connection.execute(query, *args)

    async def fetch(self, query: str, *args):
        """Fetch multiple rows"""
        async with self.acquire() as connection:
            return await connection.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        """Fetch single row"""
        async with self.acquire() as connection:
            return await connection.fetchrow(query, *args)

    async def fetchval(self, query: str, *args):
        """Fetch single value"""
        async with self.acquire() as connection:
            return await connection.fetchval(query, *args)


# Global database instance
db = DatabaseConnection()


async def test_connection():
    """Test database connection"""
    try:
        result = await db.fetchval("SELECT 1")
        logger.info("Database connection test successful", result=result)
        return True
    except Exception as e:
        logger.error("Database connection test failed", error=str(e))
        return False