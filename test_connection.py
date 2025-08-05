# Створи файл test_connection.py в корені проекту:

import asyncio
from src.database.connection import db, test_connection
from src.config import settings


async def main():
    print("Testing database connection...")
    print(f"Database URL: {settings.database_url}")

    await db.init()
    result = await test_connection()

    if result:
        print("✅ Database connection successful!")

        # Test query
        version = await db.fetchval("SELECT version()")
        print(f"PostgreSQL version: {version}")

        # Check tables
        tables = await db.fetch("""
                                SELECT tablename
                                FROM pg_tables
                                WHERE schemaname = 'public'
                                ORDER BY tablename
                                """)
        print(f"\nFound {len(tables)} tables:")
        for table in tables:
            print(f"  - {table['tablename']}")
    else:
        print("❌ Database connection failed!")

    await db.close()


if __name__ == "__main__":
    asyncio.run(main())