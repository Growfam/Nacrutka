# test_db_simple.py
import asyncio
import asyncpg
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()


async def test_methods():
    # Метод 1: URL з екрануванням
    print("Testing Method 1: URL with escaping...")
    try:
        password = quote_plus("Rostak987()")
        url = f"postgresql://postgres:{password}@db.fifyjyhlafmckyhdpraf.supabase.co:5432/postgres"

        conn = await asyncpg.connect(url)
        version = await conn.fetchval('SELECT version()')
        print(f"✅ Method 1 works! {version[:30]}...")
        await conn.close()
    except Exception as e:
        print(f"❌ Method 1 failed: {e}")

    print("\n" + "=" * 50 + "\n")

    # Метод 2: Окремі параметри
    print("Testing Method 2: Separate parameters...")
    try:
        conn = await asyncpg.connect(
            host='db.fifyjyhlafmckyhdpraf.supabase.co',
            port=5432,
            database='postgres',
            user='postgres',
            password='Rostak987()'
        )
        version = await conn.fetchval('SELECT version()')
        print(f"✅ Method 2 works! {version[:30]}...")
        await conn.close()
    except Exception as e:
        print(f"❌ Method 2 failed: {e}")


asyncio.run(test_methods())