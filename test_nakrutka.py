# Створи файл test_nakrutka.py:

import asyncio
from src.services.nakrutka import NakrutkaClient


async def main():
    async with NakrutkaClient() as client:
        print("Testing Nakrutka API...")

        # Test balance
        try:
            balance = await client.get_balance()
            print(f"✅ Balance: {balance}")
        except Exception as e:
            print(f"❌ Balance error: {e}")

        # Test services
        try:
            services = await client.get_services()
            print(f"✅ Found {len(services)} services")
        except Exception as e:
            print(f"❌ Services error: {e}")


if __name__ == "__main__":
    asyncio.run(main())