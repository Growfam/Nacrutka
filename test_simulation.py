# Створи файл test_simulation.py:

import asyncio
from src.database.connection import db


async def main():
    await db.init()

    print("Simulating new post processing...")

    # Run simulation
    result = await db.fetch("""
                            SELECT *
                            FROM simulate_new_post(1)
                            """)

    print("\n📊 Simulation results:")
    print(f"{'Type':<12} {'Total':<8} {'#':<3} {'Formula':<12} {'Sum':<6} {'Interval':<10} {'Start':<10}")
    print("-" * 70)

    for row in result:
        print(f"{row['service_type']:<12} {row['total_quantity']:<8} {row['portion_number']:<3} "
              f"{row['formula']:<12} {row['actual_total']:<6} {row['interval_min']:<10} {row['start_at']:<10}")

    await db.close()


if __name__ == "__main__":
    asyncio.run(main())