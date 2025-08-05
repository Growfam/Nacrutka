# Створи файл test_channels.py:

import asyncio
from src.database.connection import db
from src.database.queries import Queries


async def main():
    await db.init()
    queries = Queries(db)

    print("Testing channel setup...")

    # Get channels
    channels = await queries.get_active_channels()
    print(f"\nActive channels: {len(channels)}")

    for channel in channels:
        print(f"\n📢 Channel: {channel.channel_username}")
        print(f"   ID: {channel.channel_id}")
        print(f"   Active: {channel.is_active}")

        # Get settings
        settings = await queries.get_channel_settings(channel.id)
        if settings:
            print(f"   Views: {settings.views_target}")
            print(f"   Reactions: {settings.reactions_target} (±{settings.randomize_percent}%)")
            print(f"   Reposts: {settings.reposts_target} (±{settings.randomize_percent}%)")

            # Test randomization
            random_reactions = await queries.calculate_random_quantity(
                settings.reactions_target,
                settings.randomize_percent
            )
            random_reposts = await queries.calculate_random_quantity(
                settings.reposts_target,
                settings.randomize_percent
            )
            print(f"   Random test: {random_reactions} reactions, {random_reposts} reposts")

        # Get reaction services
        reaction_services = await queries.get_reaction_services(channel.id)
        if reaction_services:
            print("   Reaction distribution:")
            for rs in reaction_services:
                print(f"     - Service {rs.service_id}: {rs.target_quantity} ({rs.emoji})")

    await db.close()


if __name__ == "__main__":
    asyncio.run(main())