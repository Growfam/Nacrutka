"""
Channel monitoring service - finds new posts
"""
import asyncio
import logging
from typing import List, Optional
from datetime import datetime
from telegram import Bot
from telegram.error import TelegramError

from src.config import settings
from src.database.connection import db

logger = logging.getLogger(__name__)


class ChannelMonitor:
    """Monitors Telegram channels for new posts"""

    def __init__(self):
        self.bot = Bot(token=settings.telegram_bot_token)
        self.db = db
        self.first_run_channels = set()  # Запам'ятовуємо канали при першому запуску

    async def check_channels(self):
        """Main monitoring function"""
        try:
            # Get active channels
            channels = await self.db.fetch("""
                SELECT id, username, telegram_id, process_old_posts
                FROM channels 
                WHERE is_active = true
            """)

            if not channels:
                logger.debug("No active channels to monitor")
                return

            logger.info(f"Checking {len(channels)} channels")

            for channel in channels:
                try:
                    await self.check_single_channel(
                        channel_id=channel['id'],
                        username=channel['username'],
                        telegram_id=channel['telegram_id'],
                        process_old_posts=channel['process_old_posts']
                    )
                    await asyncio.sleep(2)  # Rate limiting

                except Exception as e:
                    logger.error(f"Error checking channel {channel['username']}: {e}")

        except Exception as e:
            logger.error(f"Monitor check failed: {e}")

    async def check_single_channel(
        self,
        channel_id: int,
        username: str,
        telegram_id: Optional[int],
        process_old_posts: int
    ):
        """Check single channel for new posts"""

        logger.debug(f"Checking channel {username}, process_old_posts={process_old_posts}")

        # Перевіряємо чи це перший запуск для цього каналу
        existing_count = await self.db.fetchval(
            "SELECT COUNT(*) FROM posts WHERE channel_id = $1",
            channel_id
        )

        # Якщо це перший запуск І process_old_posts = 0
        if existing_count == 0 and process_old_posts == 0:
            # Запам'ятовуємо що це перший запуск з 0 старих постів
            if channel_id not in self.first_run_channels:
                logger.info(f"First run for {username} with process_old_posts=0, marking baseline")
                self.first_run_channels.add(channel_id)

                # Додаємо фіктивний маркер щоб позначити початкову точку
                # Це НЕ створить замовлення через is_processed=true
                last_post_id = await self.get_last_post_id(telegram_id or f"@{username}")
                if last_post_id:
                    await self.db.execute("""
                        INSERT INTO posts (channel_id, post_id, post_url, is_processed)
                        VALUES ($1, $2, $3, true)
                        ON CONFLICT (channel_id, post_id) DO NOTHING
                    """, channel_id, last_post_id, f"https://t.me/{username}/{last_post_id}")
                    logger.info(f"Set baseline at post {last_post_id} for {username}")
                return  # НЕ обробляємо нічого при першому запуску

        # Отримуємо останній відомий пост з БД
        last_known_post = await self.db.fetchval(
            "SELECT MAX(post_id) FROM posts WHERE channel_id = $1",
            channel_id
        )

        # Отримуємо поточний останній пост в каналі
        current_last_post = await self.get_last_post_id(telegram_id or f"@{username}")

        if not current_last_post:
            logger.warning(f"Could not get posts for {username}")
            return

        # Визначаємо які пости перевіряти
        posts_to_check = []

        if existing_count == 0 and process_old_posts > 0:
            # Перший запуск з обробкою старих постів
            logger.info(f"First run for {username}, processing {process_old_posts} old posts")
            for i in range(process_old_posts):
                post_id = current_last_post - i
                if post_id > 0:
                    posts_to_check.append(post_id)

        elif last_known_post and last_known_post < current_last_post:
            # Є нові пости після останнього відомого
            logger.info(f"Checking new posts for {username}: {last_known_post+1} to {current_last_post}")
            for post_id in range(last_known_post + 1, current_last_post + 1):
                posts_to_check.append(post_id)
                # Обмежуємо кількість за раз
                if len(posts_to_check) >= 10:
                    break
        else:
            logger.debug(f"No new posts for {username}")
            return

        if not posts_to_check:
            return

        # Перевіряємо які пости вже є в БД
        existing_posts = await self.db.fetch(
            "SELECT post_id FROM posts WHERE channel_id = $1 AND post_id = ANY($2)",
            channel_id, posts_to_check
        )
        existing_ids = {p['post_id'] for p in existing_posts}

        # Знаходимо справді нові пости
        new_posts = [pid for pid in posts_to_check if pid not in existing_ids]

        if not new_posts:
            logger.debug(f"All posts already exist for {username}")
            return

        logger.info(f"Found {len(new_posts)} new posts in {username}: {new_posts}")

        # Зберігаємо нові пости
        created_count = 0
        for post_id in new_posts:
            post_url = f"https://t.me/{username}/{post_id}"

            result = await self.db.execute("""
                INSERT INTO posts (channel_id, post_id, post_url, is_processed)
                VALUES ($1, $2, $3, false)
                ON CONFLICT (channel_id, post_id) DO NOTHING
            """, channel_id, post_id, post_url)

            if result and 'INSERT' in result:
                created_count += 1
                logger.info(f"Added new post: {post_url}")

        if created_count > 0:
            logger.info(f"Created {created_count} new posts for {username}")

    async def get_last_post_id(self, chat_identifier) -> Optional[int]:
        """Get last post ID from channel - using fixed estimate"""
        try:
            # Спробуємо отримати chat_id
            if isinstance(chat_identifier, int):
                chat_id = chat_identifier
            else:
                try:
                    chat = await self.bot.get_chat(chat_identifier)
                    chat_id = chat.id
                except:
                    chat_id = None

            # Bot API не дає доступ до історії постів
            # ТИМЧАСОВЕ РІШЕННЯ: використовуємо фіксований ID
            known_channels = {
                -1002352719919: 581,  # mark_crypto_inside - останній пост 581
                # Додайте інші канали тут
            }

            if chat_id and chat_id in known_channels:
                return known_channels[chat_id]

            # Для невідомих каналів - повертаємо 100 як початкову точку
            logger.warning(f"Unknown channel {chat_identifier}, using default post ID 100")
            return 100

        except Exception as e:
            logger.error(f"Could not get last post ID: {e}")
            return None