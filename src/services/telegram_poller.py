"""
Telegram Polling Service - Автоматично отримує нові пости коли бот є адміном
"""
import asyncio
import logging
from datetime import datetime
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

from src.config import settings
from src.database.connection import db

logger = logging.getLogger(__name__)


class TelegramPoller:
    """Отримує нові пости через Telegram updates"""

    def __init__(self):
        self.db = db
        self.app = None
        self.processed_channels = {}  # Зберігаємо останній оброблений пост для кожного каналу

    async def setup(self):
        """Налаштування Telegram polling"""
        self.app = Application.builder().token(settings.telegram_bot_token).build()

        # Обробник для нових постів в каналах
        channel_handler = MessageHandler(
            filters.UpdateType.CHANNEL_POST,
            self.handle_new_post
        )
        self.app.add_handler(channel_handler)

        logger.info("✅ Telegram polling configured")

    async def start_polling(self):
        """Запуск polling"""
        await self.app.initialize()
        await self.app.start()
        logger.info("🟢 Started polling for new posts")

        # Запускаємо polling
        await self.app.updater.start_polling(
            allowed_updates=['channel_post'],  # Тільки пости з каналів
            drop_pending_updates=True  # Ігноруємо старі оновлення
        )

    async def handle_new_post(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обробка нового посту в каналі"""

        if not update.channel_post:
            return

        message = update.channel_post
        channel_telegram_id = message.chat.id
        channel_username = message.chat.username or str(channel_telegram_id)
        post_id = message.message_id

        logger.info(f"📨 New post #{post_id} in @{channel_username}")

        # Перевіряємо чи це наш канал
        channel = await self.db.fetchrow("""
            SELECT id, username, is_active, process_old_posts
            FROM channels 
            WHERE telegram_id = $1 OR username = $2
        """, channel_telegram_id, channel_username)

        if not channel:
            logger.debug(f"Channel @{channel_username} not tracked")
            return

        if not channel['is_active']:
            logger.debug(f"Channel @{channel_username} is inactive")
            return

        channel_id = channel['id']
        process_old_posts = channel['process_old_posts']

        # КРИТИЧНО: Перевіряємо чи це перший запуск
        existing_posts_count = await self.db.fetchval(
            "SELECT COUNT(*) FROM posts WHERE channel_id = $1",
            channel_id
        )

        # Якщо це ПЕРШИЙ запуск і process_old_posts = 0
        if existing_posts_count == 0 and process_old_posts == 0:
            # НЕ обробляємо цей пост, тільки зберігаємо як baseline
            logger.info(f"⚠️ First run for @{channel_username} with process_old_posts=0")
            logger.info(f"Setting baseline at post #{post_id}, NOT processing")

            await self.db.execute("""
                INSERT INTO posts (channel_id, post_id, post_url, is_processed)
                VALUES ($1, $2, $3, true)  -- is_processed = true, щоб НЕ створювати замовлення
                ON CONFLICT (channel_id, post_id) DO NOTHING
            """, channel_id, post_id, f"https://t.me/{channel_username}/{post_id}")

            self.processed_channels[channel_id] = post_id
            return

        # Якщо це перший запуск і треба обробити старі пости
        if existing_posts_count == 0 and process_old_posts > 0:
            logger.info(f"📦 First run: processing {process_old_posts} old posts")

            # Обробляємо старі пости (від post_id назад)
            for i in range(process_old_posts):
                old_post_id = post_id - i
                if old_post_id > 0:
                    await self.save_and_process_post(
                        channel_id,
                        channel_username,
                        old_post_id
                    )
            return

        # Звичайний режим - обробляємо тільки НОВІ пости
        post_url = f"https://t.me/{channel_username}/{post_id}"

        # Зберігаємо новий пост
        result = await self.db.execute("""
            INSERT INTO posts (channel_id, post_id, post_url, is_processed)
            VALUES ($1, $2, $3, false)
            ON CONFLICT (channel_id, post_id) DO NOTHING
            RETURNING id
        """, channel_id, post_id, post_url)

        if result and 'INSERT' in result:
            logger.info(f"✅ Saved new post: {post_url}")

            # Створюємо замовлення для нового посту
            await self.create_orders_for_post(channel_id, post_id, post_url)
        else:
            logger.debug(f"Post {post_url} already exists")

    async def save_and_process_post(self, channel_id: int, channel_username: str, post_id: int):
        """Зберігає пост та створює замовлення"""

        post_url = f"https://t.me/{channel_username}/{post_id}"

        result = await self.db.execute("""
            INSERT INTO posts (channel_id, post_id, post_url, is_processed)
            VALUES ($1, $2, $3, false)
            ON CONFLICT (channel_id, post_id) DO NOTHING
            RETURNING id
        """, channel_id, post_id, post_url)

        if result and 'INSERT' in result:
            logger.info(f"📝 Processing old post: {post_url}")
            await self.create_orders_for_post(channel_id, post_id, post_url)

    async def create_orders_for_post(self, channel_id: int, post_id: int, post_url: str):
        """Створює замовлення для посту згідно конфігурації"""

        # Отримуємо конфігурацію каналу
        config_row = await self.db.fetchrow("""
            SELECT config FROM channel_config 
            WHERE channel_id = $1
        """, channel_id)

        if not config_row or not config_row['config']:
            logger.warning(f"No config for channel {channel_id}")
            return

        config = config_row['config']

        # Отримуємо ID посту з БД
        post_record = await self.db.fetchrow("""
            SELECT id FROM posts 
            WHERE channel_id = $1 AND post_id = $2
        """, channel_id, post_id)

        if not post_record:
            logger.error(f"Post not found: {post_url}")
            return

        post_db_id = post_record['id']
        orders_created = 0

        # Створюємо замовлення для views
        if config.get('views', {}).get('enabled'):
            for portion in config['views'].get('portions', []):
                if portion.get('service_id') and portion.get('quantity'):
                    status = 'scheduled' if portion.get('delay', 0) > 0 else 'pending'

                    await self.db.execute("""
                        INSERT INTO ready_orders (
                            post_id, channel_id, service_id, service_type,
                            quantity, runs, interval, link, status, portion_number
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    """, post_db_id, channel_id, portion['service_id'], 'views',
                        portion['quantity'], portion.get('runs', 1),
                        portion.get('interval', 0), post_url, status,
                        portion.get('number', 1))

                    orders_created += 1

        # Створюємо замовлення для reactions
        if config.get('reactions', {}).get('enabled'):
            for reaction in config['reactions'].get('emojis', []):
                if reaction.get('service_id') and reaction.get('quantity'):
                    await self.db.execute("""
                        INSERT INTO ready_orders (
                            post_id, channel_id, service_id, service_type,
                            quantity, runs, interval, link, status, emoji
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    """, post_db_id, channel_id, reaction['service_id'], 'reactions',
                        reaction['quantity'], reaction.get('runs', 1),
                        reaction.get('interval', 0), post_url, 'pending',
                        reaction.get('emoji'))

                    orders_created += 1

        # Створюємо замовлення для reposts
        if config.get('reposts', {}).get('enabled'):
            reposts = config['reposts']
            if reposts.get('service_id') and reposts.get('quantity'):
                status = 'scheduled' if reposts.get('delay', 0) > 0 else 'pending'

                await self.db.execute("""
                    INSERT INTO ready_orders (
                        post_id, channel_id, service_id, service_type,
                        quantity, runs, interval, link, status
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """, post_db_id, channel_id, reposts['service_id'], 'reposts',
                    reposts['quantity'], reposts.get('runs', 1),
                    reposts.get('interval', 0), post_url, status)

                orders_created += 1

        # Позначаємо пост як оброблений
        await self.db.execute("""
            UPDATE posts 
            SET is_processed = true, processed_at = $1
            WHERE id = $2
        """, datetime.utcnow(), post_db_id)

        logger.info(f"✅ Created {orders_created} orders for post {post_url}")

    async def stop(self):
        """Зупинка polling"""
        if self.app:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
            logger.info("🔴 Stopped polling")