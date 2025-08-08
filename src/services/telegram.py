"""
Telegram channel monitoring service - SIMPLIFIED VERSION
"""
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import asyncio

from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError
from telegram.constants import ParseMode

from src.config import settings
from src.database.connection import DatabaseConnection
from src.database.queries import Queries
from src.database.models import Channel as DBChannel
from src.services.nakrutka import NakrutkaClient
from src.utils.logger import get_logger, DatabaseLogger
from src.utils.helpers import format_number, truncate_text

logger = get_logger(__name__)


class TelegramMonitor:
    """Simplified Telegram monitor - Bot API only"""

    def __init__(self, db: DatabaseConnection, nakrutka_client: Optional[NakrutkaClient] = None):
        self.db = db
        self.queries = Queries(db)
        self.db_logger = DatabaseLogger(db)
        self.nakrutka_client = nakrutka_client

        # Bot API
        self.bot = Bot(token=settings.telegram_bot_token)
        self.app: Optional[Application] = None

        # Simple cache
        self._posts_cache: Dict[int, List[int]] = {}
        self._cache_ttl = 300  # 5 minutes
        self._last_check: Dict[int, datetime] = {}
        self._min_check_interval = 20  # seconds

        # Stats
        self.start_time = datetime.utcnow()
        self._stats = {
            'posts_found': 0,
            'channels_checked': 0,
            'errors': 0
        }

    async def setup_bot(self):
        """Setup telegram bot handlers"""
        self.app = Application.builder().token(settings.telegram_bot_token).build()

        # Core commands only
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("status", self.cmd_status))
        self.app.add_handler(CommandHandler("channels", self.cmd_channels))
        self.app.add_handler(CommandHandler("balance", self.cmd_balance))
        self.app.add_handler(CommandHandler("help", self.cmd_help))

        # Admin channel management
        self.app.add_handler(CommandHandler("add_channel", self.cmd_add_channel))
        self.app.add_handler(CommandHandler("remove_channel", self.cmd_remove_channel))

        logger.info("Telegram bot handlers configured")

    async def start_bot(self):
        """Start telegram bot"""
        if self.app:
            await self.app.initialize()
            await self.app.start()

            # Start polling
            asyncio.create_task(self.app.updater.start_polling(
                allowed_updates=["message"],
                drop_pending_updates=True
            ))

            bot_info = await self.bot.get_me()
            logger.info(
                "Telegram bot started",
                username=bot_info.username,
                bot_id=bot_info.id
            )

    async def stop_bot(self):
        """Stop telegram bot"""
        if self.app:
            await self.app.updater.stop()
            await self.app.stop()
            logger.info("Telegram bot stopped")

    async def check_channels(self):
        """Check all active channels for new posts"""
        try:
            channels = await self.queries.get_active_channels()
            if not channels:
                return

            logger.info(f"Checking {len(channels)} channels")

            for channel in channels:
                # Rate limiting
                if not self._can_check_channel(channel.id):
                    continue

                try:
                    new_posts = await self.check_channel(channel)
                    if new_posts:
                        self._stats['posts_found'] += new_posts
                    self._stats['channels_checked'] += 1
                    self._last_check[channel.id] = datetime.utcnow()

                    await asyncio.sleep(1)  # Small delay between channels

                except Exception as e:
                    logger.error(f"Error checking {channel.channel_username}: {e}")
                    self._stats['errors'] += 1

        except Exception as e:
            logger.error(f"Failed to check channels: {e}")

    async def check_channel(self, channel: DBChannel) -> int:
        """Check single channel for new posts"""
        logger.debug(f"Checking channel {channel.channel_username}")

        # Get channel settings
        settings = await self.db.fetchrow(
            "SELECT * FROM channel_settings WHERE channel_id = $1",
            channel.id
        )

        if not settings:
            logger.warning(f"No settings for {channel.channel_username}")
            return 0

        # Check if first run
        existing_posts_count = await self.db.fetchval(
            "SELECT COUNT(*) FROM posts WHERE channel_id = $1",
            channel.id
        )

        is_first_run = existing_posts_count == 0
        posts_to_fetch = settings['process_old_posts_count'] if is_first_run else 50

        # Get recent posts (simplified - just IDs)
        recent_posts = await self.get_channel_posts(channel.channel_username, posts_to_fetch)

        if not recent_posts:
            return 0

        # Get existing posts from DB
        existing_posts = await self.queries.get_channel_posts(channel.id, limit=1000)
        existing_set = set(existing_posts)

        # Find new posts
        new_posts = [
            post_id for post_id in recent_posts
            if post_id not in existing_set
        ]

        if not new_posts:
            return 0

        # Limit for first run
        if is_first_run and settings['process_old_posts_count'] > 0:
            new_posts = new_posts[-settings['process_old_posts_count']:]

        logger.info(f"Found {len(new_posts)} new posts in {channel.channel_username}")

        # Save new posts
        created_count = 0
        for post_id in new_posts:
            post_url = f"https://t.me/{channel.channel_username}/{post_id}"

            result = await self.queries.create_post(
                channel_id=channel.id,
                post_id=post_id,
                post_url=post_url
            )

            if result:
                created_count += 1

        return created_count

    async def get_channel_posts(self, username: str, limit: int = 20) -> List[int]:
        """Get channel posts - simplified version"""
        username = username.lstrip('@')

        # Check cache
        cache_key = username
        if cache_key in self._posts_cache:
            cached_time, posts = self._posts_cache[cache_key]
            if (datetime.utcnow() - cached_time).total_seconds() < self._cache_ttl:
                return posts[:limit]

        # In simplified version, we just return empty or use stored data
        # Real implementation would need external service or different approach

        # For now, simulate getting posts
        # In production, this would need actual implementation
        posts = []

        # Update cache
        if posts:
            self._posts_cache[cache_key] = (datetime.utcnow(), posts)

        return posts[:limit]

    def _can_check_channel(self, channel_id: int) -> bool:
        """Check if enough time passed since last check"""
        last_check = self._last_check.get(channel_id)
        if last_check:
            time_since = (datetime.utcnow() - last_check).total_seconds()
            if time_since < self._min_check_interval:
                return False
        return True

    # ============ BOT COMMANDS (SIMPLIFIED) ============

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        message = (
            "🤖 <b>Telegram SMM Bot</b>\n\n"
            "Автоматична накрутка для Telegram каналів\n\n"
            "<b>Команди:</b>\n"
            "/status - Статус системи\n"
            "/channels - Список каналів\n"
            "/balance - Баланс Nakrutka\n"
            "/add_channel @username - Додати канал\n"
            "/remove_channel @username - Видалити канал\n"
            "/help - Допомога"
        )

        await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command"""
        if not self._is_admin(update.effective_user.id):
            return

        uptime = datetime.utcnow() - self.start_time
        uptime_hours = uptime.total_seconds() / 3600

        # Get stats
        try:
            active_orders = await self.db.fetchval(
                "SELECT COUNT(*) FROM orders WHERE status = 'in_progress'"
            )
            active_channels = await self.db.fetchval(
                "SELECT COUNT(*) FROM channels WHERE is_active = true"
            )
        except:
            active_orders = "?"
            active_channels = "?"

        message = (
            "🟢 <b>Статус системи</b>\n\n"
            f"⏱ Uptime: {uptime_hours:.1f} годин\n"
            f"📺 Активних каналів: {active_channels}\n"
            f"📋 Активних замовлень: {active_orders}\n"
            f"📊 Знайдено постів: {self._stats['posts_found']}\n"
            f"✅ Перевірено: {self._stats['channels_checked']}\n"
            f"❌ Помилок: {self._stats['errors']}"
        )

        await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    async def cmd_channels(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /channels command"""
        if not self._is_admin(update.effective_user.id):
            return

        try:
            channels = await self.db.fetch("""
                SELECT 
                    c.*,
                    cs.views_target,
                    cs.reactions_target,
                    cs.reposts_target
                FROM channels c
                LEFT JOIN channel_settings cs ON c.id = cs.channel_id
                WHERE c.is_active = true
                ORDER BY c.created_at DESC
                LIMIT 20
            """)

            if not channels:
                await update.message.reply_text("📺 Немає активних каналів")
                return

            message = "📺 <b>Активні канали</b>\n\n"

            for ch in channels:
                message += (
                    f"@{ch['channel_username']}\n"
                    f"├ 👁 {ch['views_target'] or 0} "
                    f"❤️ {ch['reactions_target'] or 0} "
                    f"🔄 {ch['reposts_target'] or 0}\n\n"
                )

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error(f"Error in cmd_channels: {e}")
            await update.message.reply_text("❌ Помилка")

    async def cmd_balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /balance command"""
        if not self._is_admin(update.effective_user.id):
            return

        if not self.nakrutka_client:
            await update.message.reply_text("❌ Nakrutka не налаштовано")
            return

        try:
            balance_info = await self.nakrutka_client.get_balance()
            balance = float(balance_info.get('balance', 0))
            currency = balance_info.get('currency', 'USD')

            if balance < 1:
                status = "🔴 Критично низький!"
            elif balance < 10:
                status = "🟡 Низький"
            else:
                status = "🟢 Нормальний"

            message = (
                f"💰 <b>Баланс Nakrutka</b>\n\n"
                f"Баланс: ${balance:.2f} {currency}\n"
                f"Статус: {status}"
            )

            if balance < 10:
                message += "\n\n⚠️ Рекомендується поповнити!"

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error(f"Error getting balance: {e}")
            await update.message.reply_text("❌ Помилка отримання балансу")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        if not self._is_admin(update.effective_user.id):
            return

        help_message = (
            "📚 <b>Допомога</b>\n\n"
            "/status - Статус системи\n"
            "/channels - Список каналів\n"
            "/balance - Баланс Nakrutka\n"
            "/add_channel @username [N] - Додати канал\n"
            "/remove_channel @username - Видалити канал\n\n"
            "N - кількість старих постів (0 = не обробляти)"
        )

        await update.message.reply_text(help_message, parse_mode=ParseMode.HTML)

    async def cmd_add_channel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /add_channel command"""
        if not self._is_admin(update.effective_user.id):
            return

        if not context.args:
            await update.message.reply_text("❌ Вкажіть @username")
            return

        username = context.args[0].lstrip('@').lower()
        process_old_posts = 0

        if len(context.args) > 1:
            try:
                process_old_posts = int(context.args[1])
                if process_old_posts < 0 or process_old_posts > 100:
                    await update.message.reply_text("❌ Кількість має бути 0-100")
                    return
            except ValueError:
                await update.message.reply_text("❌ Невірне число")
                return

        # Check if exists
        existing = await self.db.fetchrow(
            "SELECT * FROM channels WHERE channel_username = $1",
            username
        )

        if existing:
            if existing['is_active']:
                await update.message.reply_text(f"❌ @{username} вже активний")
            else:
                await self.db.execute(
                    "UPDATE channels SET is_active = true WHERE id = $1",
                    existing['id']
                )
                await update.message.reply_text(f"✅ @{username} активовано")
            return

        # Add new channel
        try:
            channel_id = await self.db.fetchval(
                "INSERT INTO channels (channel_username, channel_id, is_active) "
                "VALUES ($1, NULL, true) RETURNING id",
                username
            )

            await self.db.execute(
                "INSERT INTO channel_settings "
                "(channel_id, views_target, reactions_target, reposts_target, "
                "randomize_percent, process_old_posts_count) "
                "VALUES ($1, 1000, 50, 20, 30, $2)",
                channel_id, process_old_posts
            )

            await update.message.reply_text(
                f"✅ Канал @{username} додано!\n"
                f"Старі пости: {process_old_posts if process_old_posts > 0 else 'не обробляти'}"
            )

        except Exception as e:
            logger.error(f"Error adding channel: {e}")
            await update.message.reply_text(f"❌ Помилка: {str(e)[:100]}")

    async def cmd_remove_channel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /remove_channel command"""
        if not self._is_admin(update.effective_user.id):
            return

        if not context.args:
            await update.message.reply_text("❌ Вкажіть @username")
            return

        username = context.args[0].lstrip('@').lower()

        channel = await self.db.fetchrow(
            "SELECT * FROM channels WHERE channel_username = $1",
            username
        )

        if not channel:
            await update.message.reply_text(f"❌ @{username} не знайдено")
            return

        await self.db.execute(
            "UPDATE channels SET is_active = false WHERE id = $1",
            channel['id']
        )

        await update.message.reply_text(f"✅ Канал @{username} деактивовано")

    def _is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        return settings.admin_telegram_id and user_id == settings.admin_telegram_id

    def get_monitor_stats(self) -> Dict[str, Any]:
        """Get monitoring statistics"""
        return {
            'uptime_seconds': (datetime.utcnow() - self.start_time).total_seconds(),
            'posts_found': self._stats['posts_found'],
            'channels_checked': self._stats['channels_checked'],
            'errors': self._stats['errors']
        }