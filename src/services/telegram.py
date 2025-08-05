"""
Telegram channel monitoring service with Nakrutka integration
"""
from typing import List, Optional, Dict, Any, Set
from datetime import datetime, timedelta
import asyncio
import re
import html

from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from telegram.error import TelegramError, BadRequest, Forbidden
from telegram.constants import ParseMode
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.errors import SessionPasswordNeededError

from src.config import settings
from src.database.connection import DatabaseConnection
from src.database.queries import Queries
from src.database.models import Channel, Post
from src.services.nakrutka import NakrutkaClient
from src.utils.logger import get_logger, DatabaseLogger
from src.utils.helpers import (
    validate_telegram_channel_id,
    format_number,
    format_price,
    truncate_text,
    format_duration
)

logger = get_logger(__name__)


class TelegramMonitor:
    """Monitors Telegram channels for new posts with Nakrutka integration"""

    def __init__(self, db: DatabaseConnection, nakrutka_client: Optional[NakrutkaClient] = None):
        self.db = db
        self.queries = Queries(db)
        self.db_logger = DatabaseLogger(db)
        self.bot = Bot(token=settings.telegram_bot_token)
        self.app: Optional[Application] = None
        self.nakrutka_client = nakrutka_client

        # Start time for uptime calculation
        self.start_time = datetime.utcnow()

        # Cache for channel posts to avoid duplicates
        self._channel_cache: Dict[int, Set[int]] = {}
        self._cache_ttl = 300  # 5 minutes
        self._cache_updated: Dict[int, datetime] = {}

        # Rate limiting
        self._last_check: Dict[int, datetime] = {}
        self._min_check_interval = 20  # seconds between channel checks

        # Error tracking
        self._channel_errors: Dict[int, List[str]] = {}
        self._max_errors_per_channel = 5

        # Admin commands tracking
        self._admin_commands_count = 0

    async def setup_bot(self):
        """Setup telegram bot handlers"""
        self.app = Application.builder().token(settings.telegram_bot_token).build()

        # Add command handlers
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("status", self.cmd_status))
        self.app.add_handler(CommandHandler("stats", self.cmd_stats))
        self.app.add_handler(CommandHandler("channels", self.cmd_channels))
        self.app.add_handler(CommandHandler("costs", self.cmd_costs))
        self.app.add_handler(CommandHandler("balance", self.cmd_balance))
        self.app.add_handler(CommandHandler("orders", self.cmd_orders))
        self.app.add_handler(CommandHandler("errors", self.cmd_errors))
        self.app.add_handler(CommandHandler("cache", self.cmd_cache))
        self.app.add_handler(CommandHandler("health", self.cmd_health))
        self.app.add_handler(CommandHandler("help", self.cmd_help))

        # Add message handler for non-command messages
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

        # Error handler
        self.app.add_error_handler(self.error_handler)

        logger.info("Telegram bot handlers configured")

    async def start_bot(self):
        """Start telegram bot (for handling commands)"""
        if self.app:
            await self.app.initialize()
            await self.app.start()

            # Start polling in background
            asyncio.create_task(self.app.updater.start_polling(
                allowed_updates=["message", "callback_query"],
                drop_pending_updates=True
            ))

            # Log bot info
            bot_info = await self.bot.get_me()
            logger.info(
                "Telegram bot started",
                username=bot_info.username,
                bot_id=bot_info.id,
                can_read_all_group_messages=bot_info.can_read_all_group_messages
            )

            await self.db_logger.info(
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
                logger.debug("No active channels to monitor")
                return

            logger.info(f"Checking {len(channels)} active channels for new posts")

            # Process channels with rate limiting
            results = []
            for channel in channels:
                # Check if channel has too many errors
                channel_errors = self._channel_errors.get(channel.id, [])
                if len(channel_errors) >= self._max_errors_per_channel:
                    logger.warning(
                        f"Skipping {channel.channel_username} - too many errors",
                        error_count=len(channel_errors)
                    )
                    continue

                # Check rate limit
                last_check = self._last_check.get(channel.id)
                if last_check:
                    time_since = (datetime.utcnow() - last_check).total_seconds()
                    if time_since < self._min_check_interval:
                        logger.debug(
                            f"Skipping {channel.channel_username} - checked {time_since:.1f}s ago"
                        )
                        continue

                # Check channel
                try:
                    result = await self.check_channel(channel)
                    results.append(result)
                    self._last_check[channel.id] = datetime.utcnow()

                    # Clear errors on success
                    if channel.id in self._channel_errors:
                        del self._channel_errors[channel.id]

                    # Small delay between channels
                    await asyncio.sleep(1)

                except Exception as e:
                    logger.error(
                        f"Error checking channel {channel.channel_username}",
                        error=str(e),
                        channel_id=channel.id,
                        exc_info=True
                    )

                    # Track error
                    if channel.id not in self._channel_errors:
                        self._channel_errors[channel.id] = []
                    self._channel_errors[channel.id].append(str(e))

                    results.append(None)

            # Count results
            new_posts = sum(r or 0 for r in results if isinstance(r, int))
            errors = sum(1 for r in results if r is None)
            skipped = len(channels) - len(results)

            if new_posts > 0:
                logger.info(
                    f"Channel check completed",
                    new_posts=new_posts,
                    channels_checked=len(results),
                    errors=errors,
                    skipped=skipped
                )
                await self.db_logger.info(
                    "New posts found",
                    count=new_posts,
                    channels_checked=len(channels)
                )

            if errors > 0:
                logger.warning(f"Failed to check {errors} channels")

        except Exception as e:
            logger.error("Failed to check channels", error=str(e), exc_info=True)
            await self.db_logger.error(
                "Channel check failed",
                error=str(e),
                error_type=type(e).__name__
            )

    async def check_channel(self, channel: Channel) -> Optional[int]:
        """Check single channel for new posts"""
        try:
            logger.debug(f"Checking channel {channel.channel_username}")

            # Get recent posts from channel
            recent_posts = await self.get_channel_posts(
                channel.channel_username,
                channel.channel_id,
                limit=50  # Check last 50 posts
            )

            if not recent_posts:
                logger.debug(f"No posts found in {channel.channel_username}")
                return 0

            # Get existing posts from DB (last 1000 to be safe)
            existing_posts = await self.queries.get_channel_posts(
                channel.id,
                limit=1000
            )
            existing_set = set(existing_posts)

            # Find new posts
            new_posts = [
                post_id for post_id in recent_posts
                if post_id not in existing_set
            ]

            if not new_posts:
                logger.debug(f"No new posts in {channel.channel_username}")
                return 0

            logger.info(
                f"Found {len(new_posts)} new posts in {channel.channel_username}",
                post_ids=new_posts[:5]  # Log first 5
            )

            # Save new posts to database
            created_count = 0
            for post_id in new_posts:
                post_url = f"https://t.me/{channel.channel_username}/{post_id}"

                # Create post record
                result = await self.queries.create_post(
                    channel_id=channel.id,
                    post_id=post_id,
                    post_url=post_url
                )

                if result:
                    created_count += 1
                    logger.debug(f"Created post record {result} for {post_url}")
                else:
                    logger.debug(f"Post {post_id} already exists")

            if created_count > 0:
                await self.db_logger.info(
                    "New posts saved",
                    channel=channel.channel_username,
                    count=created_count,
                    post_ids=new_posts[:10]
                )

            return created_count

        except Exception as e:
            logger.error(
                f"Failed to check channel {channel.channel_username}",
                error=str(e),
                channel_id=channel.id,
                exc_info=True
            )
            # Don't propagate error - continue with other channels
            return None

    async def get_channel_posts(
        self,
        username: str,
        channel_id: int,
        limit: int = 20
    ) -> List[int]:
        """Get recent post IDs from channel using multiple methods"""

        # Clean username
        username = username.lstrip('@')

        # Check cache first
        cache_key = channel_id
        if cache_key in self._channel_cache:
            cache_age = datetime.utcnow() - self._cache_updated.get(cache_key, datetime.min)
            if cache_age.total_seconds() < self._cache_ttl:
                cached_posts = list(self._channel_cache[cache_key])
                logger.debug(f"Using cached posts for {username}: {len(cached_posts)} posts")
                return sorted(cached_posts)[-limit:]

        try:
            posts = []

            # Method 1: Try public channel access
            if not str(channel_id).startswith('-100'):
                # Public channel - try to access via username
                posts = await self._get_public_channel_posts(username, limit)

            # Method 2: Try bot API if bot is admin
            if not posts and validate_telegram_channel_id(channel_id):
                posts = await self._get_private_channel_posts(channel_id, limit)

            # Method 3: Parse from Telegram web (as fallback)
            if not posts:
                posts = await self._get_channel_posts_from_web(username, limit)

            # Update cache
            if posts:
                if cache_key not in self._channel_cache:
                    self._channel_cache[cache_key] = set()
                self._channel_cache[cache_key].update(posts)
                self._cache_updated[cache_key] = datetime.utcnow()

                # Clean old posts from cache (keep last 2000)
                if len(self._channel_cache[cache_key]) > 2000:
                    sorted_posts = sorted(self._channel_cache[cache_key])
                    self._channel_cache[cache_key] = set(sorted_posts[-2000:])

            logger.debug(f"Found {len(posts)} posts in {username}")
            return sorted(posts)[-limit:]

        except Exception as e:
            logger.error(
                f"Failed to get posts from {username}",
                error=str(e),
                channel_id=channel_id,
                exc_info=True
            )
            return []

    async def _get_public_channel_posts(self, username: str, limit: int) -> List[int]:
        """Get posts from public channel"""
        try:
            # For public channels, we need to use different approach
            # Bot API doesn't allow getting channel history directly

            # Try to get channel info first
            try:
                chat = await self.bot.get_chat(f"@{username}")
                logger.debug(f"Got chat info for @{username}: {chat.type}")

                # If we can access the chat, we might be able to get some recent messages
                # This is limited but better than nothing
                if hasattr(chat, 'linked_chat_id') and chat.linked_chat_id:
                    # Try to get from linked chat
                    pass

            except (BadRequest, Forbidden) as e:
                logger.debug(f"Cannot access @{username} via bot API: {e}")

            # Parse from web as primary method for public channels
            return await self._get_channel_posts_from_web(username, limit)

        except Exception as e:
            logger.error(f"Error getting public channel posts: {e}")
            return []

    async def _get_private_channel_posts(self, channel_id: int, limit: int) -> List[int]:
        """Get posts from private channel where bot is admin"""
        try:
            # This only works if bot is admin in channel
            # Try to get chat to verify access
            try:
                chat = await self.bot.get_chat(channel_id)
                logger.debug(f"Bot has access to channel {channel_id}: {chat.title}")
            except (BadRequest, Forbidden):
                logger.debug(f"Bot doesn't have access to channel {channel_id}")
                return []

            # Unfortunately, Bot API doesn't have a direct method to get message history
            # We would need to use MTProto (Telethon) for this
            # For now, return empty and rely on web parsing
            return []

        except Exception as e:
            logger.debug(f"Cannot get private channel posts: {e}")
            return []

    async def _get_channel_posts_from_web(self, username: str, limit: int) -> List[int]:
        """Parse channel posts from Telegram web preview"""
        try:
            import aiohttp
            from bs4 import BeautifulSoup

            url = f"https://t.me/s/{username}"

            async with aiohttp.ClientSession() as session:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 404:
                        logger.warning(f"Channel not found: {username}")
                        return []

                    if response.status != 200:
                        logger.warning(f"Failed to fetch {url}: {response.status}")
                        return []

                    html_content = await response.text()

            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find message containers
            posts = []

            # Look for message divs with data-post attribute
            for message in soup.find_all('div', {'class': 'tgme_widget_message'}):
                post_attr = message.get('data-post')
                if post_attr:
                    # Extract post ID from data-post attribute (format: channel/123)
                    parts = post_attr.split('/')
                    if len(parts) == 2 and parts[1].isdigit():
                        posts.append(int(parts[1]))

            # Alternative: find links to posts
            if not posts:
                for link in soup.find_all('a', href=re.compile(rf't\.me/{username}/\d+')):
                    match = re.search(rf't\.me/{username}/(\d+)', link.get('href', ''))
                    if match:
                        post_id = int(match.group(1))
                        if post_id not in posts:
                            posts.append(post_id)

            # Remove duplicates and sort
            posts = sorted(list(set(posts)))

            logger.debug(f"Parsed {len(posts)} posts from web for {username}")
            return posts[-limit:]

        except Exception as e:
            logger.error(f"Failed to parse web posts: {e}", exc_info=True)
            return []

    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle errors in telegram bot"""
        logger.error(
            "Exception while handling an update",
            error=str(context.error),
            exc_info=context.error
        )

        # Notify admin about error
        if settings.admin_telegram_id and update and hasattr(update, 'effective_message'):
            try:
                await context.bot.send_message(
                    chat_id=settings.admin_telegram_id,
                    text=f"⚠️ Error: {str(context.error)[:200]}"
                )
            except:
                pass

    # ============ BOT COMMANDS ============

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        self._admin_commands_count += 1

        welcome_message = (
            "🤖 <b>Telegram SMM Bot</b>\n\n"
            "Автоматична система накрутки для Telegram каналів\n\n"
            "<b>Основні команди:</b>\n"
            "/status - Статус системи\n"
            "/channels - Список каналів\n"
            "/stats - Статистика по каналах\n"
            "/costs - Витрати на накрутку\n"
            "/balance - Баланс Nakrutka\n"
            "/orders - Активні замовлення\n"
            "/errors - Помилки системи\n"
            "/health - Перевірка здоров'я\n"
            "/help - Детальна допомога\n\n"
            "<i>Бот працює 24/7 в автоматичному режимі</i>"
        )

        await update.message.reply_text(
            welcome_message,
            parse_mode=ParseMode.HTML
        )

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command with Nakrutka integration"""
        if not self._is_admin(update.effective_user.id):
            return

        self._admin_commands_count += 1

        try:
            # Send initial message
            msg = await update.message.reply_text("⏳ Збираю інформацію...")

            # Get system status
            channels = await self.queries.get_active_channels()

            # Get today's stats
            today_costs = await self.queries.get_today_costs()
            total_cost = sum(today_costs.values())

            # Get active orders
            active_orders = await self.queries.get_active_orders()

            # Get recent posts
            recent_posts = await self.db.fetch(
                """
                SELECT COUNT(*) as count, status
                FROM posts
                WHERE created_at > NOW() - INTERVAL '24 hours'
                GROUP BY status
                """
            )

            post_stats = {row['status']: row['count'] for row in recent_posts}

            # Calculate uptime
            uptime = datetime.utcnow() - self.start_time
            uptime_str = format_duration(int(uptime.total_seconds()))

            # Get Nakrutka balance if client available
            balance_text = "N/A"
            if self.nakrutka_client:
                try:
                    balance_info = await self.nakrutka_client.get_balance()
                    balance = balance_info.get('balance', 0)
                    currency = balance_info.get('currency', 'USD')
                    balance_text = f"${balance:.2f} {currency}"

                    # Add warning if low
                    if balance < 10:
                        balance_text += " ⚠️"
                    elif balance < 1:
                        balance_text += " ❌"
                except Exception as e:
                    logger.error(f"Failed to get Nakrutka balance: {e}")
                    balance_text = "Error ❌"
            else:
                balance_text = "Not configured ⚠️"

            # Format message
            message = (
                "📊 <b>Статус системи</b>\n\n"
                f"⏱ Uptime: {uptime_str}\n"
                f"✅ Активних каналів: {len(channels)}\n"
                f"📝 Активних замовлень: {len(active_orders)}\n\n"
                f"<b>Пости за 24 години:</b>\n"
                f"🆕 Нові: {post_stats.get('new', 0)}\n"
                f"⏳ В обробці: {post_stats.get('processing', 0)}\n"
                f"✅ Завершені: {post_stats.get('completed', 0)}\n"
                f"❌ Помилки: {post_stats.get('failed', 0)}\n\n"
                f"<b>Витрати сьогодні:</b>\n"
                f"👁 Перегляди: ${today_costs.get('views', 0):.2f}\n"
                f"❤️ Реакції: ${today_costs.get('reactions', 0):.2f}\n"
                f"🔄 Репости: ${today_costs.get('reposts', 0):.2f}\n"
                f"💰 Всього: ${total_cost:.2f}\n\n"
                f"<b>Баланс Nakrutka:</b> {balance_text}\n\n"
                f"<i>Оновлено: {datetime.utcnow().strftime('%H:%M:%S UTC')}</i>"
            )

            await msg.edit_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error("Failed to get status", error=str(e), exc_info=True)
            await update.message.reply_text("❌ Помилка отримання статусу")

    async def cmd_channels(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /channels command"""
        if not self._is_admin(update.effective_user.id):
            return

        self._admin_commands_count += 1

        try:
            channels = await self.queries.get_active_channels()

            if not channels:
                await update.message.reply_text("📢 Немає активних каналів")
                return

            message = "📢 <b>Активні канали:</b>\n\n"

            for i, channel in enumerate(channels, 1):
                # Get settings
                settings = await self.queries.get_channel_settings(channel.id)

                # Get stats
                stats = await self.queries.get_channel_stats(channel.id)

                # Check cache status
                is_cached = channel.id in self._channel_cache
                cache_size = len(self._channel_cache.get(channel.id, [])) if is_cached else 0

                message += (
                    f"<b>{i}. {channel.channel_username}</b>\n"
                    f"├ ID: <code>{channel.channel_id}</code>\n"
                    f"├ Постів: {stats.get('total_posts', 0)} "
                    f"(24h: {stats.get('posts_24h', 0)})\n"
                )

                if settings:
                    message += (
                        f"├ 👁 {format_number(settings.views_target)}"
                    )
                    if settings.views_target > 0:
                        message += " ✓"
                    message += "\n"

                    message += f"├ ❤️ {format_number(settings.reactions_target)}"
                    if settings.randomize_reactions:
                        message += f" (±{settings.randomize_percent}%)"
                    if settings.reactions_target > 0:
                        message += " ✓"
                    message += "\n"

                    message += f"├ 🔄 {format_number(settings.reposts_target)}"
                    if settings.randomize_reposts:
                        message += f" (±{settings.randomize_percent}%)"
                    if settings.reposts_target > 0:
                        message += " ✓"
                    message += "\n"
                else:
                    message += "├ ⚠️ Налаштування відсутні\n"

                # Add cache info
                if is_cached:
                    message += f"└ 💾 Cache: {cache_size} posts\n\n"
                else:
                    message += "└ 💾 Cache: empty\n\n"

                # Limit message size
                if len(message) > 3500:
                    message += f"<i>...та ще {len(channels) - i} каналів</i>"
                    break

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error("Failed to get channels", error=str(e), exc_info=True)
            await update.message.reply_text("❌ Помилка отримання каналів")

    async def cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command"""
        if not self._is_admin(update.effective_user.id):
            return

        self._admin_commands_count += 1

        try:
            channels = await self.queries.get_active_channels()

            message = "📈 <b>Статистика по каналах</b>\n\n"

            total_posts = 0
            total_completed = 0
            total_views = 0
            total_reactions = 0
            total_reposts = 0

            for channel in channels[:10]:  # Limit to 10
                stats = await self.queries.get_channel_stats(channel.id)

                total_posts += stats.get('total_posts', 0)
                total_completed += stats.get('completed_posts', 0)
                total_views += stats.get('total_views', 0)
                total_reactions += stats.get('total_reactions', 0)
                total_reposts += stats.get('total_reposts', 0)

                success_rate = (
                    stats['completed_posts'] / stats['total_posts'] * 100
                    if stats.get('total_posts', 0) > 0 else 0
                )

                message += (
                    f"📢 <b>{channel.channel_username}</b>\n"
                    f"├ Всього постів: {stats.get('total_posts', 0)}\n"
                    f"├ Завершено: {stats.get('completed_posts', 0)}\n"
                    f"├ В обробці: {stats.get('processing_posts', 0)}\n"
                    f"├ Помилки: {stats.get('failed_posts', 0)}\n"
                    f"├ Успішність: {success_rate:.1f}%\n"
                    f"└ За 7 днів: {stats.get('posts_7d', 0)} постів\n\n"
                )

            # Overall stats
            overall_success = (
                total_completed / total_posts * 100
                if total_posts > 0 else 0
            )

            message += (
                f"<b>Загальна статистика:</b>\n"
                f"├ Всього постів: {format_number(total_posts)}\n"
                f"├ Успішно оброблено: {format_number(total_completed)}\n"
                f"├ Загальна успішність: {overall_success:.1f}%\n"
                f"├ 👁 Переглядів: {format_number(total_views)}\n"
                f"├ ❤️ Реакцій: {format_number(total_reactions)}\n"
                f"└ 🔄 Репостів: {format_number(total_reposts)}"
            )

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error("Failed to get stats", error=str(e), exc_info=True)
            await update.message.reply_text("❌ Помилка отримання статистики")

    async def cmd_costs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /costs command"""
        if not self._is_admin(update.effective_user.id):
            return

        self._admin_commands_count += 1

        try:
            # Today's costs
            today_costs = await self.queries.get_today_costs()

            # This week's costs
            week_costs = await self.db.fetch(
                """
                SELECT 
                    DATE(o.created_at) as date,
                    o.service_type,
                    SUM(o.total_quantity * s.price_per_1000 / 1000) as cost
                FROM orders o
                JOIN services s ON s.nakrutka_id = o.service_id
                WHERE o.created_at > NOW() - INTERVAL '7 days'
                GROUP BY DATE(o.created_at), o.service_type
                ORDER BY date DESC, service_type
                """
            )

            # This month's costs
            month_total = await self.db.fetchval(
                """
                SELECT SUM(o.total_quantity * s.price_per_1000 / 1000)
                FROM orders o
                JOIN services s ON s.nakrutka_id = o.service_id
                WHERE DATE_TRUNC('month', o.created_at) = DATE_TRUNC('month', CURRENT_DATE)
                """
            ) or 0

            # Format message
            message = "💰 <b>Витрати на накрутку</b>\n\n"

            # Today
            today_total = sum(today_costs.values())
            message += (
                f"<b>Сьогодні:</b>\n"
                f"├ 👁 Перегляди: {format_price(today_costs.get('views', 0))}\n"
                f"├ ❤️ Реакції: {format_price(today_costs.get('reactions', 0))}\n"
                f"├ 🔄 Репости: {format_price(today_costs.get('reposts', 0))}\n"
                f"└ 💰 Всього: <b>{format_price(today_total)}</b>\n\n"
            )

            # Week by day
            message += "<b>За тиждень:</b>\n"

            daily_totals = {}
            for row in week_costs:
                date = row['date'].strftime('%d.%m')
                if date not in daily_totals:
                    daily_totals[date] = 0
                daily_totals[date] += float(row['cost'])

            days_shown = 0
            for date, total in list(daily_totals.items())[:7]:
                if days_shown < 6:
                    message += f"├ {date}: {format_price(total)}\n"
                else:
                    message += f"└ {date}: {format_price(total)}\n"
                days_shown += 1

            week_total = sum(daily_totals.values())
            message += f"\n<b>Всього за тиждень:</b> {format_price(week_total)}\n"
            message += f"<b>Всього за місяць:</b> {format_price(float(month_total))}\n\n"

            # Average per day
            if daily_totals:
                avg_daily = week_total / len(daily_totals)
                message += f"<i>Середньо за день: {format_price(avg_daily)}</i>"

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error("Failed to get costs", error=str(e), exc_info=True)
            await update.message.reply_text("❌ Помилка отримання витрат")

    async def cmd_balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /balance command"""
        if not self._is_admin(update.effective_user.id):
            return

        self._admin_commands_count += 1

        if not self.nakrutka_client:
            await update.message.reply_text("❌ Nakrutka client не налаштований")
            return

        try:
            # Get balance
            balance_info = await self.nakrutka_client.get_balance()
            balance = float(balance_info.get('balance', 0))
            currency = balance_info.get('currency', 'USD')

            # Get today's spending
            today_costs = await self.queries.get_today_costs()
            today_total = sum(today_costs.values())

            # Calculate days remaining at current rate
            days_remaining = "∞"
            if today_total > 0:
                days_remaining = f"{int(balance / today_total)}"

            # Get account info
            services = await self.nakrutka_client.get_services()
            service_count = len(services) if services else 0

            # Format message
            status_emoji = "✅" if balance >= 10 else "⚠️" if balance >= 1 else "❌"

            message = (
                f"💰 <b>Баланс Nakrutka</b> {status_emoji}\n\n"
                f"<b>Поточний баланс:</b> ${balance:.2f} {currency}\n"
                f"<b>Витрати сьогодні:</b> ${today_total:.2f}\n"
                f"<b>Днів залишилось:</b> {days_remaining}\n"
                f"<b>Доступно сервісів:</b> {service_count}\n\n"
            )

            # Add warnings
            if balance < 1:
                message += "❌ <b>КРИТИЧНО!</b> Баланс майже вичерпано!\n"
            elif balance < 10:
                message += "⚠️ <b>Увага!</b> Низький баланс, потрібно поповнити.\n"
            elif balance < today_total * 3:
                message += "💡 <i>Рекомендую поповнити баланс найближчим часом.</i>\n"

            # Add cache stats
            cache_stats = self.nakrutka_client.get_cache_stats()
            message += f"\n<i>Cache: {cache_stats['cache_size']} items</i>"

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error("Failed to get balance", error=str(e), exc_info=True)
            await update.message.reply_text(f"❌ Помилка: {str(e)[:100]}")

    async def cmd_orders(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /orders command"""
        if not self._is_admin(update.effective_user.id):
            return

        self._admin_commands_count += 1

        try:
            # Get active orders
            active_orders = await self.queries.get_active_orders()

            if not active_orders:
                await update.message.reply_text("📝 Немає активних замовлень")
                return

            message = f"📝 <b>Активні замовлення ({len(active_orders)})</b>\n\n"

            # Group by service type
            by_type = {
                'views': [],
                'reactions': [],
                'reposts': []
            }

            for order in active_orders:
                by_type.get(order.service_type, []).append(order)

            # Show orders by type
            for service_type, orders in by_type.items():
                if not orders:
                    continue

                emoji = {
                    'views': '👁',
                    'reactions': '❤️',
                    'reposts': '🔄'
                }.get(service_type, '❓')

                message += f"<b>{emoji} {service_type.capitalize()} ({len(orders)}):</b>\n"

                for order in orders[:5]:  # Limit to 5 per type
                    # Get post info
                    post = await self.queries.get_post_by_id(order.post_id)

                    # Format order info
                    age = (datetime.utcnow() - order.created_at).total_seconds() / 60
                    age_str = f"{int(age)}m ago"

                    status_emoji = "⏳" if order.status == "pending" else "▶️"

                    message += (
                        f"{status_emoji} {format_number(order.total_quantity)} "
                        f"• {age_str}"
                    )

                    if order.nakrutka_order_id:
                        message += f" • #{order.nakrutka_order_id[:8]}..."

                    message += "\n"

                if len(orders) > 5:
                    message += f"<i>...та ще {len(orders) - 5}</i>\n"

                message += "\n"

            # Add summary
            total_quantity = {
                'views': sum(o.total_quantity for o in by_type['views']),
                'reactions': sum(o.total_quantity for o in by_type['reactions']),
                'reposts': sum(o.total_quantity for o in by_type['reposts'])
            }

            message += (
                f"<b>Всього в обробці:</b>\n"
                f"👁 {format_number(total_quantity['views'])} | "
                f"❤️ {format_number(total_quantity['reactions'])} | "
                f"🔄 {format_number(total_quantity['reposts'])}"
            )

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error("Failed to get orders", error=str(e), exc_info=True)
            await update.message.reply_text("❌ Помилка отримання замовлень")

    async def cmd_errors(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /errors command"""
        if not self._is_admin(update.effective_user.id):
            return

        self._admin_commands_count += 1

        try:
            # Get recent error logs
            error_logs = await self.queries.get_recent_logs(level='error', limit=20)

            if not error_logs:
                await update.message.reply_text("✅ Немає помилок за останній час")
                return

            message = "❌ <b>Останні помилки системи</b>\n\n"

            # Group errors by type
            error_types = {}
            for log in error_logs:
                error_type = log.context.get('error_type', 'Unknown')
                if error_type not in error_types:
                    error_types[error_type] = []
                error_types[error_type].append(log)

            # Show errors by type
            for error_type, errors in list(error_types.items())[:5]:
                message += f"<b>{error_type} ({len(errors)}):</b>\n"

                for error in errors[:3]:
                    time_ago = datetime.utcnow() - error.created_at
                    time_str = format_duration(int(time_ago.total_seconds()))

                    error_msg = truncate_text(error.message, 50)
                    message += f"• {error_msg} ({time_str} ago)\n"

                message += "\n"

            # Channel errors
            if self._channel_errors:
                message += f"<b>Канали з помилками ({len(self._channel_errors)}):</b>\n"
                for channel_id, errors in list(self._channel_errors.items())[:5]:
                    channel = await self.queries.get_channel_by_id(channel_id)
                    if channel:
                        message += f"• {channel.channel_username}: {len(errors)} errors\n"

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error("Failed to get errors", error=str(e), exc_info=True)
            await update.message.reply_text("❌ Помилка отримання логів")

    async def cmd_cache(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /cache command"""
        if not self._is_admin(update.effective_user.id):
            return

        self._admin_commands_count += 1

        try:
            # Channel cache stats
            total_cached_posts = sum(len(posts) for posts in self._channel_cache.values())

            message = "💾 <b>Статистика кешу</b>\n\n"

            message += f"<b>Канали в кеші:</b> {len(self._channel_cache)}\n"
            message += f"<b>Всього постів в кеші:</b> {format_number(total_cached_posts)}\n\n"

            # Show cache details for each channel
            if self._channel_cache:
                message += "<b>Деталі по каналах:</b>\n"

                for channel_id, posts in list(self._channel_cache.items())[:10]:
                    channel = await self.queries.get_channel_by_id(channel_id)
                    if channel:
                        cache_age = datetime.utcnow() - self._cache_updated.get(channel_id, datetime.min)
                        age_str = format_duration(int(cache_age.total_seconds()))

                        message += (
                            f"• {channel.channel_username}: "
                            f"{len(posts)} posts, age: {age_str}\n"
                        )

            # Nakrutka cache stats
            if self.nakrutka_client:
                nakrutka_stats = self.nakrutka_client.get_cache_stats()
                message += (
                    f"\n<b>Nakrutka cache:</b>\n"
                    f"├ Items: {nakrutka_stats['cache_size']}\n"
                    f"├ Service limits: {nakrutka_stats['service_limits_cache_size']}\n"
                    f"└ Keys: {', '.join(nakrutka_stats['cache_keys'])}\n"
                )

            # Admin commands count
            message += f"\n<i>Admin commands used: {self._admin_commands_count}</i>"

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error("Failed to get cache stats", error=str(e), exc_info=True)
            await update.message.reply_text("❌ Помилка отримання статистики кешу")

    async def cmd_health(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /health command"""
        if not self._is_admin(update.effective_user.id):
            return

        self._admin_commands_count += 1

        msg = await update.message.reply_text("🔍 Перевіряю систему...")

        try:
            health_status = {
                'database': '❓',
                'nakrutka': '❓',
                'telegram': '❓',
                'scheduler': '❓'
            }

            # Check database
            try:
                db_test = await self.db.fetchval("SELECT 1")
                health_status['database'] = '✅' if db_test == 1 else '❌'
            except:
                health_status['database'] = '❌'

            # Check Nakrutka
            if self.nakrutka_client:
                try:
                    health = await self.nakrutka_client.health_check()
                    if health['status'] == 'healthy':
                        health_status['nakrutka'] = '✅'
                    elif health['status'] == 'auth_error':
                        health_status['nakrutka'] = '🔑'
                    else:
                        health_status['nakrutka'] = '❌'
                except:
                    health_status['nakrutka'] = '❌'
            else:
                health_status['nakrutka'] = '⚠️'

            # Check Telegram bot
            try:
                bot_info = await self.bot.get_me()
                health_status['telegram'] = '✅' if bot_info else '❌'
            except:
                health_status['telegram'] = '❌'

            # Check scheduler (assumed OK if we're running)
            health_status['scheduler'] = '✅'

            # Format message
            message = "🏥 <b>Перевірка здоров'я системи</b>\n\n"

            for component, status in health_status.items():
                message += f"{status} <b>{component.capitalize()}</b>\n"

            # Add details if issues found
            if '❌' in health_status.values() or '⚠️' in health_status.values():
                message += "\n<b>Деталі проблем:</b>\n"

                if health_status['database'] == '❌':
                    message += "• База даних не відповідає\n"

                if health_status['nakrutka'] == '❌':
                    message += "• Nakrutka API недоступний\n"
                elif health_status['nakrutka'] == '🔑':
                    message += "• Проблема з API ключем Nakrutka\n"
                elif health_status['nakrutka'] == '⚠️':
                    message += "• Nakrutka client не налаштований\n"

                if health_status['telegram'] == '❌':
                    message += "• Telegram Bot API недоступний\n"

            # Overall status
            all_ok = all(s == '✅' for s in health_status.values())
            if all_ok:
                message += "\n✅ <b>Всі системи працюють нормально!</b>"
            else:
                message += "\n⚠️ <b>Виявлено проблеми, перевірте логи.</b>"

            await msg.edit_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error("Failed to check health", error=str(e), exc_info=True)
            await msg.edit_text("❌ Помилка перевірки здоров'я системи")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        if not self._is_admin(update.effective_user.id):
            return

        self._admin_commands_count += 1

        help_message = (
            "ℹ️ <b>Довідка по командах</b>\n\n"
            "<b>Основні команди:</b>\n"
            "/start - Початок роботи\n"
            "/status - Поточний статус системи\n"
            "/channels - Список активних каналів\n"
            "/stats - Детальна статистика\n"
            "/costs - Витрати на накрутку\n\n"
            "<b>Додаткові команди:</b>\n"
            "/balance - Баланс Nakrutka\n"
            "/orders - Активні замовлення\n"
            "/errors - Останні помилки\n"
            "/cache - Статистика кешу\n"
            "/health - Перевірка здоров'я системи\n"
            "/help - Ця довідка\n\n"
            "<b>Як працює бот:</b>\n"
            "1. Моніторить канали кожні 30 секунд\n"
            "2. Знаходить нові пости\n"
            "3. Створює замовлення на накрутку\n"
            "4. Розподіляє по порціях (drip-feed)\n"
            "5. Відслідковує виконання\n\n"
            "<b>Налаштування:</b>\n"
            "• Перегляди - без рандомізації\n"
            "• Реакції - ±40% рандомізація\n"
            "• Репости - ±40% рандомізація\n\n"
            "<b>Розподіл навантаження:</b>\n"
            "• 70% за перші 3-5 годин\n"
            "• 30% протягом наступних 19 годин\n\n"
            "<i>Бот працює повністю автоматично 24/7</i>"
        )

        await update.message.reply_text(help_message, parse_mode=ParseMode.HTML)

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle non-command messages"""
        if not self._is_admin(update.effective_user.id):
            return

        # Could add functionality for adding channels by forwarding messages
        # or other interactive features
        await update.message.reply_text(
            "💡 Використовуйте команди для управління ботом.\n"
            "Наберіть /help для списку команд."
        )

    def _is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        return settings.admin_telegram_id and user_id == settings.admin_telegram_id

    def get_monitor_stats(self) -> Dict[str, Any]:
        """Get monitoring statistics"""
        return {
            'channels_monitored': len(self._channel_cache),
            'total_cached_posts': sum(len(posts) for posts in self._channel_cache.values()),
            'channels_with_errors': len(self._channel_errors),
            'admin_commands_count': self._admin_commands_count,
            'uptime_seconds': (datetime.utcnow() - self.start_time).total_seconds()
        }