"""
Telegram channel monitoring service - Complete implementation
"""
from typing import List, Optional, Dict, Any, Set
from datetime import datetime, timedelta
import asyncio
import re

from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError, BadRequest, Forbidden
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.errors import SessionPasswordNeededError

from src.config import settings
from src.database.connection import DatabaseConnection
from src.database.queries import Queries
from src.database.models import Channel, Post
from src.utils.logger import get_logger, DatabaseLogger
from src.utils.helpers import validate_telegram_channel_id, format_number, format_price

logger = get_logger(__name__)


class TelegramMonitor:
    """Monitors Telegram channels for new posts"""

    def __init__(self, db: DatabaseConnection):
        self.db = db
        self.queries = Queries(db)
        self.db_logger = DatabaseLogger(db)
        self.bot = Bot(token=settings.telegram_bot_token)
        self.app: Optional[Application] = None

        # Cache for channel posts to avoid duplicates
        self._channel_cache: Dict[int, Set[int]] = {}
        self._cache_ttl = 300  # 5 minutes
        self._cache_updated: Dict[int, datetime] = {}

        # Rate limiting
        self._last_check: Dict[int, datetime] = {}
        self._min_check_interval = 20  # seconds between channel checks

    async def setup_bot(self):
        """Setup telegram bot handlers"""
        self.app = Application.builder().token(settings.telegram_bot_token).build()

        # Add command handlers
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("status", self.cmd_status))
        self.app.add_handler(CommandHandler("stats", self.cmd_stats))
        self.app.add_handler(CommandHandler("channels", self.cmd_channels))
        self.app.add_handler(CommandHandler("costs", self.cmd_costs))
        self.app.add_handler(CommandHandler("help", self.cmd_help))

        logger.info("Telegram bot handlers configured")

    async def start_bot(self):
        """Start telegram bot (for handling commands)"""
        if self.app:
            await self.app.initialize()
            await self.app.start()

            # Start polling in background
            asyncio.create_task(self.app.updater.start_polling(
                allowed_updates=["message", "callback_query"]
            ))

            logger.info("Telegram bot started for commands")

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

                    # Small delay between channels
                    await asyncio.sleep(1)

                except Exception as e:
                    logger.error(
                        f"Error checking channel {channel.channel_username}",
                        error=str(e),
                        channel_id=channel.id
                    )
                    results.append(None)

            # Count results
            new_posts = sum(r or 0 for r in results if isinstance(r, int))
            errors = sum(1 for r in results if r is None)

            if new_posts > 0:
                logger.info(f"Found {new_posts} new posts across all channels")
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
                error=str(e)
            )

    async def check_channel(self, channel: Channel) -> Optional[int]:
        """Check single channel for new posts"""
        try:
            logger.debug(f"Checking channel {channel.channel_username}")

            # Get recent posts from channel
            recent_posts = await self.get_channel_posts(
                channel.channel_username,
                channel.channel_id,
                limit=30  # Check last 30 posts
            )

            if not recent_posts:
                logger.debug(f"No posts found in {channel.channel_username}")
                return 0

            # Get existing posts from DB (last 500 to be safe)
            existing_posts = await self.queries.get_channel_posts(
                channel.id,
                limit=500
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
                return cached_posts[-limit:]

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

                # Clean old posts from cache (keep last 1000)
                if len(self._channel_cache[cache_key]) > 1000:
                    sorted_posts = sorted(self._channel_cache[cache_key])
                    self._channel_cache[cache_key] = set(sorted_posts[-1000:])

            logger.debug(f"Found {len(posts)} posts in {username}")
            return posts[-limit:]

        except Exception as e:
            logger.error(
                f"Failed to get posts from {username}",
                error=str(e),
                channel_id=channel_id
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
            except (BadRequest, Forbidden) as e:
                logger.debug(f"Cannot access @{username}: {e}")
                return []

            # Parse from web as fallback
            return await self._get_channel_posts_from_web(username, limit)

        except Exception as e:
            logger.error(f"Error getting public channel posts: {e}")
            return []

    async def _get_private_channel_posts(self, channel_id: int, limit: int) -> List[int]:
        """Get posts from private channel where bot is admin"""
        try:
            # This only works if bot is admin in channel
            # Try to get recent messages
            updates = await self.bot.get_updates(
                offset=-1,
                limit=100,
                allowed_updates=["channel_post"]
            )

            posts = []
            for update in updates:
                if update.channel_post and update.channel_post.chat.id == channel_id:
                    posts.append(update.channel_post.message_id)

            return posts[-limit:]

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
                async with session.get(url, timeout=10) as response:
                    if response.status != 200:
                        logger.warning(f"Failed to fetch {url}: {response.status}")
                        return []

                    html = await response.text()

            # Parse HTML
            soup = BeautifulSoup(html, 'html.parser')

            # Find message links
            posts = []
            for link in soup.find_all('a', href=re.compile(rf't\.me/{username}/\d+')):
                match = re.search(rf't\.me/{username}/(\d+)', link.get('href', ''))
                if match:
                    post_id = int(match.group(1))
                    posts.append(post_id)

            # Remove duplicates and sort
            posts = sorted(list(set(posts)))

            logger.debug(f"Parsed {len(posts)} posts from web for {username}")
            return posts[-limit:]

        except Exception as e:
            logger.error(f"Failed to parse web posts: {e}")
            return []

    async def get_post_info(self, channel_id: int, post_id: int) -> Dict[str, Any]:
        """Get detailed post information"""
        try:
            # Try to get message info if bot has access
            # This is limited and may not work for all channels

            try:
                # Try forwarding to get info (then delete)
                # This is a workaround but not recommended for production
                pass
            except:
                pass

            # Return basic info
            return {
                'post_id': post_id,
                'channel_id': channel_id,
                'timestamp': datetime.utcnow()
            }

        except Exception as e:
            logger.error("Failed to get post info", error=str(e))
            return {}

    # ============ BOT COMMANDS ============

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        welcome_message = (
            "🤖 *Telegram SMM Bot*\n\n"
            "Автоматична система накрутки для Telegram каналів\n\n"
            "*Команди:*\n"
            "/status - Статус системи\n"
            "/channels - Список каналів\n"
            "/stats - Статистика по каналах\n"
            "/costs - Витрати за сьогодні\n"
            "/help - Допомога\n\n"
            "_Бот працює 24/7 в автоматичному режимі_"
        )

        await update.message.reply_text(
            welcome_message,
            parse_mode='Markdown'
        )

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command"""
        if not self._is_admin(update.effective_user.id):
            return

        try:
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

            # Get Nakrutka balance
            try:
                balance = await self.nakrutka.get_balance()
                balance_text = f"${balance.get('balance', 0)} {balance.get('currency', 'USD')}"
            except:
                balance_text = "N/A"

            message = (
                "📊 *Статус системи*\n\n"
                f"✅ Активних каналів: {len(channels)}\n"
                f"📝 Активних замовлень: {len(active_orders)}\n\n"
                f"*Пости за 24 години:*\n"
                f"🆕 Нові: {post_stats.get('new', 0)}\n"
                f"⏳ В обробці: {post_stats.get('processing', 0)}\n"
                f"✅ Завершені: {post_stats.get('completed', 0)}\n"
                f"❌ Помилки: {post_stats.get('failed', 0)}\n\n"
                f"*Витрати сьогодні:*\n"
                f"👁 Перегляди: ${today_costs.get('views', 0):.2f}\n"
                f"❤️ Реакції: ${today_costs.get('reactions', 0):.2f}\n"
                f"🔄 Репости: ${today_costs.get('reposts', 0):.2f}\n"
                f"💰 Всього: ${total_cost:.2f}\n\n"
                f"*Баланс Nakrutka:* {balance_text}"
            )

            await update.message.reply_text(message, parse_mode='Markdown')

        except Exception as e:
            logger.error("Failed to get status", error=str(e))
            await update.message.reply_text("❌ Помилка отримання статусу")

    async def cmd_channels(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /channels command"""
        if not self._is_admin(update.effective_user.id):
            return

        try:
            channels = await self.queries.get_active_channels()

            if not channels:
                await update.message.reply_text("📢 Немає активних каналів")
                return

            message = "📢 *Активні канали:*\n\n"

            for channel in channels:
                # Get settings
                settings = await self.queries.get_channel_settings(channel.id)

                # Get post count
                post_count = await self.db.fetchval(
                    "SELECT COUNT(*) FROM posts WHERE channel_id = $1",
                    channel.id
                )

                message += (
                    f"*{channel.channel_username}*\n"
                    f"├ ID: `{channel.channel_id}`\n"
                    f"├ Постів: {post_count}\n"
                )

                if settings:
                    message += (
                        f"├ Перегляди: {format_number(settings.views_target)}\n"
                        f"├ Реакції: {format_number(settings.reactions_target)}"
                    )
                    if settings.randomize_reactions:
                        message += f" (±{settings.randomize_percent}%)"
                    message += "\n"

                    message += f"└ Репости: {format_number(settings.reposts_target)}"
                    if settings.randomize_reposts:
                        message += f" (±{settings.randomize_percent}%)"
                    message += "\n\n"
                else:
                    message += "└ ⚠️ Налаштування відсутні\n\n"

            await update.message.reply_text(message, parse_mode='Markdown')

        except Exception as e:
            logger.error("Failed to get channels", error=str(e))
            await update.message.reply_text("❌ Помилка отримання каналів")

    async def cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command"""
        if not self._is_admin(update.effective_user.id):
            return

        try:
            channels = await self.queries.get_active_channels()

            message = "📈 *Статистика по каналах*\n\n"

            total_posts = 0
            total_completed = 0

            for channel in channels[:10]:  # Limit to 10
                stats = await self.queries.get_channel_stats(channel.id)

                total_posts += stats['total_posts']
                total_completed += stats['completed_posts']

                success_rate = (
                    stats['completed_posts'] / stats['total_posts'] * 100
                    if stats['total_posts'] > 0 else 0
                )

                message += (
                    f"📢 *{channel.channel_username}*\n"
                    f"├ Всього постів: {stats['total_posts']}\n"
                    f"├ Завершено: {stats['completed_posts']}\n"
                    f"├ В обробці: {stats['processing_posts']}\n"
                    f"├ Помилки: {stats['failed_posts']}\n"
                    f"└ Успішність: {success_rate:.1f}%\n\n"
                )

            # Overall stats
            overall_success = (
                total_completed / total_posts * 100
                if total_posts > 0 else 0
            )

            message += (
                f"*Загальна статистика:*\n"
                f"├ Всього постів: {total_posts}\n"
                f"├ Успішно оброблено: {total_completed}\n"
                f"└ Загальна успішність: {overall_success:.1f}%"
            )

            await update.message.reply_text(message, parse_mode='Markdown')

        except Exception as e:
            logger.error("Failed to get stats", error=str(e))
            await update.message.reply_text("❌ Помилка отримання статистики")

    async def cmd_costs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /costs command"""
        if not self._is_admin(update.effective_user.id):
            return

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

            # Format message
            message = "💰 *Витрати на накрутку*\n\n"

            # Today
            today_total = sum(today_costs.values())
            message += (
                f"*Сьогодні:*\n"
                f"├ Перегляди: {format_price(today_costs.get('views', 0))}\n"
                f"├ Реакції: {format_price(today_costs.get('reactions', 0))}\n"
                f"├ Репости: {format_price(today_costs.get('reposts', 0))}\n"
                f"└ Всього: {format_price(today_total)}\n\n"
            )

            # Week by day
            message += "*За тиждень:*\n"

            daily_totals = {}
            for row in week_costs:
                date = row['date'].strftime('%d.%m')
                if date not in daily_totals:
                    daily_totals[date] = 0
                daily_totals[date] += float(row['cost'])

            for date, total in list(daily_totals.items())[:7]:
                message += f"├ {date}: {format_price(total)}\n"

            week_total = sum(daily_totals.values())
            message += f"└ Всього за тиждень: {format_price(week_total)}"

            await update.message.reply_text(message, parse_mode='Markdown')

        except Exception as e:
            logger.error("Failed to get costs", error=str(e))
            await update.message.reply_text("❌ Помилка отримання витрат")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        if not self._is_admin(update.effective_user.id):
            return

        help_message = (
            "ℹ️ *Допомога по боту*\n\n"
            "*Основні команди:*\n"
            "/start - Початок роботи\n"
            "/status - Поточний статус системи\n"
            "/channels - Список активних каналів\n"
            "/stats - Детальна статистика\n"
            "/costs - Витрати на накрутку\n"
            "/help - Ця допомога\n\n"
            "*Як працює бот:*\n"
            "1. Моніторить канали кожні 30 секунд\n"
            "2. Знаходить нові пости\n"
            "3. Створює замовлення на накрутку\n"
            "4. Розподіляє по порціях (drip-feed)\n"
            "5. Відслідковує виконання\n\n"
            "*Налаштування:*\n"
            "• Перегляди - без рандомізації\n"
            "• Реакції - ±40% рандомізація\n"
            "• Репости - ±40% рандомізація\n\n"
            "_Бот працює повністю автоматично_"
        )

        await update.message.reply_text(help_message, parse_mode='Markdown')

    def _is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        return settings.admin_telegram_id and user_id == settings.admin_telegram_id