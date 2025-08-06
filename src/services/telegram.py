"""
Telegram channel monitoring service with Telethon + Bot API integration
"""
from typing import List, Optional, Dict, Any, Set, Union
from datetime import datetime, timedelta
import asyncio
import re
import html
import os
from pathlib import Path

from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from telegram.error import TelegramError, BadRequest, Forbidden
from telegram.constants import ParseMode

# Telethon imports
from telethon import TelegramClient, events
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import Channel, InputChannel, Message
from telethon.errors import (
    SessionPasswordNeededError,
    ChannelPrivateError,
    ChannelInvalidError,
    FloodWaitError,
    AuthKeyError
)
from telethon.sessions import StringSession

from src.config import settings
from src.database.connection import DatabaseConnection
from src.database.queries import Queries
from src.database.models import Channel as DBChannel, Post
from src.services.nakrutka import NakrutkaClient
from src.utils.logger import get_logger, DatabaseLogger
from src.utils.helpers import (
    validate_telegram_channel_id,
    format_number,
    format_price,
    truncate_text,
    format_duration,
    async_retry
)

logger = get_logger(__name__)


class EnhancedTelegramMonitor:
    """Enhanced Telegram monitor with Telethon integration for better channel access"""

    def __init__(self, db: DatabaseConnection, nakrutka_client: Optional[NakrutkaClient] = None):
        self.db = db
        self.queries = Queries(db)
        self.db_logger = DatabaseLogger(db)
        self.nakrutka_client = nakrutka_client

        # Bot API
        self.bot = Bot(token=settings.telegram_bot_token)
        self.app: Optional[Application] = None

        # Telethon client
        self.telethon_client: Optional[TelegramClient] = None
        self.telethon_initialized = False
        self.telethon_me = None  # Current user info

        # Session management
        self.session_string: Optional[str] = None
        self.session_file = "telegram_monitor.session"

        # Start time for uptime calculation
        self.start_time = datetime.utcnow()

        # Enhanced cache with TTL
        self._channel_cache: Dict[int, Dict[str, Any]] = {}
        self._cache_ttl = {
            'posts': 300,      # 5 minutes for posts
            'channel_info': 3600,  # 1 hour for channel info
            'access': 7200     # 2 hours for access info
        }
        self._cache_timestamps: Dict[str, Dict[int, datetime]] = {
            'posts': {},
            'channel_info': {},
            'access': {}
        }

        # Channel access tracking
        self._channel_access: Dict[int, str] = {}  # channel_id -> access_type
        self._access_methods = ['telethon', 'bot_api', 'web_parse']

        # Rate limiting
        self._last_check: Dict[int, datetime] = {}
        self._min_check_interval = 20  # seconds between channel checks
        self._telethon_rate_limit = 3   # seconds between Telethon requests
        self._last_telethon_request = datetime.min

        # Error tracking
        self._channel_errors: Dict[int, List[Dict[str, Any]]] = {}
        self._max_errors_per_channel = 5
        self._flood_wait_until: Dict[int, datetime] = {}  # channel_id -> wait_until

        # Statistics
        self._stats = {
            'telethon_success': 0,
            'telethon_fail': 0,
            'bot_api_success': 0,
            'bot_api_fail': 0,
            'web_parse_success': 0,
            'web_parse_fail': 0,
            'total_posts_found': 0
        }

        # Admin commands tracking
        self._admin_commands_count = 0

    async def initialize_telethon(self):
        """Initialize Telethon client with proper session management"""
        try:
            # Check if we have API credentials
            if not hasattr(settings, 'telethon_api_id') or not settings.telethon_api_id:
                logger.warning("Telethon API credentials not configured")
                return False

            # Try to load existing session
            session = await self._load_or_create_session()

            # Create client
            self.telethon_client = TelegramClient(
                session,
                settings.telethon_api_id,
                settings.telethon_api_hash,
                flood_sleep_threshold=60,
                device_model="SMM Bot Monitor",
                app_version="1.0.0",
                lang_code="en"
            )

            # Connect
            await self.telethon_client.connect()

            # Check if authorized
            if not await self.telethon_client.is_user_authorized():
                logger.warning("Telethon not authorized, need to login")

                # Try phone auth if configured
                if hasattr(settings, 'telethon_phone') and settings.telethon_phone:
                    await self._authenticate_telethon()
                else:
                    logger.error("Telethon phone number not configured for authentication")
                    return False

            # Get current user info
            self.telethon_me = await self.telethon_client.get_me()
            logger.info(
                "Telethon initialized successfully",
                user_id=self.telethon_me.id,
                username=self.telethon_me.username,
                phone=self.telethon_me.phone
            )

            # Save session for future use
            await self._save_session()

            self.telethon_initialized = True

            await self.db_logger.info(
                "Telethon client initialized",
                user_id=self.telethon_me.id,
                username=self.telethon_me.username
            )

            return True

        except Exception as e:
            logger.error("Failed to initialize Telethon", error=str(e), exc_info=True)
            self.telethon_initialized = False
            return False

    async def _load_or_create_session(self) -> Union[str, StringSession]:
        """Load existing session or create new one"""
        try:
            # Try to load from database first
            session_data = await self.db.fetchval(
                """
                SELECT api_key FROM api_keys 
                WHERE service_name = 'telethon_session' AND is_active = true
                LIMIT 1
                """
            )

            if session_data:
                logger.info("Loaded Telethon session from database")
                return StringSession(session_data)

            # Try file session
            if os.path.exists(self.session_file):
                logger.info("Using file session")
                return self.session_file

            # Create new string session
            logger.info("Creating new Telethon session")
            return StringSession()

        except Exception as e:
            logger.error(f"Failed to load session: {e}")
            return StringSession()

    async def _save_session(self):
        """Save session to database"""
        try:
            if not self.telethon_client:
                return

            # Get string session
            session_string = StringSession.save(self.telethon_client.session)

            # Save to database
            await self.db.execute(
                """
                INSERT INTO api_keys (service_name, api_key, is_active)
                VALUES ('telethon_session', $1, true)
                ON CONFLICT (service_name) 
                DO UPDATE SET api_key = $1, created_at = CURRENT_TIMESTAMP
                """,
                session_string
            )

            logger.info("Telethon session saved to database")

        except Exception as e:
            logger.error(f"Failed to save session: {e}")

    async def _authenticate_telethon(self):
        """Authenticate Telethon client"""
        try:
            phone = settings.telethon_phone

            # Send code
            await self.telethon_client.send_code_request(phone)

            # In production, this would need to be handled via admin command
            # For now, log that manual intervention is needed
            logger.warning(
                "Telethon authentication required",
                phone=phone,
                message="Use /telethon_auth <code> command to complete"
            )

            # Store state for later completion
            self._pending_auth = {
                'phone': phone,
                'timestamp': datetime.utcnow()
            }

        except FloodWaitError as e:
            logger.error(f"Flood wait error during auth: wait {e.seconds}s")
            raise
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            raise

    async def setup_bot(self):
        """Setup telegram bot handlers with Telethon support"""
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

        # Channel management commands
        self.app.add_handler(CommandHandler("add_channel", self.cmd_add_channel))
        self.app.add_handler(CommandHandler("remove_channel", self.cmd_remove_channel))
        self.app.add_handler(CommandHandler("channel_info", self.cmd_channel_info))
        self.app.add_handler(CommandHandler("stop_order", self.cmd_stop_order))
        self.app.add_handler(CommandHandler("set_old_posts", self.cmd_set_old_posts))

        # Telethon specific commands
        self.app.add_handler(CommandHandler("telethon_status", self.cmd_telethon_status))
        self.app.add_handler(CommandHandler("telethon_auth", self.cmd_telethon_auth))
        self.app.add_handler(CommandHandler("check_access", self.cmd_check_access))

        # Add message handler for non-command messages
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

        # Error handler
        self.app.add_error_handler(self.error_handler)

        logger.info("Telegram bot handlers configured")

    async def start_bot(self):
        """Start telegram bot and initialize Telethon"""
        # Start Bot API
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

        # Initialize Telethon
        telethon_ok = await self.initialize_telethon()
        if telethon_ok:
            logger.info("Telethon integration ready")
        else:
            logger.warning("Running without Telethon - limited to public channels")

        await self.db_logger.info(
            "Telegram monitor started",
            bot_api=True,
            telethon=telethon_ok
        )

    async def stop_bot(self):
        """Stop telegram bot and Telethon client"""
        # Stop Bot API
        if self.app:
            await self.app.updater.stop()
            await self.app.stop()
            logger.info("Telegram bot stopped")

        # Stop Telethon
        if self.telethon_client and self.telethon_client.is_connected():
            await self.telethon_client.disconnect()
            logger.info("Telethon client disconnected")

    async def check_channels(self):
        """Check all active channels for new posts using best available method"""
        try:
            channels = await self.queries.get_active_channels()

            if not channels:
                logger.debug("No active channels to monitor")
                return

            logger.info(f"Checking {len(channels)} active channels for new posts")

            # Process channels
            results = []
            for channel in channels:
                # Check if channel has too many errors
                if self._should_skip_channel(channel):
                    continue

                # Check rate limit
                if not self._can_check_channel(channel.id):
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
                    self._track_channel_error(channel.id, str(e), type(e).__name__)
                    results.append(None)

            # Log results
            new_posts = sum(r or 0 for r in results if isinstance(r, int))
            errors = sum(1 for r in results if r is None)
            skipped = len(channels) - len(results)

            if new_posts > 0 or errors > 0:
                logger.info(
                    "Channel check completed",
                    new_posts=new_posts,
                    channels_checked=len(results),
                    errors=errors,
                    skipped=skipped,
                    stats=self._stats
                )

                await self.db_logger.info(
                    "Channel check summary",
                    new_posts=new_posts,
                    checked=len(results),
                    errors=errors,
                    method_stats=self._stats
                )

            self._stats['total_posts_found'] += new_posts

        except Exception as e:
            logger.error("Failed to check channels", error=str(e), exc_info=True)
            await self.db_logger.error(
                "Channel check failed",
                error=str(e),
                error_type=type(e).__name__
            )

    async def check_channel(self, channel: DBChannel) -> Optional[int]:
        """Check single channel for new posts using best available method"""
        try:
            logger.debug(
                f"Checking channel {channel.channel_username}",
                channel_id=channel.channel_id,
                access_type=self._channel_access.get(channel.id, 'unknown')
            )

            # Отримуємо налаштування каналу
            settings = await self.db.fetchrow(
                "SELECT * FROM channel_settings WHERE channel_id = $1",
                channel.id
            )

            if not settings:
                logger.warning(f"No settings found for channel {channel.channel_username}")
                return 0

            # Перевіряємо чи це перший запуск для каналу
            existing_posts_count = await self.db.fetchval(
                "SELECT COUNT(*) FROM posts WHERE channel_id = $1",
                channel.id
            )

            is_first_run = existing_posts_count == 0

            # Визначаємо скільки постів отримати
            if is_first_run:
                # При першому запуску обмежуємо кількість
                posts_to_fetch = settings['process_old_posts_count'] if settings['process_old_posts_count'] > 0 else 1
                logger.info(
                    f"First run for {channel.channel_username}",
                    process_old_posts=settings['process_old_posts_count'],
                    will_fetch=posts_to_fetch
                )
            else:
                # Звичайна перевірка
                posts_to_fetch = 50

            # Отримуємо пости
            recent_posts = await self.get_channel_posts_cascade(
                channel.channel_username,
                channel.channel_id,
                limit=posts_to_fetch
            )

            if not recent_posts:
                logger.debug(f"No posts found in {channel.channel_username}")
                return 0

            # Отримуємо існуючі пости з БД
            existing_posts = await self.queries.get_channel_posts(
                channel.id,
                limit=1000
            )
            existing_set = set(existing_posts)

            # Фільтруємо нові пости
            new_posts = []

            if is_first_run and settings['process_old_posts_count'] == 0:
                # Не обробляємо старі пости при першому запуску
                logger.info(f"First run with process_old_posts_count=0, skipping all old posts")
                return 0
            else:
                # Звичайна фільтрація - всі пости яких немає в БД
                new_posts = [
                    post_id for post_id in recent_posts
                    if post_id not in existing_set
                ]

            if not new_posts:
                logger.debug(f"No new posts in {channel.channel_username}")
                return 0

            # При першому запуску обмежуємо кількість
            if is_first_run and settings['process_old_posts_count'] > 0:
                # Беремо тільки вказану кількість останніх
                new_posts = new_posts[-settings['process_old_posts_count']:]
                logger.info(
                    f"First run: limiting to {len(new_posts)} most recent posts",
                    channel=channel.channel_username,
                    requested=settings['process_old_posts_count']
                )

            logger.info(
                f"Found {len(new_posts)} new posts in {channel.channel_username}",
                post_ids=new_posts[:5],
                is_first_run=is_first_run,
                access_method=self._channel_access.get(channel.id, 'unknown')
            )

            # Зберігаємо нові пости в БД
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
                    logger.debug(f"Created post record {result} for {post_url}")

            if created_count > 0:
                await self.db_logger.info(
                    "New posts saved",
                    channel=channel.channel_username,
                    count=created_count,
                    is_first_run=is_first_run,
                    process_old_posts_count=settings['process_old_posts_count'] if is_first_run else None,
                    access_method=self._channel_access.get(channel.id, 'unknown'),
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
            return None

    async def get_channel_posts_cascade(
        self,
        username: str,
        channel_id: int,
        limit: int = 20
    ) -> List[int]:
        """Get posts using cascade of methods: Telethon -> Bot API -> Web Parse"""

        # Clean username
        username = username.lstrip('@')

        # Check cache first
        cached_posts = self._get_cached_posts(channel_id)
        if cached_posts is not None:
            return cached_posts[:limit]

        posts = []
        access_method = None

        # Method 1: Try Telethon (best method)
        if self.telethon_initialized and not self._is_flood_limited(channel_id):
            try:
                posts = await self._get_posts_via_telethon(username, channel_id, limit)
                if posts:
                    access_method = 'telethon'
                    self._stats['telethon_success'] += 1
                    logger.debug(f"Got {len(posts)} posts via Telethon for {username}")
            except FloodWaitError as e:
                self._handle_flood_wait(channel_id, e.seconds)
                self._stats['telethon_fail'] += 1
            except (ChannelPrivateError, ChannelInvalidError) as e:
                logger.debug(f"No Telethon access to {username}: {e}")
                self._stats['telethon_fail'] += 1
            except Exception as e:
                logger.error(f"Telethon error for {username}: {e}")
                self._stats['telethon_fail'] += 1

        # Method 2: Try Bot API (if bot is admin)
        if not posts:
            try:
                posts = await self._get_posts_via_bot_api(channel_id, limit)
                if posts:
                    access_method = 'bot_api'
                    self._stats['bot_api_success'] += 1
                    logger.debug(f"Got {len(posts)} posts via Bot API for {username}")
            except Exception as e:
                logger.debug(f"Bot API failed for {username}: {e}")
                self._stats['bot_api_fail'] += 1

        # Method 3: Web parsing (fallback)
        if not posts:
            try:
                posts = await self._get_posts_via_web_parse(username, limit)
                if posts:
                    access_method = 'web_parse'
                    self._stats['web_parse_success'] += 1
                    logger.debug(f"Got {len(posts)} posts via web parse for {username}")
            except Exception as e:
                logger.error(f"Web parse failed for {username}: {e}")
                self._stats['web_parse_fail'] += 1

        # Update cache and access info
        if posts:
            self._cache_posts(channel_id, posts)
            self._channel_access[channel_id] = access_method

        return sorted(posts)[-limit:] if posts else []

    @async_retry(max_attempts=2, exceptions=(FloodWaitError,))
    async def _get_posts_via_telethon(
        self,
        username: str,
        channel_id: int,
        limit: int
    ) -> List[int]:
        """Get posts using Telethon client"""
        if not self.telethon_client or not self.telethon_initialized:
            return []

        # Rate limit
        await self._telethon_rate_limit_check()

        try:
            # Get channel entity
            try:
                # Try by username first
                entity = await self.telethon_client.get_entity(username)
            except Exception:
                # Try by ID
                try:
                    entity = await self.telethon_client.get_entity(channel_id)
                except Exception:
                    # Try as input peer
                    entity = await self.telethon_client.get_input_entity(channel_id)

            # Get messages
            messages = await self.telethon_client.get_messages(
                entity,
                limit=limit * 2  # Get more to filter channel posts only
            )

            # Extract post IDs (only channel posts, not replies)
            post_ids = []
            for msg in messages:
                if isinstance(msg, Message) and not msg.reply_to:
                    post_ids.append(msg.id)
                if len(post_ids) >= limit:
                    break

            return post_ids

        except FloodWaitError:
            raise  # Let retry decorator handle this
        except (ChannelPrivateError, ChannelInvalidError):
            raise  # No access
        except Exception as e:
            logger.error(f"Telethon get posts error: {e}", exc_info=True)
            return []

    async def _get_posts_via_bot_api(
        self,
        channel_id: int,
        limit: int
    ) -> List[int]:
        """Get posts using Bot API (limited functionality)"""
        try:
            # Bot API doesn't have getChatHistory, but we can try some workarounds

            # First check if bot can access the chat
            try:
                chat = await self.bot.get_chat(channel_id)
                logger.debug(f"Bot has access to chat: {chat.title}")
            except (BadRequest, Forbidden):
                return []

            # Bot API is very limited for getting message history
            # This is mainly useful for channels where bot receives updates
            # Return empty for now as Bot API doesn't support history
            return []

        except Exception as e:
            logger.debug(f"Bot API error: {e}")
            return []

    async def _get_posts_via_web_parse(
        self,
        username: str,
        limit: int
    ) -> List[int]:
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

            posts = []

            # Look for message containers
            for message in soup.find_all('div', {'class': 'tgme_widget_message'}):
                post_attr = message.get('data-post')
                if post_attr:
                    # Extract post ID from data-post attribute
                    parts = post_attr.split('/')
                    if len(parts) == 2 and parts[1].isdigit():
                        posts.append(int(parts[1]))

            # Alternative method
            if not posts:
                for link in soup.find_all('a', href=re.compile(rf't\.me/{username}/\d+')):
                    match = re.search(rf't\.me/{username}/(\d+)', link.get('href', ''))
                    if match:
                        posts.append(int(match.group(1)))

            return sorted(list(set(posts)))[-limit:]

        except Exception as e:
            logger.error(f"Web parse error: {e}", exc_info=True)
            return []

    # ============ CACHE MANAGEMENT ============

    def _get_cached_posts(self, channel_id: int) -> Optional[List[int]]:
        """Get posts from cache if valid"""
        if channel_id not in self._channel_cache:
            return None

        cache_data = self._channel_cache[channel_id]
        if 'posts' not in cache_data:
            return None

        # Check if cache is still valid
        timestamp = self._cache_timestamps['posts'].get(channel_id)
        if not timestamp:
            return None

        age = (datetime.utcnow() - timestamp).total_seconds()
        if age > self._cache_ttl['posts']:
            return None

        return cache_data['posts']

    def _cache_posts(self, channel_id: int, posts: List[int]):
        """Cache posts for channel"""
        if channel_id not in self._channel_cache:
            self._channel_cache[channel_id] = {}

        # Merge with existing posts
        existing = self._channel_cache[channel_id].get('posts', [])
        all_posts = sorted(list(set(existing + posts)))

        # Keep last 2000 posts
        if len(all_posts) > 2000:
            all_posts = all_posts[-2000:]

        self._channel_cache[channel_id]['posts'] = all_posts
        self._cache_timestamps['posts'][channel_id] = datetime.utcnow()

    # ============ RATE LIMITING & ERROR HANDLING ============

    def _should_skip_channel(self, channel: DBChannel) -> bool:
        """Check if channel should be skipped due to errors"""
        errors = self._channel_errors.get(channel.id, [])
        if len(errors) >= self._max_errors_per_channel:
            # Check if last error was recent
            if errors:
                last_error_time = errors[-1]['timestamp']
                if (datetime.utcnow() - last_error_time).total_seconds() < 3600:
                    logger.warning(
                        f"Skipping {channel.channel_username} - too many recent errors",
                        error_count=len(errors)
                    )
                    return True
        return False

    def _can_check_channel(self, channel_id: int) -> bool:
        """Check if enough time passed since last check"""
        last_check = self._last_check.get(channel_id)
        if last_check:
            time_since = (datetime.utcnow() - last_check).total_seconds()
            if time_since < self._min_check_interval:
                logger.debug(f"Rate limit: checked {time_since:.1f}s ago")
                return False
        return True

    def _is_flood_limited(self, channel_id: int) -> bool:
        """Check if channel is flood limited"""
        wait_until = self._flood_wait_until.get(channel_id)
        if wait_until and datetime.utcnow() < wait_until:
            return True
        return False

    def _handle_flood_wait(self, channel_id: int, wait_seconds: int):
        """Handle flood wait error"""
        wait_until = datetime.utcnow() + timedelta(seconds=wait_seconds)
        self._flood_wait_until[channel_id] = wait_until
        logger.warning(
            f"Flood wait for channel {channel_id}",
            wait_seconds=wait_seconds,
            wait_until=wait_until
        )

    async def _telethon_rate_limit_check(self):
        """Check Telethon rate limit"""
        time_since = (datetime.utcnow() - self._last_telethon_request).total_seconds()
        if time_since < self._telethon_rate_limit:
            wait_time = self._telethon_rate_limit - time_since
            await asyncio.sleep(wait_time)
        self._last_telethon_request = datetime.utcnow()

    def _track_channel_error(self, channel_id: int, error: str, error_type: str):
        """Track channel errors"""
        if channel_id not in self._channel_errors:
            self._channel_errors[channel_id] = []

        self._channel_errors[channel_id].append({
            'error': error,
            'type': error_type,
            'timestamp': datetime.utcnow()
        })

        # Keep only last N errors
        if len(self._channel_errors[channel_id]) > self._max_errors_per_channel:
            self._channel_errors[channel_id] = self._channel_errors[channel_id][-self._max_errors_per_channel:]

    # ============ BOT COMMANDS ============

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        self._admin_commands_count += 1

        welcome_message = (
            "🤖 <b>Telegram SMM Bot</b>\n\n"
            "Автоматична система накрутки для Telegram каналів\n"
            f"<i>Telethon: {'✅ Active' if self.telethon_initialized else '❌ Inactive'}</i>\n\n"
            "<b>Основні команди:</b>\n"
            "/status - Статус системи\n"
            "/channels - Список каналів\n"
            "/stats - Статистика по каналах\n"
            "/costs - Витрати на накрутку\n"
            "/balance - Баланс Nakrutka\n"
            "/orders - Активні замовлення\n"
            "/errors - Помилки системи\n"
            "/health - Перевірка здоров'я\n"
            "/telethon_status - Статус Telethon\n"
            "/check_access <username> - Перевірити доступ\n"
            "/help - Детальна допомога\n\n"
            "<i>Бот працює 24/7 в автоматичному режимі</i>"
        )

        await update.message.reply_text(
            welcome_message,
            parse_mode=ParseMode.HTML
        )

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command - показати статус системи"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        self._admin_commands_count += 1

        # Збираємо інформацію про статус
        uptime = datetime.utcnow() - self.start_time
        uptime_hours = uptime.total_seconds() / 3600

        # Перевірка компонентів
        db_status = "✅" if self.db else "❌"
        nakrutka_status = "✅" if self.nakrutka_client else "❌"
        telethon_status = "✅" if self.telethon_initialized else "❌"

        # Активні замовлення
        try:
            active_orders = await self.db.fetchval(
                "SELECT COUNT(*) FROM orders WHERE status = 'in_progress'"
            )
        except:
            active_orders = "Error"

        # Активні канали
        try:
            active_channels = await self.db.fetchval(
                "SELECT COUNT(*) FROM channels WHERE is_active = true"
            )
        except:
            active_channels = "Error"

        # Статистика моніторингу
        total_posts_today = self._stats['total_posts_found']

        message = (
            "🟢 <b>Статус системи</b>\n\n"
            f"⏱ Uptime: {uptime_hours:.1f} годин\n"
            f"💾 База даних: {db_status}\n"
            f"💰 Nakrutka API: {nakrutka_status}\n"
            f"🔌 Telethon: {telethon_status}\n\n"
            f"📊 <b>Поточні показники:</b>\n"
            f"Активних каналів: {active_channels}\n"
            f"Активних замовлень: {active_orders}\n"
            f"Знайдено постів за сесію: {total_posts_today}\n\n"
            f"📈 <b>Методи доступу:</b>\n"
            f"Telethon: {self._stats['telethon_success']} ✓ / {self._stats['telethon_fail']} ✗\n"
            f"Bot API: {self._stats['bot_api_success']} ✓ / {self._stats['bot_api_fail']} ✗\n"
            f"Web Parse: {self._stats['web_parse_success']} ✓ / {self._stats['web_parse_fail']} ✗"
        )

        await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    async def cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command - статистика по каналах"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        self._admin_commands_count += 1

        try:
            # Отримуємо статистику по каналах
            stats = await self.db.fetch("""
                SELECT 
                    c.channel_username,
                    COUNT(DISTINCT p.id) as total_posts,
                    COUNT(DISTINCT CASE WHEN p.status = 'completed' THEN p.id END) as completed_posts,
                    COUNT(DISTINCT o.id) as total_orders,
                    COALESCE(SUM(o.total_quantity), 0) as total_quantity_ordered
                FROM channels c
                LEFT JOIN posts p ON c.id = p.channel_id
                LEFT JOIN orders o ON p.id = o.post_id
                WHERE c.is_active = true
                GROUP BY c.id, c.channel_username
                ORDER BY total_posts DESC
                LIMIT 10
            """)

            if not stats:
                await update.message.reply_text("📊 Немає даних для відображення")
                return

            message = "📊 <b>Статистика по каналах</b>\n\n"

            for stat in stats:
                message += (
                    f"📺 <b>@{stat['channel_username']}</b>\n"
                    f"├ Постів: {stat['total_posts']} (оброблено: {stat['completed_posts']})\n"
                    f"├ Замовлень: {stat['total_orders']}\n"
                    f"└ Накручено: {format_number(stat['total_quantity_ordered'])}\n\n"
                )

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error(f"Error in cmd_stats: {e}")
            await update.message.reply_text("❌ Помилка отримання статистики")

    async def cmd_channels(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /channels command - список активних каналів"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        self._admin_commands_count += 1

        try:
            channels = await self.db.fetch("""
                SELECT 
                    c.*,
                    cs.views_target,
                    cs.reactions_target,
                    cs.reposts_target,
                    cs.randomize_percent
                FROM channels c
                LEFT JOIN channel_settings cs ON c.id = cs.channel_id
                WHERE c.is_active = true
                ORDER BY c.created_at DESC
            """)

            if not channels:
                await update.message.reply_text(
                    "📺 Немає активних каналів\n\n"
                    "Додати канал: /add_channel @username"
                )
                return

            message = "📺 <b>Активні канали</b>\n\n"

            for ch in channels:
                access_method = self._channel_access.get(ch['id'], 'unknown')
                message += (
                    f"@{ch['channel_username']} "
                    f"({'✅' + access_method if access_method != 'unknown' else '❓'})\n"
                    f"├ 👁 {ch['views_target'] or 0} "
                    f"❤️ {ch['reactions_target'] or 0} "
                    f"🔄 {ch['reposts_target'] or 0}\n"
                    f"└ 🎲 ±{ch['randomize_percent'] or 0}%\n\n"
                )

            message += (
                "\n<b>Команди:</b>\n"
                "/add_channel @username - Додати канал\n"
                "/remove_channel @username - Видалити канал\n"
                "/channel_info @username - Детальна інформація"
            )

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error(f"Error in cmd_channels: {e}")
            await update.message.reply_text("❌ Помилка отримання списку каналів")

    async def cmd_costs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /costs command - витрати на накрутку"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        self._admin_commands_count += 1

        try:
            # Витрати за сьогодні
            today_costs = await self.db.fetchrow("""
                SELECT 
                    COUNT(DISTINCT o.id) as orders_count,
                    SUM(o.total_quantity) as total_quantity,
                    SUM(o.total_quantity * s.price_per_1000 / 1000) as total_cost
                FROM orders o
                JOIN services s ON o.service_id = s.nakrutka_id
                WHERE DATE(o.created_at) = CURRENT_DATE
            """)

            # Витрати за тиждень
            week_costs = await self.db.fetchrow("""
                SELECT 
                    COUNT(DISTINCT o.id) as orders_count,
                    SUM(o.total_quantity) as total_quantity,
                    SUM(o.total_quantity * s.price_per_1000 / 1000) as total_cost
                FROM orders o
                JOIN services s ON o.service_id = s.nakrutka_id
                WHERE o.created_at >= CURRENT_DATE - INTERVAL '7 days'
            """)

            # Витрати за місяць
            month_costs = await self.db.fetchrow("""
                SELECT 
                    COUNT(DISTINCT o.id) as orders_count,
                    SUM(o.total_quantity) as total_quantity,
                    SUM(o.total_quantity * s.price_per_1000 / 1000) as total_cost
                FROM orders o
                JOIN services s ON o.service_id = s.nakrutka_id
                WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'
            """)

            # Витрати по типах
            type_costs = await self.db.fetch("""
                SELECT 
                    o.service_type,
                    COUNT(DISTINCT o.id) as orders_count,
                    SUM(o.total_quantity) as total_quantity,
                    SUM(o.total_quantity * s.price_per_1000 / 1000) as total_cost
                FROM orders o
                JOIN services s ON o.service_id = s.nakrutka_id
                WHERE o.created_at >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY o.service_type
                ORDER BY total_cost DESC
            """)

            message = "💰 <b>Витрати на накрутку</b>\n\n"

            # Сьогодні
            message += "📅 <b>Сьогодні:</b>\n"
            message += f"├ Замовлень: {today_costs['orders_count'] or 0}\n"
            message += f"├ Накручено: {format_number(today_costs['total_quantity'] or 0)}\n"
            message += f"└ Витрачено: ${today_costs['total_cost'] or 0:.2f}\n\n"

            # Тиждень
            message += "📅 <b>За тиждень:</b>\n"
            message += f"├ Замовлень: {week_costs['orders_count'] or 0}\n"
            message += f"├ Накручено: {format_number(week_costs['total_quantity'] or 0)}\n"
            message += f"└ Витрачено: ${week_costs['total_cost'] or 0:.2f}\n\n"

            # Місяць
            message += "📅 <b>За місяць:</b>\n"
            message += f"├ Замовлень: {month_costs['orders_count'] or 0}\n"
            message += f"├ Накручено: {format_number(month_costs['total_quantity'] or 0)}\n"
            message += f"└ Витрачено: ${month_costs['total_cost'] or 0:.2f}\n\n"

            # По типах
            if type_costs:
                message += "📊 <b>По типах (тиждень):</b>\n"
                for tc in type_costs:
                    emoji = {'views': '👁', 'reactions': '❤️', 'reposts': '🔄'}.get(tc['service_type'], '❓')
                    message += f"{emoji} {tc['service_type']}: ${tc['total_cost'] or 0:.2f}\n"

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error(f"Error in cmd_costs: {e}")
            await update.message.reply_text("❌ Помилка отримання витрат")

    async def cmd_balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /balance command - баланс Nakrutka"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        self._admin_commands_count += 1

        if not self.nakrutka_client:
            await update.message.reply_text("❌ Nakrutka client не ініціалізовано")
            return

        try:
            balance_info = await self.nakrutka_client.get_balance()
            balance = float(balance_info.get('balance', 0))
            currency = balance_info.get('currency', 'USD')

            # Визначаємо статус балансу
            if balance < 1:
                status = "🔴 Критично низький!"
                emoji = "❌"
            elif balance < 10:
                status = "🟡 Низький"
                emoji = "⚠️"
            elif balance < 50:
                status = "🟢 Нормальний"
                emoji = "✅"
            else:
                status = "🟢 Відмінний"
                emoji = "💎"

            # Розраховуємо на скільки вистачить
            avg_daily_cost = await self.db.fetchval("""
                SELECT AVG(daily_cost) as avg_cost
                FROM (
                    SELECT 
                        DATE(o.created_at) as date,
                        SUM(o.total_quantity * s.price_per_1000 / 1000) as daily_cost
                    FROM orders o
                    JOIN services s ON o.service_id = s.nakrutka_id
                    WHERE o.created_at >= CURRENT_DATE - INTERVAL '7 days'
                    GROUP BY DATE(o.created_at)
                ) daily_costs
            """)

            days_left = int(balance / avg_daily_cost) if avg_daily_cost and avg_daily_cost > 0 else 999

            message = (
                f"💰 <b>Баланс Nakrutka</b>\n\n"
                f"{emoji} Баланс: ${balance:.2f} {currency}\n"
                f"📊 Статус: {status}\n"
            )

            if avg_daily_cost and avg_daily_cost > 0:
                message += (
                    f"\n📈 <b>Аналітика:</b>\n"
                    f"├ Середні витрати: ${avg_daily_cost:.2f}/день\n"
                    f"└ Вистачить на: ~{days_left} днів\n"
                )

            if balance < 10:
                message += "\n⚠️ <b>Рекомендується поповнити баланс!</b>"

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error(f"Error in cmd_balance: {e}")
            await update.message.reply_text("❌ Помилка отримання балансу")

    async def cmd_orders(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /orders command - активні замовлення"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        self._admin_commands_count += 1

        try:
            orders = await self.db.fetch("""
                SELECT 
                    o.*,
                    p.post_url,
                    c.channel_username,
                    s.service_name,
                    s.price_per_1000
                FROM orders o
                JOIN posts p ON o.post_id = p.id
                JOIN channels c ON p.channel_id = c.id
                LEFT JOIN services s ON o.service_id = s.nakrutka_id
                WHERE o.status IN ('pending', 'in_progress')
                ORDER BY o.created_at DESC
                LIMIT 15
            """)

            if not orders:
                await update.message.reply_text("📭 Немає активних замовлень")
                return

            message = "📋 <b>Активні замовлення</b>\n\n"

            # Групуємо по статусу
            pending = [o for o in orders if o['status'] == 'pending']
            in_progress = [o for o in orders if o['status'] == 'in_progress']

            if in_progress:
                message += "🔄 <b>В процесі:</b>\n"
                for order in in_progress[:7]:
                    emoji = {'views': '👁', 'reactions': '❤️', 'reposts': '🔄'}.get(order['service_type'], '❓')
                    cost = (order['total_quantity'] * order['price_per_1000'] / 1000) if order['price_per_1000'] else 0
                    message += (
                        f"{emoji} @{order['channel_username']}/{order['post_url'].split('/')[-1]}\n"
                        f"├ К-сть: {format_number(order['total_quantity'])}\n"
                        f"├ Ціна: ${cost:.3f}\n"
                        f"└ ID: {order['nakrutka_order_id']}\n\n"
                    )

            if pending:
                message += "⏳ <b>В черзі:</b>\n"
                for order in pending[:5]:
                    emoji = {'views': '👁', 'reactions': '❤️', 'reposts': '🔄'}.get(order['service_type'], '❓')
                    message += f"{emoji} {order['channel_username']}: {format_number(order['total_quantity'])}\n"

            message += f"\n📊 Всього активних: {len(orders)}"

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error(f"Error in cmd_orders: {e}")
            await update.message.reply_text("❌ Помилка отримання замовлень")

    async def cmd_errors(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /errors command - останні помилки"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        self._admin_commands_count += 1

        try:
            # Помилки з БД
            db_errors = await self.db.fetch("""
                SELECT * FROM logs 
                WHERE level = 'error' 
                ORDER BY created_at DESC 
                LIMIT 10
            """)

            message = "❌ <b>Останні помилки</b>\n\n"

            # Помилки каналів в пам'яті
            if self._channel_errors:
                message += "📺 <b>Помилки каналів:</b>\n"
                for channel_id, errors in list(self._channel_errors.items())[:5]:
                    channel = await self.db.fetchrow(
                        "SELECT channel_username FROM channels WHERE id = $1",
                        channel_id
                    )
                    if channel:
                        last_error = errors[-1]
                        message += (
                            f"@{channel['channel_username']}:\n"
                            f"├ {last_error['type']}\n"
                            f"├ {truncate_text(last_error['error'], 50)}\n"
                            f"└ {last_error['timestamp'].strftime('%H:%M')}\n\n"
                        )

            # Помилки з БД
            if db_errors:
                message += "\n💾 <b>Системні помилки:</b>\n"
                for error in db_errors[:5]:
                    context_data = error.get('context', {})
                    message += (
                        f"🕐 {error['created_at'].strftime('%d.%m %H:%M')}\n"
                        f"├ {truncate_text(error['message'], 100)}\n"
                    )
                    if context_data.get('error_type'):
                        message += f"└ Type: {context_data['error_type']}\n"
                    message += "\n"

            if not self._channel_errors and not db_errors:
                message = "✅ Немає помилок!"

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except Exception as e:
            logger.error(f"Error in cmd_errors: {e}")
            await update.message.reply_text("❌ Помилка отримання логів помилок")

    async def cmd_cache(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /cache command - інформація про кеш"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        self._admin_commands_count += 1

        # Статистика кешу
        total_cached_channels = len(self._channel_cache)
        total_cached_posts = sum(
            len(data.get('posts', []))
            for data in self._channel_cache.values()
        )

        # Розміри кешу по типах
        cache_sizes = {
            'posts': len(self._cache_timestamps['posts']),
            'channel_info': len(self._cache_timestamps['channel_info']),
            'access': len(self._cache_timestamps['access'])
        }

        message = (
            "💾 <b>Статистика кешу</b>\n\n"
            f"📊 Закешовано каналів: {total_cached_channels}\n"
            f"📝 Закешовано постів: {format_number(total_cached_posts)}\n\n"
            f"<b>По типах:</b>\n"
        )

        for cache_type, size in cache_sizes.items():
            ttl = self._cache_ttl[cache_type]
            message += f"├ {cache_type}: {size} записів (TTL: {ttl}s)\n"

        # Топ каналів по кількості постів в кеші
        if self._channel_cache:
            message += "\n<b>Топ каналів в кеші:</b>\n"
            sorted_channels = sorted(
                self._channel_cache.items(),
                key=lambda x: len(x[1].get('posts', [])),
                reverse=True
            )[:5]

            for channel_id, data in sorted_channels:
                posts_count = len(data.get('posts', []))
                channel = await self.db.fetchrow(
                    "SELECT channel_username FROM channels WHERE id = $1",
                    channel_id
                )
                if channel:
                    message += f"├ @{channel['channel_username']}: {posts_count} постів\n"

        await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    async def cmd_health(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /health command - перевірка здоров'я системи"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        self._admin_commands_count += 1

        msg = await update.message.reply_text("🏥 Перевіряю здоров'я системи...")

        health_checks = {
            'database': False,
            'nakrutka': False,
            'telethon': False,
            'bot_api': False
        }

        # Перевірка БД
        try:
            await self.db.fetchval("SELECT 1")
            health_checks['database'] = True
        except:
            pass

        # Перевірка Nakrutka
        if self.nakrutka_client:
            try:
                health = await self.nakrutka_client.health_check()
                health_checks['nakrutka'] = health['status'] == 'healthy'
            except:
                pass

        # Перевірка Telethon
        health_checks['telethon'] = self.telethon_initialized

        # Перевірка Bot API
        try:
            bot_info = await self.bot.get_me()
            health_checks['bot_api'] = True
        except:
            pass

        # Формуємо повідомлення
        all_ok = all(health_checks.values())
        status_emoji = "✅" if all_ok else "⚠️"

        message = f"{status_emoji} <b>Здоров'я системи</b>\n\n"

        for component, status in health_checks.items():
            emoji = "✅" if status else "❌"
            message += f"{emoji} {component.title()}\n"

        # Додаткова статистика
        uptime = datetime.utcnow() - self.start_time
        message += (
            f"\n📊 <b>Статистика:</b>\n"
            f"├ Uptime: {format_duration(int(uptime.total_seconds()))}\n"
            f"├ Команд від адміна: {self._admin_commands_count}\n"
            f"├ Помилок каналів: {sum(len(e) for e in self._channel_errors.values())}\n"
            f"└ Flood limits: {len(self._flood_wait_until)}\n"
        )

        if not all_ok:
            message += "\n⚠️ <b>Деякі компоненти не працюють!</b>"

        await msg.edit_text(message, parse_mode=ParseMode.HTML)

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command - детальна допомога"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        self._admin_commands_count += 1

        help_message = (
            "📚 <b>Детальна допомога по командах</b>\n\n"
            "<b>🔍 Моніторинг:</b>\n"
            "/status - Загальний статус системи\n"
            "/channels - Список активних каналів\n"
            "/stats - Статистика по каналах\n"
            "/check_access @username - Перевірити доступ до каналу\n\n"

            "<b>💰 Фінанси:</b>\n"
            "/balance - Баланс Nakrutka\n"
            "/costs - Витрати на накрутку\n"
            "/orders - Активні замовлення\n\n"
            
            "<b>⚙️ Система:</b>\n"
            "/health - Перевірка компонентів\n"
            "/errors - Останні помилки\n"
            "/cache - Статистика кешу\n"
            "/telethon_status - Статус Telethon\n\n"
            
            "<b>📺 Управління каналами:</b>\n"
            "/add_channel @username - Додати канал\n"
            "/remove_channel @username - Видалити канал\n"
            "/channel_info @username - Інформація про канал\n"
            "/set_old_posts @username N - Змінити к-сть старих постів\n"
            "/stop_order <ID> - Зупинити замовлення\n\n"
            
            "<b>💡 Поради:</b>\n"
            "• Бот автоматично перевіряє канали кожні 30 секунд\n"
            "• Використовуйте Telethon для приватних каналів\n"
            "• Слідкуйте за балансом Nakrutka\n"
            "• Перевіряйте /errors при проблемах\n"
        )

        await update.message.reply_text(help_message, parse_mode=ParseMode.HTML)

    async def cmd_add_channel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /add_channel command - додати новий канал"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        if not context.args:
            await update.message.reply_text(
                "❌ Вкажіть username каналу\n\n"
                "Формат: /add_channel @username [кількість_старих_постів]\n\n"
                "Приклади:\n"
                "• /add_channel @durov - не обробляти старі пости\n"
                "• /add_channel @durov 0 - теж не обробляти старі\n"
                "• /add_channel @durov 5 - обробити останні 5 постів\n"
                "• /add_channel @durov 20 - обробити останні 20 постів"
            )
            return

        username = context.args[0].lstrip('@').lower()

        # Визначаємо кількість старих постів для обробки
        process_old_posts = 0  # За замовчуванням - не обробляти старі

        if len(context.args) > 1:
            try:
                process_old_posts = int(context.args[1])
                if process_old_posts < 0:
                    await update.message.reply_text("❌ Кількість постів не може бути від'ємною")
                    return
                if process_old_posts > 100:
                    await update.message.reply_text("❌ Максимум 100 старих постів")
                    return
            except ValueError:
                await update.message.reply_text("❌ Кількість постів має бути числом")
                return

        # Перевіряємо чи вже є такий канал
        existing = await self.db.fetchrow(
            "SELECT * FROM channels WHERE channel_username = $1",
            username
        )

        if existing:
            if existing['is_active']:
                await update.message.reply_text(f"❌ Канал @{username} вже активний")
            else:
                # Активуємо існуючий канал і оновлюємо налаштування
                await self.db.execute(
                    "UPDATE channels SET is_active = true WHERE id = $1",
                    existing['id']
                )
                # Оновлюємо process_old_posts_count
                await self.db.execute(
                    "UPDATE channel_settings SET process_old_posts_count = $1 WHERE channel_id = $2",
                    process_old_posts, existing['id']
                )
                await update.message.reply_text(
                    f"✅ Канал @{username} активовано\n"
                    f"Старі пости: {process_old_posts if process_old_posts > 0 else 'не обробляти'}"
                )
            return

        # Перевіряємо доступ до каналу
        msg = await update.message.reply_text(f"🔍 Перевіряю канал @{username}...")

        try:
            # Спробуємо отримати інформацію про канал
            channel_id = None
            channel_title = None

            # Спроба через Telethon
            if self.telethon_initialized:
                try:
                    entity = await self.telethon_client.get_entity(username)
                    if hasattr(entity, 'id'):
                        channel_id = entity.id
                        channel_title = getattr(entity, 'title', username)
                except:
                    pass

            # Спроба через Bot API
            if not channel_id:
                try:
                    chat = await self.bot.get_chat(f"@{username}")
                    channel_id = chat.id
                    channel_title = chat.title
                except:
                    pass

            # Створюємо канал
            new_channel_id = await self.db.fetchval("""
                                                    INSERT INTO channels (channel_username, channel_id, is_active)
                                                    VALUES ($1, $2, true) RETURNING id
                                                    """, username, channel_id)

            # Створюємо налаштування з вказаною кількістю старих постів
            await self.db.execute("""
                                  INSERT INTO channel_settings (channel_id,
                                                                views_target,
                                                                reactions_target,
                                                                reposts_target,
                                                                randomize_percent,
                                                                process_old_posts_count,
                                                                max_post_age_hours)
                                  VALUES ($1, 1000, 50, 20, 30, $2, 24)
                                  """, new_channel_id, process_old_posts)

            success_msg = f"✅ Канал @{username} додано!\n"
            if channel_title:
                success_msg += f"Назва: {channel_title}\n"
            if channel_id:
                success_msg += f"ID: {channel_id}\n"
            success_msg += "\nНалаштування за замовчуванням:\n"
            success_msg += "👁 1000 | ❤️ 50 | 🔄 20 | 🎲 ±30%\n\n"
            success_msg += f"📝 Старі пости: {process_old_posts if process_old_posts > 0 else 'не обробляти'}\n"
            success_msg += f"⏰ Макс. вік поста: 24 години"

            await msg.edit_text(success_msg)

            # Логуємо
            await self.db_logger.info(
                "Channel added",
                channel=username,
                channel_id=channel_id,
                process_old_posts=process_old_posts,
                added_by=update.effective_user.id
            )

        except Exception as e:
            logger.error(f"Error adding channel: {e}")
            await msg.edit_text(f"❌ Помилка додавання каналу: {str(e)[:100]}")

    async def cmd_remove_channel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /remove_channel command - видалити канал"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        if not context.args:
            await update.message.reply_text(
                "❌ Вкажіть username каналу\n"
                "Приклад: /remove_channel @durov"
            )
            return

        username = context.args[0].lstrip('@').lower()

        # Знаходимо канал
        channel = await self.db.fetchrow(
            "SELECT * FROM channels WHERE channel_username = $1",
            username
        )

        if not channel:
            await update.message.reply_text(f"❌ Канал @{username} не знайдено")
            return

        # Деактивуємо канал (не видаляємо для збереження історії)
        await self.db.execute(
            "UPDATE channels SET is_active = false WHERE id = $1",
            channel['id']
        )

        # Скасовуємо активні замовлення
        cancelled = await self.db.fetchval("""
            UPDATE orders 
            SET status = 'cancelled'
            WHERE post_id IN (
                SELECT id FROM posts WHERE channel_id = $1
            ) AND status IN ('pending', 'in_progress')
            RETURNING COUNT(*)
        """, channel['id'])

        message = f"✅ Канал @{username} деактивовано"
        if cancelled:
            message += f"\n📛 Скасовано замовлень: {cancelled}"

        await update.message.reply_text(message)

        # Логуємо
        await self.db_logger.info(
            "Channel removed",
            channel=username,
            removed_by=update.effective_user.id,
            cancelled_orders=cancelled
        )

    async def cmd_channel_info(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /channel_info command - детальна інформація про канал"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        if not context.args:
            await update.message.reply_text(
                "❌ Вкажіть username каналу\n"
                "Приклад: /channel_info @durov"
            )
            return

        username = context.args[0].lstrip('@').lower()

        # Отримуємо інформацію з БД
        channel_data = await self.db.fetchrow("""
            SELECT 
                c.*,
                cs.*,
                COUNT(DISTINCT p.id) as total_posts,
                COUNT(DISTINCT CASE WHEN p.status = 'completed' THEN p.id END) as completed_posts,
                COUNT(DISTINCT o.id) as total_orders,
                COALESCE(SUM(o.total_quantity * s.price_per_1000 / 1000), 0) as total_spent
            FROM channels c
            LEFT JOIN channel_settings cs ON c.id = cs.channel_id
            LEFT JOIN posts p ON c.id = p.channel_id
            LEFT JOIN orders o ON p.id = o.post_id
            LEFT JOIN services s ON o.service_id = s.nakrutka_id
            WHERE c.channel_username = $1
            GROUP BY c.id, cs.id
        """, username)

        if not channel_data:
            await update.message.reply_text(f"❌ Канал @{username} не знайдено")
            return

        # Формуємо повідомлення
        status = "✅ Активний" if channel_data['is_active'] else "❌ Неактивний"
        access = self._channel_access.get(channel_data['id'], 'unknown')

        message = (
            f"📺 <b>Канал @{username}</b>\n\n"
            f"Статус: {status}\n"
            f"Доступ: {access}\n"
            f"ID: {channel_data['channel_id'] or 'невідомо'}\n"
            f"Додано: {channel_data['created_at'].strftime('%d.%m.%Y')}\n\n"
            
            f"<b>Налаштування накрутки:</b>\n"
            f"👁 Перегляди: {channel_data['views_target'] or 0}\n"
            f"❤️ Реакції: {channel_data['reactions_target'] or 0}\n"
            f"🔄 Репости: {channel_data['reposts_target'] or 0}\n"
            f"🎲 Рандомізація: ±{channel_data['randomize_percent'] or 0}%\n\n"
            
            f"<b>Статистика:</b>\n"
            f"Постів: {channel_data['total_posts']} (оброблено: {channel_data['completed_posts']})\n"
            f"Замовлень: {channel_data['total_orders']}\n"
            f"Витрачено: ${channel_data['total_spent']:.2f}\n"
        )

        # Останні пости
        recent_posts = await self.db.fetch("""
            SELECT * FROM posts 
            WHERE channel_id = $1 
            ORDER BY created_at DESC 
            LIMIT 5
        """, channel_data['id'])

        if recent_posts:
            message += "\n<b>Останні пости:</b>\n"
            for post in recent_posts:
                status_emoji = {
                    'new': '🆕',
                    'processing': '⏳',
                    'completed': '✅',
                    'failed': '❌'
                }.get(post['status'], '❓')
                message += f"{status_emoji} /{post['post_id']} - {post['created_at'].strftime('%H:%M')}\n"

        await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    async def cmd_set_old_posts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Змінити налаштування обробки старих постів для каналу"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        if len(context.args) < 2:
            await update.message.reply_text(
                "❌ Вкажіть канал і кількість\n\n"
                "Формат: /set_old_posts @username кількість\n\n"
                "Приклад: /set_old_posts @durov 0"
            )
            return

        username = context.args[0].lstrip('@').lower()

        try:
            count = int(context.args[1])
            if count < 0 or count > 100:
                await update.message.reply_text("❌ Кількість має бути від 0 до 100")
                return
        except ValueError:
            await update.message.reply_text("❌ Кількість має бути числом")
            return

        # Оновлюємо налаштування
        result = await self.db.execute("""
                                       UPDATE channel_settings
                                       SET process_old_posts_count = $1
                                       WHERE channel_id = (SELECT id
                                                           FROM channels
                                                           WHERE channel_username = $2)
                                       """, count, username)

        if result == "UPDATE 1":
            await update.message.reply_text(
                f"✅ Оновлено налаштування для @{username}\n"
                f"Старі пости: {count if count > 0 else 'не обробляти'}"
            )
        else:
            await update.message.reply_text(f"❌ Канал @{username} не знайдено")

    async def cmd_stop_order(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stop_order command - зупинити замовлення"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        if not context.args:
            await update.message.reply_text(
                "❌ Вкажіть ID замовлення\n"
                "Приклад: /stop_order 12345"
            )
            return

        try:
            order_id = context.args[0]

            # Перевіряємо чи це Nakrutka ID чи наш ID
            if order_id.isdigit():
                # Наш ID
                order = await self.db.fetchrow(
                    "SELECT * FROM orders WHERE id = $1",
                    int(order_id)
                )
            else:
                # Nakrutka ID
                order = await self.db.fetchrow(
                    "SELECT * FROM orders WHERE nakrutka_order_id = $1",
                    order_id
                )

            if not order:
                await update.message.reply_text(f"❌ Замовлення {order_id} не знайдено")
                return

            if order['status'] not in ['pending', 'in_progress']:
                await update.message.reply_text(
                    f"❌ Замовлення вже {order['status']}"
                )
                return

            # Скасовуємо через Nakrutka API
            if self.nakrutka_client and order['nakrutka_order_id']:
                try:
                    result = await self.nakrutka_client.cancel_order(
                        order['nakrutka_order_id']
                    )
                    if result:
                        await update.message.reply_text(
                            f"✅ Замовлення {order['nakrutka_order_id']} скасовано в Nakrutka"
                        )
                except Exception as e:
                    logger.error(f"Error cancelling order in Nakrutka: {e}")

            # Оновлюємо статус в БД
            await self.db.execute(
                "UPDATE orders SET status = 'cancelled' WHERE id = $1",
                order['id']
            )

            await update.message.reply_text(
                f"⏹ Замовлення {order['id']} ({order['service_type']}) скасовано"
            )

            # Логуємо
            await self.db_logger.info(
                "Order cancelled",
                order_id=order['id'],
                nakrutka_id=order['nakrutka_order_id'],
                cancelled_by=update.effective_user.id
            )

        except Exception as e:
            logger.error(f"Error in cmd_stop_order: {e}")
            await update.message.reply_text(f"❌ Помилка: {str(e)[:100]}")

    async def cmd_telethon_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /telethon_status command"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        self._admin_commands_count += 1

        status = "❌ Not initialized"
        details = []

        if self.telethon_initialized and self.telethon_me:
            status = "✅ Connected"
            details.append(f"User ID: <code>{self.telethon_me.id}</code>")
            details.append(f"Username: @{self.telethon_me.username or 'none'}")
            details.append(f"Phone: {self.telethon_me.phone or 'hidden'}")
        elif self.telethon_client:
            status = "⚠️ Not authorized"
            details.append("Need to complete authentication")
        else:
            details.append("Telethon client not created")

        # Add statistics
        details.append("\n<b>Access Statistics:</b>")
        details.append(f"Telethon: {self._stats['telethon_success']} ✓ / {self._stats['telethon_fail']} ✗")
        details.append(f"Bot API: {self._stats['bot_api_success']} ✓ / {self._stats['bot_api_fail']} ✗")
        details.append(f"Web Parse: {self._stats['web_parse_success']} ✓ / {self._stats['web_parse_fail']} ✗")

        # Channel access breakdown
        access_counts = {}
        for method in self._access_methods:
            access_counts[method] = sum(1 for m in self._channel_access.values() if m == method)

        details.append("\n<b>Channel Access Methods:</b>")
        for method, count in access_counts.items():
            details.append(f"{method}: {count} channels")

        message = f"🔌 <b>Telethon Status</b>\n\n{status}\n\n" + "\n".join(details)

        await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    async def cmd_telethon_auth(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /telethon_auth <code> command"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        if not context.args or len(context.args) != 1:
            await update.message.reply_text("Usage: /telethon_auth <code>")
            return

        code = context.args[0]

        try:
            if not hasattr(self, '_pending_auth'):
                await update.message.reply_text("❌ No pending authentication")
                return

            # Complete authentication
            await self.telethon_client.sign_in(self._pending_auth['phone'], code)

            # Check if 2FA needed
            # In production, would need to handle 2FA password

            self.telethon_me = await self.telethon_client.get_me()
            self.telethon_initialized = True

            # Save session
            await self._save_session()

            del self._pending_auth

            await update.message.reply_text(
                f"✅ Authentication successful!\n"
                f"Logged in as: @{self.telethon_me.username or 'none'}"
            )

        except SessionPasswordNeededError:
            await update.message.reply_text(
                "❌ 2FA password required. This is not implemented yet."
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Authentication failed: {str(e)[:200]}")

    async def cmd_check_access(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /check_access <username> command"""
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Unauthorized")
            return

        if not context.args:
            await update.message.reply_text("Usage: /check_access <username>")
            return

        username = context.args[0].lstrip('@')
        msg = await update.message.reply_text(f"🔍 Checking access to @{username}...")

        results = []

        # Try Telethon
        if self.telethon_initialized:
            try:
                entity = await self.telethon_client.get_entity(username)
                if isinstance(entity, Channel):
                    results.append(f"✅ Telethon: Can access (ID: {entity.id})")

                    # Get additional info
                    full = await self.telethon_client(GetFullChannelRequest(entity))
                    results.append(f"├ Participants: {full.full_chat.participants_count or 'hidden'}")
                    results.append(f"├ About: {full.full_chat.about or 'none'}")
                    results.append(f"└ Type: {'Broadcast' if entity.broadcast else 'Group'}")
                else:
                    results.append(f"❌ Telethon: Not a channel (type: {type(entity).__name__})")
            except Exception as e:
                results.append(f"❌ Telethon: {type(e).__name__}")
        else:
            results.append("⚠️ Telethon: Not initialized")

        # Try Bot API
        try:
            chat = await self.bot.get_chat(f"@{username}")
            results.append(f"✅ Bot API: Basic info available")
            results.append(f"└ Title: {chat.title}")
        except Exception as e:
            results.append(f"❌ Bot API: {str(e)[:50]}")

        # Try Web parse
        try:
            posts = await self._get_posts_via_web_parse(username, 5)
            if posts:
                results.append(f"✅ Web Parse: Found {len(posts)} posts")
            else:
                results.append("❌ Web Parse: No posts found")
        except Exception:
            results.append("❌ Web Parse: Failed")

        message = f"📊 <b>Access Check: @{username}</b>\n\n" + "\n".join(results)
        await msg.edit_text(message, parse_mode=ParseMode.HTML)

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle non-command messages"""
        if not self._is_admin(update.effective_user.id):
            return

        # Можна додати обробку звичайних повідомлень
        # Наприклад, автоматичне розпізнавання username каналів
        text = update.message.text

        # Шукаємо username в тексті
        username_match = re.search(r'@(\w+)', text)
        if username_match:
            username = username_match.group(1)
            # Можна автоматично запропонувати додати канал
            await update.message.reply_text(
                f"Знайдено канал @{username}\n"
                f"Додати його? Використайте:\n"
                f"/add_channel @{username}"
            )

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
                error_text = str(context.error)[:200]
                await context.bot.send_message(
                    chat_id=settings.admin_telegram_id,
                    text=f"⚠️ Error: {error_text}"
                )
            except:
                pass

    def _is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        return settings.admin_telegram_id and user_id == settings.admin_telegram_id

    def get_monitor_stats(self) -> Dict[str, Any]:
        """Get monitoring statistics"""
        stats = {
            'channels_monitored': len(self._channel_cache),
            'total_cached_posts': sum(
                len(data.get('posts', []))
                for data in self._channel_cache.values()
            ),
            'channels_with_errors': len(self._channel_errors),
            'admin_commands_count': self._admin_commands_count,
            'uptime_seconds': (datetime.utcnow() - self.start_time).total_seconds(),
            'telethon_active': self.telethon_initialized,
            'access_methods': dict(self._channel_access),
            'method_stats': dict(self._stats)
        }

        # Count channels by access method
        for method in self._access_methods:
            stats[f'channels_via_{method}'] = sum(
                1 for m in self._channel_access.values() if m == method
            )

        return stats


# For backward compatibility
TelegramMonitor = EnhancedTelegramMonitor