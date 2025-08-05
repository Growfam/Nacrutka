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

            # Get recent posts using cascade of methods
            recent_posts = await self.get_channel_posts_cascade(
                channel.channel_username,
                channel.channel_id,
                limit=50
            )

            if not recent_posts:
                logger.debug(f"No posts found in {channel.channel_username}")
                return 0

            # Get existing posts from DB
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
                post_ids=new_posts[:5],
                access_method=self._channel_access.get(channel.id, 'unknown')
            )

            # Save new posts to database
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

    # ============ BOT COMMANDS (keeping all existing + new ones) ============

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

    # Keep all existing command handlers...
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

    # ... (keep all other existing commands as they are)

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