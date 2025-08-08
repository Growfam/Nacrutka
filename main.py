#!/usr/bin/env python3
"""
Telegram SMM Bot - Main entry point with MAXIMUM LOGGING and ALL FUNCTIONS
"""
import asyncio
import signal
import sys
import os
from typing import Optional
from datetime import datetime
import traceback
import socket
import re

# Базове логування для старту
print("=" * 70)
print("🚀 STARTING TELEGRAM SMM BOT - DIAGNOSTIC MODE")
print("=" * 70)
print(f"Current directory: {os.getcwd()}")
print(f"Directory contents: {os.listdir('.')}")
print(f"Python executable: {sys.executable}")
print(f"Python path: {sys.path}")
print("=" * 70)

try:
    print("📦 Importing local modules...")
    from src.config import settings

    print("✅ Config imported successfully")

    from src.database.connection import db, test_connection

    print("✅ Database module imported")

    from src.database.migrations import initialize_database, DatabaseMigrations

    print("✅ Migrations module imported")

    from src.database.initialize import DatabaseInitializer

    print("✅ Database initializer imported")

    from src.utils.logger import get_logger, DatabaseLogger

    print("✅ Logger imported")

    from src.services.telegram import TelegramMonitor

    print("✅ Telegram monitor imported")

    from src.services.processor import PostProcessor

    print("✅ Post processor imported")

    from src.services.nakrutka import NakrutkaClient

    print("✅ Nakrutka client imported")

    from src.utils.scheduler import BotScheduler

    print("✅ Scheduler imported")

    from src.utils.helpers import PerformanceMonitor

    print("✅ Performance monitor imported")

    from src.utils.validators import ConfigValidator

    print("✅ Validators imported")

    print("✅ All imports successful!")

except Exception as e:
    print(f"❌ IMPORT ERROR: {type(e).__name__}: {str(e)}")
    print(f"Traceback:\n{traceback.format_exc()}")
    sys.exit(1)

print("=" * 70)

# Налаштування логера
logger = get_logger(__name__)
logger.info("Logger initialized")

# Детальна перевірка змінних середовища
print("🔍 ENVIRONMENT VARIABLES CHECK:")
print("=" * 70)

env_vars = {
    'DATABASE_URL': os.getenv('DATABASE_URL'),
    'SUPABASE_URL': os.getenv('SUPABASE_URL'),
    'SUPABASE_KEY': os.getenv('SUPABASE_KEY'),
    'NAKRUTKA_API_KEY': os.getenv('NAKRUTKA_API_KEY'),
    'NAKRUTKA_API_URL': os.getenv('NAKRUTKA_API_URL'),
    'TELEGRAM_BOT_TOKEN': os.getenv('TELEGRAM_BOT_TOKEN'),
    'CHECK_INTERVAL': os.getenv('CHECK_INTERVAL'),
    'MAX_RETRIES': os.getenv('MAX_RETRIES'),
    'RETRY_DELAY': os.getenv('RETRY_DELAY'),
    'ADMIN_TELEGRAM_ID': os.getenv('ADMIN_TELEGRAM_ID'),
    'ENVIRONMENT': os.getenv('ENVIRONMENT'),
    'LOG_LEVEL': os.getenv('LOG_LEVEL'),
    'TZ': os.getenv('TZ')
}

for var_name, var_value in env_vars.items():
    if var_value:
        if 'KEY' in var_name or 'TOKEN' in var_name or 'PASSWORD' in var_value:
            # Маскуємо чутливі дані
            masked_value = var_value[:4] + '*' * (len(var_value) - 8) + var_value[-4:] if len(var_value) > 8 else '***'
            print(f"✅ {var_name}: {masked_value}")
        elif 'URL' in var_name:
            # Для URL показуємо початок
            print(f"✅ {var_name}: {var_value[:30]}...")
        else:
            print(f"✅ {var_name}: {var_value}")
    else:
        print(f"❌ {var_name}: NOT SET")

print("=" * 70)

# Перевірка settings з pydantic
print("🔍 CHECKING PYDANTIC SETTINGS:")
try:
    print(f"✅ settings.database_url exists: {bool(settings.database_url)}")
    print(f"✅ settings.nakrutka_api_key exists: {bool(settings.nakrutka_api_key)}")
    print(f"✅ settings.telegram_bot_token exists: {bool(settings.telegram_bot_token)}")
    print(f"✅ settings.environment: {settings.environment}")
    print(f"✅ settings.check_interval: {settings.check_interval}")
except Exception as e:
    print(f"❌ Error accessing settings: {e}")

print("=" * 70)

# Детальна перевірка мережі
print("🌐 NETWORK DIAGNOSTICS:")
print("=" * 70)


def test_network():
    """Детальний тест мережі"""
    tests = {
        'google.com': '8.8.8.8',
        'cloudflare.com': '1.1.1.1',
        'supabase.com': None
    }

    # DNS тест
    print("📡 DNS Resolution test:")
    for domain, expected_ip in tests.items():
        try:
            ip = socket.gethostbyname(domain)
            print(f"  ✅ {domain} -> {ip}")
        except Exception as e:
            print(f"  ❌ {domain} -> {type(e).__name__}: {str(e)}")

    # Тест конкретного хоста з DATABASE_URL
    if settings.database_url:
        print("\n📡 Database host test:")
        try:
            # Витягуємо хост з DATABASE_URL
            match = re.search(r'@([^:]+):', settings.database_url)
            if match:
                db_host = match.group(1)
                print(f"  Database host: {db_host}")

                # DNS тест
                try:
                    db_ip = socket.gethostbyname(db_host)
                    print(f"  ✅ Resolved to: {db_ip}")
                except Exception as e:
                    print(f"  ❌ DNS resolution failed: {e}")

                # Спроба підключення до порту
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    # Витягуємо порт
                    port_match = re.search(r':(\d+)/', settings.database_url)
                    port = int(port_match.group(1)) if port_match else 5432
                    print(f"  Testing connection to {db_host}:{port}...")
                    result = sock.connect_ex((db_host, port))
                    sock.close()
                    if result == 0:
                        print(f"  ✅ Port {port} is open")
                    else:
                        print(f"  ❌ Port {port} is closed or unreachable (error code: {result})")
                except Exception as e:
                    print(f"  ❌ Connection test failed: {e}")
            else:
                print("  ❌ Could not extract host from DATABASE_URL")
        except Exception as e:
            print(f"  ❌ Database host test failed: {e}")


test_network()
print("=" * 70)


class TelegramSMMBot:
    """Main bot application with enhanced initialization and logging"""

    def __init__(self):
        print("🤖 Initializing TelegramSMMBot class...")
        self.db = db
        self.db_logger: Optional[DatabaseLogger] = None
        self.telegram_monitor: Optional[TelegramMonitor] = None
        self.post_processor: Optional[PostProcessor] = None
        self.nakrutka_client: Optional[NakrutkaClient] = None
        self.scheduler: Optional[BotScheduler] = None
        self.performance_monitor = PerformanceMonitor()
        self.running = False
        self.start_time = datetime.utcnow()

        # Track initialization status
        self.init_status = {
            'environment': False,
            'database': False,
            'database_schema': False,
            'services': False,
            'nakrutka': False,
            'telegram': False
        }
        print("✅ Bot class initialized")

    async def verify_environment(self):
        """Verify environment variables and configuration"""
        print("\n" + "=" * 50)
        print("🔍 VERIFYING ENVIRONMENT CONFIGURATION...")
        print("=" * 50)

        try:
            # Use config validator
            errors = ConfigValidator.validate_environment()

            if errors:
                print("❌ Environment validation failed:")
                for error in errors:
                    print(f"  - {error}")
                logger.error("Environment validation failed", errors=errors)
                raise Exception(f"Invalid environment: {'; '.join(errors)}")

            print("✅ Environment configuration verified")
            self.init_status['environment'] = True
            return True

        except Exception as e:
            print(f"❌ Environment verification error: {e}")
            raise

    async def initialize_database(self):
        """Initialize database with migrations and default data"""
        print("\n" + "=" * 50)
        print("💾 INITIALIZING DATABASE...")
        print("=" * 50)

        try:
            # Initialize connection pool
            print("Creating database connection pool...")
            print(f"DATABASE_URL format check: {settings.database_url[:30]}...")

            await self.db.init()
            print("✅ Database pool created")

            # Test connection
            print("Testing database connection...")
            test_result = await test_connection()

            if not test_result:
                print("❌ Database connection test failed!")
                # Спробуємо прямий тест
                print("Trying direct connection test...")
                try:
                    import asyncpg
                    print(f"Connecting to: {settings.database_url[:30]}...")
                    test_conn = await asyncpg.connect(settings.database_url)
                    version = await test_conn.fetchval('SELECT version()')
                    print(f"✅ Direct connection successful! PostgreSQL: {version[:50]}...")
                    await test_conn.close()
                except Exception as conn_error:
                    print(f"❌ Direct connection failed: {type(conn_error).__name__}: {str(conn_error)}")
                    raise Exception("Database connection test failed")
            else:
                print("✅ Database connection established")

            self.init_status['database'] = True

            # Run migrations
            print("\nChecking database schema...")
            integrity = await initialize_database(self.db)

            # Validate schema
            schema_errors = ConfigValidator.validate_database_schema(integrity)
            if schema_errors:
                print("❌ Database schema validation failed:")
                for error in schema_errors:
                    print(f"  - {error}")
                raise Exception("Invalid database schema")

            print(f"✅ Database schema verified")
            print(f"  Tables: {len(integrity.get('tables', {}))}")
            print(f"  Functions: {len(integrity.get('functions', {}))}")
            print(f"  Views: {len(integrity.get('views', {}))}")

            self.init_status['database_schema'] = True

            # Initialize default data
            print("\nInitializing default data...")
            initializer = DatabaseInitializer(self.db)
            await initializer.initialize_all()

            # Verify initialization
            verify_result = await initializer.verify_initialization()
            if verify_result['errors']:
                print("⚠️ Database initialization warnings:")
                for warning in verify_result['errors']:
                    print(f"  - {warning}")

            print(f"✅ Database initialized successfully")
            print(f"  Services: {verify_result['services']}")
            print(f"  Channels: {verify_result['channels']}")
            print(f"  API keys: {verify_result['api_keys']}")

            self.init_status['services'] = True
            return True

        except Exception as e:
            print(f"❌ DATABASE INITIALIZATION FAILED!")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            print(f"Traceback:\n{traceback.format_exc()}")
            logger.error("Database initialization failed", error=str(e), exc_info=True)
            raise

    async def verify_nakrutka(self):
        """Verify Nakrutka API connection and configuration"""
        print("\n" + "=" * 50)
        print("🔌 VERIFYING NAKRUTKA API...")
        print("=" * 50)

        try:
            print("Performing Nakrutka health check...")
            health = await self.nakrutka_client.health_check()
            print(f"Health check result: {health}")

            if health['status'] != 'healthy':
                print(f"❌ Nakrutka health check failed: {health['status']}")
                print(f"Errors: {health.get('errors', [])}")

                if health['status'] == 'auth_error':
                    print("❌ Nakrutka authentication failed - check API key")
                    await self.db_logger.error(
                        "Nakrutka auth failed",
                        message="Invalid API key or authentication error"
                    )
                elif health['status'] == 'connection_error':
                    print("❌ Cannot connect to Nakrutka API")
                    await self.db_logger.error(
                        "Nakrutka connection failed",
                        message="Cannot reach Nakrutka API servers"
                    )

                self.init_status['nakrutka'] = False
                return False

            print(f"✅ Nakrutka API connected")
            print(f"  Balance: ${health.get('balance', 0):.2f} {health.get('currency', 'USD')}")
            print(f"  Services: {health.get('service_count', 0)}")

            # Check minimum balance
            balance = float(health.get('balance', 0))
            if balance < 1:
                print(f"❌ Nakrutka balance critically low: ${balance}")
                await self.db_logger.error(
                    "Critical: Low Nakrutka balance",
                    balance=balance,
                    message="Bot may not be able to create orders"
                )
            elif balance < 10:
                print(f"⚠️ Low Nakrutka balance: ${balance}")
                await self.db_logger.warning(
                    "Low Nakrutka balance",
                    balance=balance,
                    message="Consider adding funds soon"
                )

            self.init_status['nakrutka'] = True
            return True

        except Exception as e:
            print(f"❌ Nakrutka verification failed: {type(e).__name__}: {str(e)}")
            self.init_status['nakrutka'] = False
            return False

    async def verify_telegram(self):
        """Verify Telegram Bot API"""
        print("\n" + "=" * 50)
        print("📱 VERIFYING TELEGRAM BOT API...")
        print("=" * 50)

        try:
            print("Getting bot info...")
            bot_info = await self.telegram_monitor.bot.get_me()
            print(f"✅ Telegram bot verified")
            print(f"  Username: @{bot_info.username}")
            print(f"  Bot ID: {bot_info.id}")
            print(f"  Can read groups: {bot_info.can_read_all_group_messages}")

            if not bot_info.can_read_all_group_messages:
                print("⚠️ Bot cannot read all group messages")
                print("   Bot may not see channel posts if not admin")

            self.init_status['telegram'] = True
            return True

        except Exception as e:
            print(f"❌ Telegram API verification failed: {type(e).__name__}: {str(e)}")
            self.init_status['telegram'] = False
            return False

    async def setup(self):
        """Initialize all components with enhanced error handling"""
        print("\n" + "=" * 70)
        print("🚀 TELEGRAM SMM BOT SETUP STARTING")
        print("=" * 70)
        print(f"Environment: {settings.environment}")
        print(f"Log level: {settings.log_level}")
        print(f"Check interval: {settings.check_interval}s")
        print(f"Timezone: {settings.timezone}")

        try:
            # 1. Verify environment
            await self.verify_environment()

            # 2. Initialize and verify database
            await self.initialize_database()

            # 3. Initialize database logger
            print("\n📝 Initializing database logger...")
            self.db_logger = DatabaseLogger(self.db)
            await self.db_logger.info(
                "Bot starting",
                environment=settings.environment,
                version="1.0.0",
                python_version=sys.version
            )
            print("✅ Database logger initialized")

            # 4. Initialize Nakrutka client
            print("\n💰 Initializing Nakrutka client...")
            self.nakrutka_client = NakrutkaClient()
            print("✅ Nakrutka client initialized")

            # 5. Verify Nakrutka API
            nakrutka_ok = await self.verify_nakrutka()
            if not nakrutka_ok:
                print("⚠️ Nakrutka API not working properly!")
                await self.db_logger.warning(
                    "Nakrutka API issues",
                    message="Bot will continue but orders may fail"
                )

            # 6. Initialize Telegram monitor
            print("\n📱 Initializing Telegram monitor...")
            self.telegram_monitor = TelegramMonitor(self.db, self.nakrutka_client)
            await self.telegram_monitor.setup_bot()
            print("✅ Telegram monitor initialized")

            # 7. Verify Telegram Bot API
            telegram_ok = await self.verify_telegram()
            if not telegram_ok:
                print("⚠️ Telegram Bot API not working properly!")
                await self.db_logger.warning(
                    "Telegram Bot API issues",
                    message="Bot commands may not work"
                )

            # 8. Initialize post processor
            print("\n⚙️ Initializing post processor...")
            self.post_processor = PostProcessor(self.db, self.nakrutka_client)
            print("✅ Post processor initialized")

            # 9. Initialize scheduler
            print("\n⏰ Initializing scheduler...")
            self.scheduler = BotScheduler()
            print("✅ Scheduler initialized")

            # 10. Schedule tasks
            print("\n📅 Scheduling tasks...")
            await self._schedule_tasks()

            # Log final status
            print("\n" + "=" * 70)
            print("🎉 BOT SETUP COMPLETED")
            print("=" * 70)

            # Summary of initialization
            success_count = sum(1 for v in self.init_status.values() if v)
            total_count = len(self.init_status)

            print(f"Initialization summary: {success_count}/{total_count} successful")
            for component, status in self.init_status.items():
                print(f"  {component}: {'✅' if status else '❌'}")

            await self.db_logger.info(
                "Bot setup completed",
                initialization_status=self.init_status,
                nakrutka_ok=nakrutka_ok,
                telegram_ok=telegram_ok,
                environment=settings.environment
            )

            # Warn if critical components failed
            if not self.init_status['nakrutka']:
                print("⚠️ Running without Nakrutka - orders will fail!")
            if not self.init_status['telegram']:
                print("⚠️ Running without Telegram commands!")

            return True

        except Exception as e:
            print(f"\n❌ SETUP FAILED!")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            print(f"Traceback:\n{traceback.format_exc()}")
            print(f"Initialization status: {self.init_status}")

            if self.db_logger:
                await self.db_logger.error(
                    "Bot setup failed",
                    error=str(e),
                    error_type=type(e).__name__,
                    initialization_status=self.init_status
                )
            raise

    async def _schedule_tasks(self):
        """Schedule all periodic tasks"""
        print("Scheduling periodic tasks...")

        # Channel monitoring - main task
        self.scheduler.add_job(
            self._monitor_channels_with_metrics,
            interval_seconds=settings.check_interval,
            job_id="channel_monitor"
        )
        print(f"  ✅ Channel monitor (interval: {settings.check_interval}s)")

        # Post processing - frequent
        self.scheduler.add_job(
            self._process_posts_with_metrics,
            interval_seconds=10,
            job_id="post_processor"
        )
        print("  ✅ Post processor (interval: 10s)")

        # Status checking - less frequent
        self.scheduler.add_job(
            self._check_status_with_metrics,
            interval_seconds=60,
            job_id="status_checker"
        )
        print("  ✅ Status checker (interval: 60s)")

        # Cleanup old logs - daily
        self.scheduler.add_job(
            self._cleanup_logs,
            interval_seconds=86400,  # 24 hours
            job_id="log_cleanup"
        )
        print("  ✅ Log cleanup (interval: 24h)")

        # Health check - every 5 minutes
        self.scheduler.add_job(
            self._health_check,
            interval_seconds=300,
            job_id="health_check"
        )
        print("  ✅ Health check (interval: 5m)")

        # Balance check - every hour
        if self.init_status['nakrutka']:
            self.scheduler.add_job(
                self._check_balance,
                interval_seconds=3600,
                job_id="balance_check"
            )
            print("  ✅ Balance check (interval: 1h)")

        print("✅ All tasks scheduled")

    async def _monitor_channels_with_metrics(self):
        """Monitor channels with performance metrics"""
        print("📡 Running channel monitor task...")
        await self.performance_monitor.measure(
            "channel_monitoring",
            self.telegram_monitor.check_channels
        )

    async def _process_posts_with_metrics(self):
        """Process posts with performance metrics"""
        print("⚙️ Running post processor task...")
        await self.performance_monitor.measure(
            "post_processing",
            self.post_processor.process_new_posts
        )

    async def _check_status_with_metrics(self):
        """Check order status with performance metrics"""
        print("📊 Running status checker task...")
        await self.performance_monitor.measure(
            "status_checking",
            self.post_processor.check_order_status
        )

    async def _cleanup_logs(self):
        """Clean up old log entries"""
        print("🧹 Running log cleanup task...")
        try:
            from src.database.queries import Queries
            queries = Queries(self.db)
            deleted = await queries.cleanup_old_logs(days=7)
            print(f"✅ Cleaned up {deleted} old log entries")

            # Also clean performance metrics
            self.performance_monitor.clear_metrics()

        except Exception as e:
            print(f"❌ Log cleanup failed: {e}")
            logger.error(f"Log cleanup failed: {e}")

    async def _health_check(self):
        """Perform comprehensive health check"""
        print("🏥 Running health check...")
        try:
            health_status = {
                'timestamp': datetime.utcnow().isoformat(),
                'uptime_hours': (datetime.utcnow() - self.start_time).total_seconds() / 3600,
                'components': {}
            }

            # Check database
            try:
                db_test = await test_connection()
                health_status['components']['database'] = 'OK' if db_test else 'FAIL'
            except:
                health_status['components']['database'] = 'FAIL'

            # Check Nakrutka
            if self.nakrutka_client and self.init_status['nakrutka']:
                try:
                    nakrutka_health = await self.nakrutka_client.health_check()
                    health_status['components']['nakrutka'] = nakrutka_health['status']
                    health_status['nakrutka_balance'] = nakrutka_health.get('balance', 0)
                except:
                    health_status['components']['nakrutka'] = 'ERROR'
            else:
                health_status['components']['nakrutka'] = 'DISABLED'

            # Check Telegram
            health_status['components']['telegram'] = (
                'OK' if self.telegram_monitor else 'DISABLED'
            )

            # Get performance stats
            health_status['performance'] = {
                name: self.performance_monitor.get_stats(name)
                for name in ['channel_monitoring', 'post_processing', 'status_checking']
            }

            # Get monitoring stats
            if self.telegram_monitor:
                health_status['monitoring'] = self.telegram_monitor.get_monitor_stats()

            print(f"✅ Health check completed")
            print(f"  Uptime: {health_status['uptime_hours']:.1f} hours")
            print(f"  Components: {health_status['components']}")

            # Log to database
            await self.db_logger.info("Health check", **health_status)

            # Alert if issues found
            issues = [
                comp for comp, status in health_status['components'].items()
                if status not in ['OK', 'DISABLED']
            ]
            if issues:
                print(f"⚠️ Health check found issues: {issues}")
                await self.db_logger.warning(
                    "Health check issues",
                    failed_components=issues,
                    details=health_status
                )

        except Exception as e:
            print(f"❌ Health check failed: {e}")
            logger.error(f"Health check failed: {e}")

    async def _check_balance(self):
        """Check Nakrutka balance periodically"""
        print("💰 Checking Nakrutka balance...")
        if not self.nakrutka_client:
            return

        try:
            balance_info = await self.nakrutka_client.get_balance()
            balance = float(balance_info.get('balance', 0))
            currency = balance_info.get('currency', 'USD')

            print(f"💰 Balance: ${balance:.2f} {currency}")

            # Alert on low balance
            if balance < 1:
                print(f"❌ CRITICAL: Balance too low: ${balance:.2f}")
                await self.db_logger.error(
                    "Critical low balance",
                    balance=balance,
                    currency=currency,
                    message="Bot cannot create new orders!"
                )
            elif balance < 10:
                print(f"⚠️ Low balance warning: ${balance:.2f}")
                await self.db_logger.warning(
                    "Low balance warning",
                    balance=balance,
                    currency=currency,
                    message="Consider adding funds"
                )

        except Exception as e:
            print(f"❌ Balance check failed: {e}")
            logger.error(f"Balance check failed: {e}")

    async def start(self):
        """Start the bot"""
        self.running = True
        print("\n🚀 Starting bot services...")

        try:
            # Start scheduler
            print("Starting scheduler...")
            self.scheduler.start()
            print("✅ Scheduler started")

            # Start telegram bot
            print("Starting Telegram bot...")
            await self.telegram_monitor.start_bot()
            print("✅ Telegram bot started")

            await self.db_logger.info(
                "Bot started successfully",
                scheduled_jobs=len(self.scheduler.get_jobs()),
                initialization_status=self.init_status
            )

            print("\n" + "=" * 70)
            print("🟢 BOT IS RUNNING")
            print("Press Ctrl+C to stop")
            print("=" * 70)

            # Main loop
            loop_counter = 0
            last_stats_log = datetime.utcnow()

            while self.running:
                await asyncio.sleep(1)
                loop_counter += 1

                # Log heartbeat every minute
                if loop_counter % 60 == 0:
                    print(f"💓 Heartbeat: {loop_counter}s uptime, {len(self.scheduler.get_jobs())} active jobs")
                    logger.debug(
                        "Bot heartbeat",
                        uptime_seconds=loop_counter,
                        active_jobs=len(self.scheduler.get_jobs())
                    )

                # Log detailed stats every hour
                if (datetime.utcnow() - last_stats_log).total_seconds() >= 3600:
                    await self._log_hourly_stats()
                    last_stats_log = datetime.utcnow()

        except Exception as e:
            print(f"❌ Error in main loop: {type(e).__name__}: {str(e)}")
            logger.error("Error in main loop", error=str(e), exc_info=True)
            raise

    async def _log_hourly_stats(self):
        """Log hourly statistics"""
        print("📊 Logging hourly statistics...")
        try:
            from src.database.queries import Queries
            queries = Queries(self.db)

            # Get monitoring stats
            stats = await queries.get_monitoring_stats()

            print(f"📊 Hourly statistics:")
            print(f"  Active channels: {stats.get('active_channels', 0)}")
            print(f"  Posts (24h): {stats.get('posts_24h', 0)}")
            print(f"  Orders (24h): {stats.get('orders_24h', 0)}")
            print(f"  Cost today: ${stats.get('cost_today', 0):.2f}")

            logger.info(
                "📊 Hourly statistics",
                active_channels=stats.get('active_channels', 0),
                posts_24h=stats.get('posts_24h', 0),
                orders_24h=stats.get('orders_24h', 0),
                cost_today=f"${stats.get('cost_today', 0):.2f}"
            )

            # Performance metrics
            for metric_name in ['channel_monitoring', 'post_processing', 'status_checking']:
                metric_stats = self.performance_monitor.get_stats(metric_name)
                if metric_stats and metric_stats['count'] > 0:
                    print(f"  Performance - {metric_name}:")
                    print(f"    Calls: {metric_stats['count']}")
                    print(f"    Success rate: {metric_stats['success_rate'] * 100:.1f}%")
                    print(f"    Avg duration: {metric_stats['avg_duration']:.2f}s")

        except Exception as e:
            print(f"❌ Failed to log hourly stats: {e}")
            logger.error(f"Failed to log hourly stats: {e}")

    async def stop(self):
        """Stop the bot gracefully"""
        print("\n🛑 Stopping bot...")
        self.running = False

        try:
            # Stop scheduler first
            if self.scheduler:
                print("Stopping scheduler...")
                self.scheduler.stop()
                print("✅ Scheduler stopped")

            # Stop telegram bot
            if self.telegram_monitor:
                print("Stopping Telegram bot...")
                await self.telegram_monitor.stop_bot()
                print("✅ Telegram bot stopped")

            # Log final stats
            uptime = datetime.utcnow() - self.start_time
            print(f"Bot uptime: {uptime.total_seconds() / 3600:.1f} hours")

            # Log shutdown to database
            if self.db_logger:
                await self.db_logger.info(
                    "Bot stopping",
                    uptime_hours=uptime.total_seconds() / 3600,
                    reason="User requested",
                    final_status=self.init_status
                )

            # Close Nakrutka session
            if self.nakrutka_client:
                print("Closing Nakrutka session...")
                await self.nakrutka_client.close()
                print("✅ Nakrutka session closed")

            # Close database last
            print("Closing database connection...")
            await self.db.close()
            print("✅ Database connection closed")

            print("\n" + "=" * 70)
            print("🔴 BOT STOPPED")
            print("=" * 70)

        except Exception as e:
            print(f"❌ Error during shutdown: {str(e)}")
            logger.error(f"Error during shutdown: {str(e)}", exc_info=True)

    def handle_signal(self, sig, frame):
        """Handle shutdown signals"""
        print(f"\n📡 Received signal {sig}")
        asyncio.create_task(self.stop())


async def main():
    """Main entry point with enhanced initialization"""
    print("\n" + "=" * 70)
    print("🚀 TELEGRAM SMM BOT - MAIN FUNCTION")
    print("=" * 70)
    print(f"Python version: {sys.version}")
    print(f"Platform: {sys.platform}")
    print(f"Process ID: {os.getpid()}")
    print(f"Working directory: {os.getcwd()}")
    print("=" * 70)

    # Configure logging
    print("Configuring logging...")
    logger.info("Starting Telegram SMM Bot")
    logger.info(f"Python {sys.version}")
    logger.info(f"Platform: {sys.platform}")
    logger.info(f"Process ID: {os.getpid()}")

    # Log environment info (without sensitive data)
    env_info = {
        'DATABASE_URL': 'configured' if settings.database_url else 'missing',
        'NAKRUTKA_API_KEY': 'configured' if settings.nakrutka_api_key else 'missing',
        'TELEGRAM_BOT_TOKEN': 'configured' if settings.telegram_bot_token else 'missing',
        'ENVIRONMENT': settings.environment,
        'CHECK_INTERVAL': settings.check_interval,
        'ADMIN_ID': 'configured' if settings.admin_telegram_id else 'not set'
    }
    print("Environment configuration:")
    for key, value in env_info.items():
        print(f"  {key}: {value}")
    logger.info("Environment configuration", **env_info)

    bot = TelegramSMMBot()

    # Setup signal handlers
    for sig in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, bot.handle_signal)
    print("✅ Signal handlers configured")

    try:
        # Setup bot with all initialization
        print("\n🔧 Starting bot setup...")
        await bot.setup()

        # Start bot
        print("\n🚀 Starting bot main loop...")
        await bot.start()

    except KeyboardInterrupt:
        print("\n⌨️ Keyboard interrupt received")
    except Exception as e:
        print(f"\n💥 BOT CRASHED!")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print(f"Traceback:\n{traceback.format_exc()}")

        logger.error(f"💥 Bot crashed: {str(e)}", error=str(e), exc_info=True)

        if bot.db_logger:
            try:
                await bot.db_logger.error(
                    "Bot crashed",
                    error=str(e),
                    error_type=type(e).__name__,
                    traceback=True
                )
            except:
                print("Failed to log crash to database")
    finally:
        print("\nShutting down...")
        await bot.stop()
        print("\n👋 Bot stopped")


if __name__ == "__main__":
    print("\n🏁 STARTING BOT PROCESS...")

    # Windows event loop policy fix
    if sys.platform == "win32":
        print("Windows detected, setting event loop policy")
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Run bot
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"\n❌ FAILED TO RUN BOT!")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)