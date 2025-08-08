"""
Configuration management for Telegram SMM Bot - Enhanced version
"""
import os
from typing import Optional, Dict, List
from pydantic_settings import BaseSettings
from pydantic import Field, validator


class Settings(BaseSettings):
    """Application settings with enhanced configuration"""

    # Environment
    environment: str = Field(default="development", env="ENVIRONMENT")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")

    # Database
    database_url: str = Field(..., env="DATABASE_URL")

    # Supabase
    supabase_url: str = Field(..., env="SUPABASE_URL")
    supabase_key: str = Field(..., env="SUPABASE_KEY")

    # APIs
    nakrutka_api_key: str = Field(..., env="NAKRUTKA_API_KEY")
    nakrutka_api_url: str = Field(
        default="https://nakrutochka.com/api/v2",
        env="NAKRUTKA_API_URL"
    )
    telegram_bot_token: str = Field(..., env="TELEGRAM_BOT_TOKEN")

    # Telethon settings (for advanced monitoring)
    telethon_api_id: Optional[int] = Field(default=None, env="TELETHON_API_ID")
    telethon_api_hash: Optional[str] = Field(default=None, env="TELETHON_API_HASH")
    telethon_session_name: str = Field(default="smm_bot_session", env="TELETHON_SESSION_NAME")
    telethon_phone: Optional[str] = Field(default=None, env="TELETHON_PHONE")
    enable_telethon: bool = Field(default=False, env="ENABLE_TELETHON")

    # Bot settings
    check_interval: int = Field(default=30, env="CHECK_INTERVAL")
    max_retries: int = Field(default=3, env="MAX_RETRIES")
    retry_delay: int = Field(default=5, env="RETRY_DELAY")

    # Admin
    admin_telegram_id: Optional[int] = Field(default=None, env="ADMIN_TELEGRAM_ID")

    # Timezone
    timezone: str = Field(default="Europe/Kiev", env="TZ")

    # Performance settings
    max_concurrent_orders: int = Field(default=5, env="MAX_CONCURRENT_ORDERS")
    batch_size: int = Field(default=10, env="BATCH_SIZE")
    rate_limit_calls: int = Field(default=60, env="RATE_LIMIT_CALLS")

    # Database pool settings
    db_pool_min_size: int = Field(default=5, env="DB_POOL_MIN_SIZE")
    db_pool_max_size: int = Field(default=20, env="DB_POOL_MAX_SIZE")
    db_max_queries: int = Field(default=50000, env="DB_MAX_QUERIES")
    db_max_inactive_connection_lifetime: int = Field(default=300, env="DB_MAX_INACTIVE_CONNECTION_LIFETIME")

    # Cache settings
    enable_caching: bool = Field(default=True, env="ENABLE_CACHING")
    cache_ttl_seconds: int = Field(default=300, env="CACHE_TTL_SECONDS")  # 5 minutes
    service_cache_ttl: int = Field(default=3600, env="SERVICE_CACHE_TTL")  # 1 hour
    channel_cache_ttl: int = Field(default=600, env="CHANNEL_CACHE_TTL")  # 10 minutes
    max_cache_size: int = Field(default=1000, env="MAX_CACHE_SIZE")
    cache_cleanup_interval: int = Field(default=3600, env="CACHE_CLEANUP_INTERVAL")  # 1 hour

    # Monitoring
    enable_metrics: bool = Field(default=False, env="ENABLE_METRICS")
    metrics_port: int = Field(default=9090, env="METRICS_PORT")
    health_check_interval: int = Field(default=300, env="HEALTH_CHECK_INTERVAL")

    # Debug settings
    debug_mode: bool = Field(default=False, env="DEBUG_MODE")
    save_api_responses: bool = Field(default=False, env="SAVE_API_RESPONSES")
    log_sql_queries: bool = Field(default=False, env="LOG_SQL_QUERIES")
    log_cache_hits: bool = Field(default=False, env="LOG_CACHE_HITS")

    # Notification settings
    enable_notifications: bool = Field(default=False, env="ENABLE_NOTIFICATIONS")
    notification_chat_id: Optional[int] = Field(default=None, env="NOTIFICATION_CHAT_ID")
    notify_on_low_balance: bool = Field(default=True, env="NOTIFY_ON_LOW_BALANCE")
    low_balance_threshold: float = Field(default=10.0, env="LOW_BALANCE_THRESHOLD")

    # Backup settings
    enable_backups: bool = Field(default=False, env="ENABLE_BACKUPS")
    backup_interval: int = Field(default=86400, env="BACKUP_INTERVAL")
    backup_retention_days: int = Field(default=7, env="BACKUP_RETENTION_DAYS")

    # Channel monitoring settings
    max_posts_per_check: int = Field(default=50, env="MAX_POSTS_PER_CHECK")
    max_post_age_hours: int = Field(default=48, env="MAX_POST_AGE_HOURS")
    min_check_interval_seconds: int = Field(default=20, env="MIN_CHECK_INTERVAL_SECONDS")
    channel_error_threshold: int = Field(default=5, env="CHANNEL_ERROR_THRESHOLD")
    use_web_preview: bool = Field(default=True, env="USE_WEB_PREVIEW")

    # Order processing settings
    enable_adaptive_distribution: bool = Field(default=True, env="ENABLE_ADAPTIVE_DISTRIBUTION")
    adaptive_learning_rate: float = Field(default=0.1, env="ADAPTIVE_LEARNING_RATE")
    max_portion_size: int = Field(default=1000, env="MAX_PORTION_SIZE")
    min_portion_size: int = Field(default=10, env="MIN_PORTION_SIZE")
    drip_feed_max_runs: int = Field(default=100, env="DRIP_FEED_MAX_RUNS")
    drip_feed_min_interval: int = Field(default=5, env="DRIP_FEED_MIN_INTERVAL")

    # Service selection settings
    enable_dynamic_service_selection: bool = Field(default=True, env="ENABLE_DYNAMIC_SERVICE_SELECTION")
    service_selection_strategy: str = Field(default="balanced", env="SERVICE_SELECTION_STRATEGY")  # balanced, cheapest, quality
    service_quality_weight: float = Field(default=0.7, env="SERVICE_QUALITY_WEIGHT")
    service_price_weight: float = Field(default=0.3, env="SERVICE_PRICE_WEIGHT")
    min_service_success_rate: float = Field(default=0.8, env="MIN_SERVICE_SUCCESS_RATE")

    # Reaction distribution settings
    enable_smart_reaction_distribution: bool = Field(default=True, env="ENABLE_SMART_REACTION_DISTRIBUTION")
    reaction_distribution_strategy: str = Field(default="weighted", env="REACTION_DISTRIBUTION_STRATEGY")  # weighted, equal, custom
    max_reaction_types_per_post: int = Field(default=10, env="MAX_REACTION_TYPES_PER_POST")
    reaction_balance_factor: float = Field(default=0.2, env="REACTION_BALANCE_FACTOR")  # How much to balance reaction types

    # Error handling settings
    max_consecutive_errors: int = Field(default=3, env="MAX_CONSECUTIVE_ERRORS")
    error_backoff_multiplier: float = Field(default=2.0, env="ERROR_BACKOFF_MULTIPLIER")
    max_error_backoff_seconds: int = Field(default=300, env="MAX_ERROR_BACKOFF_SECONDS")
    retry_failed_orders: bool = Field(default=True, env="RETRY_FAILED_ORDERS")
    failed_order_retry_delay_hours: int = Field(default=24, env="FAILED_ORDER_RETRY_DELAY_HOURS")

    # Rate limiting settings
    enable_smart_rate_limiting: bool = Field(default=True, env="ENABLE_SMART_RATE_LIMITING")
    nakrutka_rate_limit_per_minute: int = Field(default=60, env="NAKRUTKA_RATE_LIMIT_PER_MINUTE")
    telegram_rate_limit_per_minute: int = Field(default=30, env="TELEGRAM_RATE_LIMIT_PER_MINUTE")
    rate_limit_burst_size: int = Field(default=10, env="RATE_LIMIT_BURST_SIZE")

    # Advanced features
    enable_ml_predictions: bool = Field(default=False, env="ENABLE_ML_PREDICTIONS")
    enable_auto_scaling: bool = Field(default=False, env="ENABLE_AUTO_SCALING")
    enable_cost_optimization: bool = Field(default=True, env="ENABLE_COST_OPTIMIZATION")
    cost_optimization_threshold: float = Field(default=0.1, env="COST_OPTIMIZATION_THRESHOLD")  # 10% cost difference

    @validator('telethon_api_id')
    def validate_telethon_api_id(cls, v, values):
        """Validate Telethon API ID"""
        if values.get('enable_telethon') and not v:
            raise ValueError("TELETHON_API_ID required when ENABLE_TELETHON is true")
        return v

    @validator('telethon_api_hash')
    def validate_telethon_api_hash(cls, v, values):
        """Validate Telethon API hash"""
        if values.get('enable_telethon') and not v:
            raise ValueError("TELETHON_API_HASH required when ENABLE_TELETHON is true")
        return v

    @validator('service_selection_strategy')
    def validate_service_strategy(cls, v):
        """Validate service selection strategy"""
        allowed = ['balanced', 'cheapest', 'quality']
        if v not in allowed:
            raise ValueError(f"SERVICE_SELECTION_STRATEGY must be one of {allowed}")
        return v

    @validator('reaction_distribution_strategy')
    def validate_reaction_strategy(cls, v):
        """Validate reaction distribution strategy"""
        allowed = ['weighted', 'equal', 'custom']
        if v not in allowed:
            raise ValueError(f"REACTION_DISTRIBUTION_STRATEGY must be one of {allowed}")
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields


# Create global settings instance
settings = Settings()


# Status constants
POST_STATUS = {
    "NEW": "new",
    "PROCESSING": "processing",
    "COMPLETED": "completed",
    "FAILED": "failed",
    "RETRYING": "retrying",
    "SKIPPED": "skipped"
}

ORDER_STATUS = {
    "PENDING": "pending",
    "IN_PROGRESS": "in_progress",
    "COMPLETED": "completed",
    "CANCELLED": "cancelled",
    "PARTIAL": "partial",
    "FAILED": "failed"
}

PORTION_STATUS = {
    "WAITING": "waiting",
    "SCHEDULED": "scheduled",
    "RUNNING": "running",
    "COMPLETED": "completed",
    "FAILED": "failed"
}

SERVICE_TYPES = {
    "VIEWS": "views",
    "REACTIONS": "reactions",
    "REPOSTS": "reposts",
    "SUBSCRIBERS": "subscribers"
}

# Default randomization settings
DEFAULT_RANDOMIZE_PERCENT = 40
DEFAULT_CHECK_INTERVAL = 30
DEFAULT_RETRY_DELAY = 5

# Limits
MAX_PORTIONS = 10
MAX_REACTION_TYPES = 20
MIN_CHECK_INTERVAL = 10
MAX_CHECK_INTERVAL = 300
MAX_ORDER_QUANTITY = 1000000
MIN_ORDER_QUANTITY = 1

# Time windows
PROMOTION_START_HOUR = 8  # UTC
PROMOTION_END_HOUR = 23   # UTC
NIGHT_MODE_REDUCTION = 0.3  # Reduce activity by 30% at night

# Service quality thresholds
MIN_SUCCESS_RATE = 0.8  # 80% success rate
MAX_ERROR_RATE = 0.2    # 20% error rate
CRITICAL_SUCCESS_RATE = 0.5  # Below this, service is disabled

# Telegram limits
TELEGRAM_MAX_MESSAGE_LENGTH = 4096
TELEGRAM_MAX_CAPTION_LENGTH = 1024
TELEGRAM_MAX_BUTTONS_PER_ROW = 8
TELEGRAM_MAX_CALLBACK_DATA_LENGTH = 64
TELEGRAM_MAX_INLINE_RESULTS = 50

# Cache keys
CACHE_KEY_SERVICES = "services:all"
CACHE_KEY_SERVICE = "service:{service_id}"
CACHE_KEY_CHANNEL = "channel:{channel_id}"
CACHE_KEY_CHANNEL_POSTS = "channel:posts:{channel_id}"
CACHE_KEY_BALANCE = "nakrutka:balance"
CACHE_KEY_STATS = "stats:{type}"

# Error categories
ERROR_CATEGORIES = {
    "TEMPORARY": ["timeout", "connection", "rate_limit"],
    "PERMANENT": ["invalid_key", "service_not_found", "invalid_quantity"],
    "FINANCIAL": ["insufficient_funds", "balance_error"],
    "VALIDATION": ["invalid_input", "limit_exceeded"]
}

# Reaction emoji groups
REACTION_GROUPS = {
    "positive": ["❤️", "🔥", "👍", "🎉", "🥰", "😍", "💯", "🏆"],
    "negative": ["👎", "😢", "💩", "🤮", "😱", "🤬", "😡", "💔"],
    "neutral": ["🤔", "😐", "🤷", "👀", "🙄", "😴", "🥱"],
    "premium": ["🤩", "💘", "🌚", "🕊", "🐳", "👾", "🦄", "🌭"],
    "special": ["⚡️", "🤝", "🙏", "👏", "💊", "🗿", "🤡", "😈"]
}

# Service categories
SERVICE_CATEGORIES = {
    "instant": ["моментальн", "instant", "fast"],
    "quality": ["premium", "высокое качество", "high quality"],
    "regional": ["UA", "США", "СНГ", "EN", "Казахстан"],
    "refill": ["R30", "R60", "R90", "R180", "R365"]
}

# Distribution patterns
DISTRIBUTION_PATTERNS = {
    "standard": {
        "fast_percent": 70,
        "portions": 5,
        "front_hours": 4,
        "total_hours": 24
    },
    "natural": {
        "fast_percent": 60,
        "portions": 7,
        "front_hours": 6,
        "total_hours": 36
    },
    "aggressive": {
        "fast_percent": 85,
        "portions": 4,
        "front_hours": 2,
        "total_hours": 12
    },
    "slow": {
        "fast_percent": 40,
        "portions": 10,
        "front_hours": 12,
        "total_hours": 72
    }
}

# Monitoring strategies
MONITORING_STRATEGIES = {
    "aggressive": {
        "check_interval": 20,
        "max_posts": 100,
        "cache_ttl": 180
    },
    "normal": {
        "check_interval": 30,
        "max_posts": 50,
        "cache_ttl": 300
    },
    "conservative": {
        "check_interval": 60,
        "max_posts": 20,
        "cache_ttl": 600
    }
}

# Cost optimization rules
COST_OPTIMIZATION_RULES = {
    "prefer_bulk": True,  # Prefer services with better bulk pricing
    "avoid_premium_for_small": 100,  # Don't use premium services for orders < 100
    "regional_preference": ["СНГ", "UA", "EN"],  # Order of regional preference
    "quality_threshold": 0.85  # Minimum quality score for service selection
}