"""
Configuration management for Telegram SMM Bot
"""
import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings"""

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

    # Monitoring
    enable_metrics: bool = Field(default=False, env="ENABLE_METRICS")
    metrics_port: int = Field(default=9090, env="METRICS_PORT")
    health_check_interval: int = Field(default=300, env="HEALTH_CHECK_INTERVAL")

    # Debug settings
    debug_mode: bool = Field(default=False, env="DEBUG_MODE")
    save_api_responses: bool = Field(default=False, env="SAVE_API_RESPONSES")
    log_sql_queries: bool = Field(default=False, env="LOG_SQL_QUERIES")

    # Notification settings
    enable_notifications: bool = Field(default=False, env="ENABLE_NOTIFICATIONS")
    notification_chat_id: Optional[int] = Field(default=None, env="NOTIFICATION_CHAT_ID")

    # Backup settings
    enable_backups: bool = Field(default=False, env="ENABLE_BACKUPS")
    backup_interval: int = Field(default=86400, env="BACKUP_INTERVAL")
    backup_retention_days: int = Field(default=7, env="BACKUP_RETENTION_DAYS")

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
    "FAILED": "failed"
}

ORDER_STATUS = {
    "PENDING": "pending",
    "IN_PROGRESS": "in_progress",
    "COMPLETED": "completed",
    "CANCELLED": "cancelled"
}

PORTION_STATUS = {
    "WAITING": "waiting",
    "RUNNING": "running",
    "COMPLETED": "completed"
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

# Time windows
PROMOTION_START_HOUR = 8  # UTC
PROMOTION_END_HOUR = 23   # UTC

# Service quality thresholds
MIN_SUCCESS_RATE = 0.8  # 80% success rate
MAX_ERROR_RATE = 0.2    # 20% error rate

# Telegram limits
TELEGRAM_MAX_MESSAGE_LENGTH = 4096
TELEGRAM_MAX_CAPTION_LENGTH = 1024
TELEGRAM_MAX_BUTTONS_PER_ROW = 8
TELEGRAM_MAX_CALLBACK_DATA_LENGTH = 64