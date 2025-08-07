"""
Application configuration management
"""
import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings"""

    # Environment
    environment: str = Field(default="development", env="ENVIRONMENT")

    # Database
    database_url: str = Field(..., env="DATABASE_URL")
    supabase_url: str = Field(..., env="SUPABASE_URL")
    supabase_key: str = Field(..., env="SUPABASE_KEY")

    # Telegram
    telegram_bot_token: str = Field(..., env="TELEGRAM_BOT_TOKEN")
    admin_telegram_id: Optional[int] = Field(None, env="ADMIN_TELEGRAM_ID")

    # Twiboost API
    twiboost_api_url: str = Field(
        default="https://twiboost.com/api/v2",
        env="TWIBOOST_API_URL"
    )
    twiboost_api_key: str = Field(..., env="TWIBOOST_API_KEY")

    # Nakrutochka API
    nakrutochka_api_url: str = Field(
        default="https://nakrutochka.com/api/v2",
        env="NAKRUTOCHKA_API_URL"
    )
    nakrutochka_api_key: str = Field(..., env="NAKRUTOCHKA_API_KEY")

    # API Usage Preferences
    use_nakrutochka_for_reactions: bool = Field(
        default=True,
        env="USE_NAKRUTOCHKA_FOR_REACTIONS"
    )
    use_nakrutochka_for_reposts: bool = Field(
        default=True,
        env="USE_NAKRUTOCHKA_FOR_REPOSTS"
    )
    use_twiboost_for_views: bool = Field(
        default=True,
        env="USE_TWIBOOST_FOR_VIEWS"
    )

    # Fallback settings
    enable_api_fallback: bool = Field(
        default=True,
        env="ENABLE_API_FALLBACK"
    )

    # API Priority (which API to try first)
    api_priority_reactions: str = Field(
        default="nakrutochka",  # "nakrutochka" or "twiboost"
        env="API_PRIORITY_REACTIONS"
    )
    api_priority_reposts: str = Field(
        default="nakrutochka",
        env="API_PRIORITY_REPOSTS"
    )
    api_priority_views: str = Field(
        default="twiboost",
        env="API_PRIORITY_VIEWS"
    )

    # Monitoring intervals (seconds)
    check_interval: int = Field(default=30, env="CHECK_INTERVAL")
    process_interval: int = Field(default=10, env="PROCESS_INTERVAL")

    # Retry settings
    max_retries: int = Field(default=3, env="MAX_RETRIES")
    retry_delay: int = Field(default=5, env="RETRY_DELAY")

    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")

    # Portion settings (defaults, can be overridden in DB)
    default_fast_delivery_percent: int = Field(default=70)
    default_portions_count: int = Field(default=5)

    # Randomization defaults
    default_randomization_percent: int = Field(default=40)

    # Performance
    max_concurrent_orders: int = Field(default=10)
    batch_size: int = Field(default=50)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()


# Service IDs mapping (will be synced from APIs)
SERVICE_TYPES = {
    "views": "view",
    "reactions": "reaction",
    "reposts": "repost"
}

# Default portion distributions
FAST_PORTIONS = [29.13, 22.50, 17.04, 13.09]  # ~82% total
SLOW_PORTIONS = [18.24]  # ~18% total

# Reaction emoji defaults (can be customized per channel)
DEFAULT_REACTIONS = {
    "👍": 45,
    "❤️": 30,
    "🔥": 25
}

# Extended emoji support for Nakrutochka
NAKRUTOCHKA_REACTIONS = {
    "👍": 30,  # Like
    "❤️": 25,  # Love
    "🔥": 15,  # Fire
    "😊": 10,  # Happy
    "😮": 8,   # Wow
    "😢": 7,   # Sad
    "😡": 5    # Angry
}

# API Provider enum
class APIProvider:
    TWIBOOST = "twiboost"
    NAKRUTOCHKA = "nakrutochka"