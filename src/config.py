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

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Ігнорувати додаткові поля


# Create global settings instance
settings = Settings()


# Service IDs mapping (from database)
SERVICE_IDS = {
    "views": {
        "instant": 3791,  # Моментальные
        "slow": 4331,     # Медленные
        "kazakhstan": 3822,  # Казахстан
        "usa": 4014,      # США
        "auto_cis": 4105, # Авто СНГ
    },
    "reactions": {
        "positive_mix": 3911,  # 👍 ❤️ 🔥 🎉
        "negative_mix": 3870,  # 👎 😁 😢 💩 🤮
        "premium_mix": 4002,   # Premium позитивные
        "heart": 3850,         # ❤️
        "fire": 3849,          # 🔥
        "thumbs_up": 3838,     # 👍
        "lightning": 3839,     # ⚡️
        "whale": 3872,         # 🐳
    },
    "reposts": {
        "statistics": 3943,  # Для статистики
    }
}


# Status mappings
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
    "REPOSTS": "reposts"
}