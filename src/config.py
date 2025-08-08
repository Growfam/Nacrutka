"""
Simplified configuration - only essential settings
"""
import os
from typing import Optional


class Settings:
    """Minimal settings for the bot"""

    # Database
    database_url: str = os.getenv('DATABASE_URL', '')

    # APIs
    nakrutka_api_key: str = os.getenv('NAKRUTKA_API_KEY', '')
    nakrutka_api_url: str = os.getenv('NAKRUTKA_API_URL', 'https://nakrutochka.com/api/v2')
    telegram_bot_token: str = os.getenv('TELEGRAM_BOT_TOKEN', '')

    # Bot settings
    check_interval: int = int(os.getenv('CHECK_INTERVAL', '30'))
    send_interval: int = int(os.getenv('SEND_INTERVAL', '10'))
    status_check_interval: int = int(os.getenv('STATUS_CHECK_INTERVAL', '60'))

    # Admin
    admin_telegram_id: Optional[int] = int(os.getenv('ADMIN_TELEGRAM_ID')) if os.getenv('ADMIN_TELEGRAM_ID') else None

    # Logging
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')
    environment: str = os.getenv('ENVIRONMENT', 'production')

    # Limits
    max_posts_per_check: int = int(os.getenv('MAX_POSTS_PER_CHECK', '50'))
    max_orders_per_batch: int = int(os.getenv('MAX_ORDERS_PER_BATCH', '10'))


settings = Settings()