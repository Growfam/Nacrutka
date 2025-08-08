"""
Simple logger configuration
"""
import logging
import sys
from datetime import datetime
from src.config import settings


def setup_logging():
    """Setup logging configuration"""

    # Determine log level
    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    # Create formatter
    if settings.environment == 'production':
        # JSON-like format for production
        formatter = logging.Formatter(
            '{"time":"%(asctime)s","level":"%(levelname)s","module":"%(name)s","message":"%(message)s"}'
        )
    else:
        # Human-readable format for development
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )

    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers
    root_logger.handlers = []

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Suppress noisy libraries
    logging.getLogger('telegram').setLevel(logging.WARNING)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    return root_logger


# Initialize on import
logger = setup_logging()


def get_logger(name: str) -> logging.Logger:
    """Get logger for module"""
    return logging.getLogger(name)